# ============================================================
# INGESTÃO DE DADOS DE MANUTENÇÃO - Camada Bronze
# ============================================================
# Esse script é responsável por buscar registros de manutenção
# de uma API HTTP e armazená-los no MinIO (nosso "S3 local").
# É a primeira etapa do pipeline de dados: a ingestão bruta,
# que vai para a camada Bronze sem nenhuma transformação.
# ============================================================

# Importamos as bibliotecas necessárias para o script funcionar.
# json: serializar/desserializar dados no formato JSON
# os: ler variáveis de ambiente da máquina (endpoints, senhas etc.)
# random: gerar dados falsos quando a API não estiver disponível
# datetime/date/timezone: trabalhar com datas e horários
# Optional: tipo do Python para dizer que um argumento pode ser None
import json
import os
import random
from datetime import date, datetime, timezone
from typing import Optional

# boto3 é a biblioteca oficial da AWS para Python, mas aqui usamos
# para falar com o MinIO, que é compatível com a API S3 da Amazon.
# requests é para fazer chamadas HTTP para a API de manutenção.
# botocore.client.Config permite personalizar a configuração do cliente S3.
import boto3
import requests
from botocore.client import Config

# Aqui lemos as configurações a partir de variáveis de ambiente.
# Isso é uma boa prática: evita deixar configurações "hardcoded" no código.
# Se a variável não estiver definida, usa o valor padrão (segundo argumento).
# Por exemplo: se rodarmos em Docker, o serviço MinIO se chama "minio" na rede interna.
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MAINTENANCE_API_BASE = os.environ.get("MAINTENANCE_API_URL", "http://maintenance-api:8000")

# O bucket "bronze" é onde vão os dados brutos — sem transformação.
# Essa nomenclatura vem da arquitetura Medallion (Bronze -> Silver -> Gold).
BUCKET = "bronze"


def get_s3():
    """
    Cria e retorna um cliente boto3 configurado para falar com o MinIO.

    O MinIO imita a API S3 da AWS, então usamos o mesmo boto3,
    mas apontando para o endpoint local (MINIO_ENDPOINT) em vez da AWS.
    As credenciais "admin/password" são as padrão do MinIO em ambiente de dev.
    """
    # Aqui estamos criando um cliente S3 compatível com MinIO.
    # O parâmetro signature_version="s3v4" é necessário porque o MinIO
    # exige esse padrão de assinatura nas requisições HTTP.
    # region_name precisa ser preenchido mesmo sendo local, senão o boto3 reclama.
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id="admin",
        aws_secret_access_key="password",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def fetch_records(execution_date: str) -> list:
    """
    Busca os registros de manutenção da API real para uma data específica.

    Se a API estiver fora do ar (erro de conexão, timeout ou erro HTTP),
    caímos automaticamente no fallback com dados simulados (_mock_records).
    Isso é útil para desenvolvimento local quando a API não está rodando.
    """
    try:
        # Fazemos uma requisição GET para o endpoint /maintenance,
        # passando a data como parâmetro de query string: ?date=2024-01-01
        # O timeout=10 evita que o script fique travado esperando para sempre.
        resp = requests.get(
            f"{MAINTENANCE_API_BASE}/maintenance",
            params={"date": execution_date},
            timeout=10,
        )
        # raise_for_status() lança uma exceção se o status HTTP for 4xx ou 5xx.
        # Assim não precisamos checar manualmente se resp.status_code == 200.
        resp.raise_for_status()
        # Retornamos o JSON da resposta já convertido para lista Python.
        return resp.json()
    except (requests.ConnectionError, requests.Timeout, requests.HTTPError):
        # Se qualquer erro de rede ou HTTP acontecer, geramos dados falsos.
        # Isso é o padrão "graceful degradation": o script não quebra,
        # apenas usa dados simulados para continuar funcionando.
        return _mock_records(execution_date)


def _mock_records(execution_date: str) -> list:
    """
    Gera registros falsos de manutenção para testes e desenvolvimento.

    Usamos o prefixo _ na função para indicar que ela é "privada",
    ou seja, é um detalhe de implementação — não deve ser chamada de fora.
    """
    # Definimos listas de valores possíveis para simular dados reais.
    # random.choice() vai sortear um valor aleatório de cada lista.
    factories = ["FAB-SP-01", "FAB-RJ-01", "FAB-MG-01"]
    statuses = ["scheduled", "in_progress", "completed", "cancelled"]
    types = ["preventive", "corrective", "inspection"]

    # Usamos list comprehension para gerar entre 10 e 30 registros de forma compacta.
    # O `for _ in range(...)` significa que não precisamos do índice do loop,
    # então usamos _ como convenção para "variável descartável".
    return [
        {
            "id": f"MNT-{random.randint(10000, 99999)}",
            # :04d formata o número com 4 dígitos, preenchendo com zeros: EQ-0012
            "equipment_id": f"EQ-{random.randint(1, 50):04d}",
            "factory_id": random.choice(factories),
            "maintenance_type": random.choice(types),
            "status": random.choice(statuses),
            "scheduled_date": execution_date,
            "technician": f"Tech-{random.randint(1,10):02d}",
            # round(..., 1) arredonda para 1 casa decimal, simulando horas reais
            "estimated_duration_hours": round(random.uniform(1, 8), 1),
        }
        for _ in range(random.randint(10, 30))
    ]


def run(execution_date: Optional[str] = None):
    """
    Função principal: orquestra o processo completo de ingestão.

    Etapas:
    1. Define a data de execução (hoje se não informada)
    2. Garante que o bucket Bronze existe no MinIO
    3. Busca os registros da API (ou fallback mock)
    4. Salva os dados no MinIO em formato JSON linha a linha (JSONL)
    """
    # Se nenhuma data for passada, usamos a data de hoje no formato ISO 8601 (YYYY-MM-DD).
    # Isso permite rodar o script manualmente com uma data específica para reprocessamento.
    if execution_date is None:
        execution_date = date.today().isoformat()

    # Criamos o cliente S3/MinIO para usar nas próximas operações.
    s3 = get_s3()

    # Verificamos se o bucket "bronze" já existe antes de tentar criar.
    # list_buckets() retorna todos os buckets do MinIO nessa conta.
    # Extraímos só os nomes com list comprehension para facilitar a checagem.
    buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if BUCKET not in buckets:
        # Se o bucket não existe, criamos agora. Isso é idempotente:
        # não vai quebrar se rodarmos mais de uma vez.
        s3.create_bucket(Bucket=BUCKET)

    # Buscamos os registros da API ou dados simulados como fallback.
    records = fetch_records(execution_date)

    # Saída antecipada: se não há registros, não faz sentido gravar um arquivo vazio.
    if not records:
        print(f"Sem registros de manutencao para {execution_date}")
        return

    # Capturamos o horário exato em UTC para usar no nome do arquivo.
    # Usar UTC é importante para evitar problemas com fusos horários diferentes.
    now = datetime.now(tz=timezone.utc)

    # Montamos o "path" do arquivo no MinIO usando particionamento por data (Hive-style).
    # Exemplo: maintenance/year=2024/month=01/day=15/records_143022.json
    # Essa estrutura permite que ferramentas como Spark e Athena leiam
    # apenas as partições necessárias, sem varrer todos os dados — muito mais eficiente!
    key = (
        f"maintenance/year={now.year}/month={now.month:02d}/day={now.day:02d}"
        f"/records_{now.strftime('%H%M%S')}.json"
    )

    # Gravamos os dados no MinIO. Cada registro fica em uma linha separada (JSONL).
    # O formato JSONL (JSON Lines) é melhor que um JSON array gigante porque
    # permite leitura linha a linha, o que é mais eficiente com grandes volumes.
    # encode() converte a string para bytes, que é o que o S3/MinIO espera.
    s3.put_object(Bucket=BUCKET, Key=key, Body=("\n".join(json.dumps(r) for r in records)).encode())

    print(f"OK {len(records)} registros manutencao -> s3://{BUCKET}/{key}")


# Esse bloco só executa quando rodamos o arquivo diretamente (python maintenance_api_ingestion.py).
# Se o arquivo for importado por outro módulo, esse trecho é ignorado.
# É um padrão muito comum em Python para deixar o código reutilizável e testável.
if __name__ == "__main__":
    run()
