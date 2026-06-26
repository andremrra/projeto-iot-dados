# Importações padrão do Python para trabalhar com JSON, variáveis de ambiente,
# números aleatórios (usado para gerar dados de teste) e datas/horários.
import json
import os
import random
from datetime import date, datetime, timezone
from typing import Optional  # Permite declarar que um parâmetro pode ser None

# boto3 é o SDK da AWS, mas aqui usamos para falar com o MinIO (S3 local)
import boto3

# psycopg2 é a biblioteca para conectar Python ao PostgreSQL.
# Usamos ela para extrair os dados do ERP simulado (banco de dados relacional).
import psycopg2
from botocore.client import Config

# ---- Lemos as configurações de conexão a partir de variáveis de ambiente ----
# Usar os.environ.get é uma boa prática: em produção, as credenciais ficam no ambiente
# (não hardcoded no código). O segundo argumento é o valor padrão para desenvolvimento local.
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_DSN = f"host={POSTGRES_HOST} port=5432 dbname=iot user=user password=password"

# Endereço do MinIO também pode variar entre ambientes (local vs Docker vs cloud)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

# Bucket "bronze" = camada de dados brutos (sem transformação) na arquitetura Medallion
BUCKET = "bronze"


def get_s3():
    # Cria o cliente boto3 apontando para o MinIO.
    # A assinatura s3v4 é necessária para autenticação no MinIO.
    # Mesmo que não seja AWS real, passamos region_name para evitar erros de validação.
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id="admin",
        aws_secret_access_key="password",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_schema(conn):
    # Esta função garante que a tabela de eventos do ERP exista no PostgreSQL.
    # Usamos CREATE TABLE IF NOT EXISTS para ser idempotente:
    # pode rodar várias vezes sem dar erro se a tabela já existir.

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS equipment_events (
                id SERIAL PRIMARY KEY,        -- ID auto-incrementado pelo banco
                equipment_id VARCHAR(20) NOT NULL,  -- Código do equipamento (ex: EQ-0001)
                factory_id   VARCHAR(20) NOT NULL,  -- Fábrica onde o equipamento está
                event_type   VARCHAR(50) NOT NULL,  -- Tipo: manutenção preventiva, corretiva, etc.
                description  TEXT,                  -- Descrição textual do evento
                technician   VARCHAR(100),           -- Nome do técnico responsável
                cost         NUMERIC(10,2),          -- Custo do serviço (2 casas decimais)
                event_date   DATE NOT NULL,          -- Data em que o evento ocorreu
                created_at   TIMESTAMPTZ DEFAULT NOW()  -- Quando o registro foi criado no banco
            )
        """)
        # Confirma a transação no banco. Sem commit, as mudanças seriam descartadas.
        conn.commit()


def seed_erp_data(conn):
    # Esta função popula o banco com dados fictícios para simular um ERP real.
    # Isso é muito útil em projetos de estudo onde não temos acesso a dados reais.

    factories = ["FAB-SP-01", "FAB-RJ-01", "FAB-MG-01"]
    event_types = ["preventive_maintenance", "corrective_maintenance", "inspection", "replacement"]

    with conn.cursor() as cur:
        # Verificamos primeiro se já existem dados na tabela.
        # Se existir, pulamos o seed para não duplicar os registros a cada execução.
        cur.execute("SELECT COUNT(*) FROM equipment_events")
        if cur.fetchone()[0] > 0:
            return  # Dados já existem, não precisa inserir de novo

        # Geramos 50 equipamentos (EQ-0001 a EQ-0050)
        for i in range(1, 51):
            eq_id = f"EQ-{i:04d}"  # Formata com zeros à esquerda: EQ-0001, EQ-0042, etc.

            # Distribuímos os equipamentos entre as 3 fábricas usando módulo 3
            fid = factories[i % 3]

            # Cada equipamento tem entre 1 e 5 eventos históricos (aleatório)
            for _ in range(random.randint(1, 5)):
                cur.execute(
                    """INSERT INTO equipment_events
                       (equipment_id, factory_id, event_type, description, technician, cost, event_date)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        eq_id,
                        fid,
                        random.choice(event_types),   # Tipo de evento aleatório
                        f"Manutenção em {eq_id}",
                        f"Tech-{random.randint(1,10):02d}",  # Técnico de Tech-01 a Tech-10
                        round(random.uniform(100, 5000), 2),  # Custo entre R$100 e R$5000
                        # 30% de chance do evento ser hoje, 70% de chance de ser em 2025
                        date.today() if random.random() < 0.3 else date(2025, random.randint(1, 12), random.randint(1, 28)),
                    ),
                )
        # Confirmamos todas as inserções de uma vez (mais eficiente que commit por linha)
        conn.commit()


def extract_and_load(s3, conn, execution_date: str):
    # Esta é a função principal de ETL (Extract, Transform, Load) do ERP.
    # Ela extrai registros do PostgreSQL para uma data específica e salva no MinIO.

    with conn.cursor() as cur:
        # Extraímos do PostgreSQL apenas os eventos da data de execução informada.
        # Isso implementa o padrão de "ingestão incremental por data":
        # a cada dia, só buscamos os registros novos daquele dia.
        # Convertemos event_date e created_at para texto porque JSON não tem tipo date nativo.
        cur.execute(
            """SELECT id, equipment_id, factory_id, event_type, description,
                      technician, cost, event_date::text, created_at::text
               FROM equipment_events WHERE event_date = %s""",
            (execution_date,),
        )
        rows = cur.fetchall()  # Busca todas as linhas de uma vez para a memória

    # Se não houver dados para essa data, apenas avisamos e saímos sem criar arquivo vazio.
    if not rows:
        print(f"Sem registros ERP para {execution_date}")
        return

    # Transformamos as tuplas do banco em dicionários Python (mais legíveis e manipuláveis).
    # Adicionamos o campo "source": "erp" para identificar a origem dos dados na camada bronze.
    # Isso é importante quando múltiplas fontes chegam ao mesmo bucket.
    records = [
        {
            "source": "erp",          # Identificamos a origem do dado
            "id": r[0],
            "equipment_id": r[1],
            "factory_id": r[2],
            "event_type": r[3],
            "description": r[4],
            "technician": r[5],
            "cost": float(r[6]) if r[6] else None,  # Convertemos Decimal para float (JSON-friendly)
            "event_date": r[7],
            "created_at": r[8],
        }
        for r in rows  # List comprehension: gera uma lista de dicts a partir das linhas do banco
    ]

    # Montamos o caminho do arquivo no MinIO seguindo a convenção de partição por data.
    # Colocamos na pasta "erp/" para separar visualmente dos dados IoT do Kafka.
    now = datetime.now(tz=timezone.utc)
    key = (
        f"erp/year={now.year}/month={now.month:02d}/day={now.day:02d}"
        f"/equipment_events_{execution_date}.json"
    )

    # Gravamos os registros no formato JSONL (um JSON por linha) no MinIO.
    # Essa escolha facilita a leitura linha a linha por processadores de dados grandes.
    s3.put_object(Bucket=BUCKET, Key=key, Body=("\n".join(json.dumps(r) for r in records)).encode())
    print(f"OK {len(records)} registros ERP -> s3://{BUCKET}/{key}")


def run(execution_date: Optional[str] = None):
    # Função orquestradora: chama tudo na ordem certa.
    # Recebe a data de execução como parâmetro, útil quando chamado por um orquestrador
    # como Airflow, que passa a data de execução automaticamente.

    # Se não passar uma data, usamos hoje. Isso facilita rodar manualmente sem argumentos.
    if execution_date is None:
        execution_date = date.today().isoformat()  # Formato ISO: "2026-06-26"

    # Cria o cliente S3/MinIO
    s3 = get_s3()

    # Verifica se o bucket bronze existe; se não, cria.
    # Sem isso, o put_object falharia com erro de bucket não encontrado.
    buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if BUCKET not in buckets:
        s3.create_bucket(Bucket=BUCKET)

    # Conectamos ao PostgreSQL usando a connection string montada no início do arquivo
    conn = psycopg2.connect(POSTGRES_DSN)
    try:
        # Passo 1: garante que a tabela existe no banco
        ensure_schema(conn)

        # Passo 2: popula com dados de teste se o banco estiver vazio
        seed_erp_data(conn)

        # Passo 3: extrai os dados do dia e salva no MinIO (bronze layer)
        extract_and_load(s3, conn, execution_date)
    finally:
        # O bloco finally garante que a conexão com o banco seja fechada SEMPRE,
        # mesmo que ocorra um erro no meio do caminho. Evita vazamento de conexões.
        conn.close()


# Ponto de entrada quando o script é executado diretamente pelo terminal.
# Permite também ser importado como módulo e chamar run() de outro lugar (ex: Airflow).
if __name__ == "__main__":
    run()
