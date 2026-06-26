# Aqui importamos o KafkaConsumer, que é a classe responsável por "escutar" um tópico no Kafka
# e receber as mensagens que foram publicadas lá. Pensa no Kafka como uma fila de mensagens.
from kafka import KafkaConsumer

# boto3 é o SDK da AWS para Python. Aqui usamos ele para falar com o MinIO,
# que é um armazenamento de objetos compatível com S3 (mesma API, mas roda local).
import boto3
from botocore.client import Config

# json para serializar/desserializar os eventos (transformar dict em string e vice-versa)
import json

# datetime e timezone para gerar timestamps no padrão UTC ao salvar os arquivos
from datetime import datetime, timezone

# io não está sendo usado diretamente aqui, mas é comum em scripts que lidam com streams de bytes
import io

# ---- Configurações de conexão com o MinIO (nosso "S3 local") ----
# O MinIO roda em localhost:9000 no ambiente de desenvolvimento via Docker
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"

# O bucket "bronze" é a primeira camada da arquitetura Medallion (Bronze > Silver > Gold)
# Aqui guardamos os dados brutos, sem nenhuma transformação
BUCKET = "bronze"

# Define quantos eventos acumulamos antes de salvar um arquivo no MinIO.
# Isso evita criar milhares de arquivos minúsculos (problema de "small files").
# Com ~500 bytes por evento, 1000 eventos = ~500KB por arquivo, um tamanho razoável.
BATCH_SIZE = 1000  # eventos por arquivo (~500KB por flush)


def get_s3_client():
    # Criamos um cliente boto3 apontando para o MinIO em vez da AWS real.
    # O truque é passar o endpoint_url com o endereço do MinIO.
    # signature_version="s3v4" é o padrão moderno de assinatura que o MinIO suporta.
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(s3, bucket: str):
    # Antes de tentar gravar arquivos, precisamos garantir que o bucket existe.
    # Listamos todos os buckets existentes e verificamos se o nosso está lá.
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        # Se não existe, criamos agora. Isso evita erro ao tentar fazer put_object.
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' criado.")


def flush_to_minio(s3, factory_id: str, batch: list[dict]):
    # Esta função pega o lote acumulado de eventos e salva como um arquivo JSON no MinIO.

    # Capturamos o momento atual em UTC para usar no caminho do arquivo.
    # Usar UTC é uma boa prática para evitar problemas com fusos horários.
    now = datetime.now(tz=timezone.utc)

    # Montamos o caminho (key) do arquivo no formato de partição Hive.
    # Isso é muito importante! O formato factory_id=X/year=Y/month=M/day=D
    # permite que ferramentas como Spark, Athena e DuckDB leiam apenas as partições
    # necessárias (partition pruning), sem varrer todos os dados.
    key = (
        f"factory_id={factory_id}"
        f"/year={now.year}/month={now.month:02d}/day={now.day:02d}"
        f"/events_{now.strftime('%H%M%S_%f')}.json"
    )

    # Convertemos a lista de eventos para o formato JSONL (JSON Lines):
    # um evento por linha. Esse formato é ótimo para processamento em batch
    # porque cada linha é independente e pode ser lida linha a linha.
    body = "\n".join(json.dumps(e) for e in batch)

    # Enviamos o arquivo para o MinIO. put_object cria ou sobrescreve o arquivo.
    s3.put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"))
    print(f"✅ {len(batch)} eventos [{factory_id}] → s3://{BUCKET}/{key}")


def main():
    # Ponto de entrada principal do script de streaming.

    # Criamos o cliente S3/MinIO e garantimos que o bucket bronze existe
    s3 = get_s3_client()
    ensure_bucket(s3, BUCKET)

    # Aqui conectamos ao Kafka para consumir mensagens do tópico "iot-sensors".
    # auto_offset_reset="earliest" significa: se não há offset salvo (primeira vez rodando),
    # comece do início do tópico para não perder nenhuma mensagem histórica.
    # value_deserializer faz o decode automático do bytes para dict Python via json.loads.
    consumer = KafkaConsumer(
        "iot-sensors",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    print("📥 Consumindo eventos do Kafka...")

    # Criamos um dicionário de buffers separado por factory_id.
    # Por que separar? Porque assim os arquivos no MinIO ficam organizados por fábrica,
    # facilitando consultas que filtram por uma fábrica específica.
    # O tipo dict[str, list[dict]] indica: chave = factory_id, valor = lista de eventos.
    buffers: dict[str, list[dict]] = {}

    # Loop infinito: o consumer.for sempre aguarda novas mensagens do Kafka.
    # Cada iteração processa UMA mensagem por vez.
    for message in consumer:
        # message.value já vem desserializado como dict (por causa do value_deserializer acima)
        event = message.value

        # Extraímos o factory_id do evento para saber em qual buffer colocar.
        # Se o campo não existir, usamos "unknown" como fallback seguro.
        fid = event.get("factory_id", "unknown")

        # setdefault é um atalho: se a chave não existe no dict, cria uma lista vazia.
        # Depois adiciona o evento nessa lista.
        buffers.setdefault(fid, []).append(event)

        # Verificamos se o buffer desta fábrica já atingiu o tamanho máximo definido.
        # Se sim, hora de salvar no MinIO e limpar o buffer para a próxima rodada.
        if len(buffers[fid]) >= BATCH_SIZE:
            flush_to_minio(s3, fid, buffers[fid])
            # Limpamos o buffer depois de salvar para liberar memória
            buffers[fid].clear()


# Padrão Python: este bloco só executa quando o script é chamado diretamente,
# não quando é importado como módulo por outro arquivo.
if __name__ == "__main__":
    main()
