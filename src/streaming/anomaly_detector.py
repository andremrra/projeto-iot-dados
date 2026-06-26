# ============================================================
# DETECTOR DE ANOMALIAS EM STREAMING - PySpark + Kafka
# ============================================================
# Esse script implementa um pipeline de processamento em tempo real.
# Ele lê dados de sensores IoT do Kafka, calcula a taxa de anomalias
# em janelas de tempo de 5 minutos, classifica o nível de alerta
# e grava os resultados no MinIO e no console.
#
# Fluxo: Kafka (iot-sensors) -> PySpark Streaming -> MinIO (alerts)
# ============================================================

# os: para ler variáveis de ambiente com as configurações de conexão
import os

# SparkSession: ponto de entrada para qualquer aplicação PySpark
from pyspark.sql import SparkSession

# Importamos as funções de transformação que vamos usar no pipeline.
# col: referencia uma coluna pelo nome
# from_json: desserializa uma string JSON para uma struct tipada
# window: cria janelas de tempo para agregações (muito usado em streaming)
# count: conta registros
# spark_sum: soma valores (renomeado para não conflitar com o sum() do Python)
# avg: calcula a média
# when: lógica condicional (similar ao CASE WHEN do SQL)
from pyspark.sql.functions import (
    col, from_json, window, count, sum as spark_sum,
    avg, when,
)

# Importamos os tipos de dados para definir o schema do nosso JSON.
# É importante definir o schema manualmente em streaming: evita o custo
# de inferência automática e garante consistência nos dados.
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType,
)

# Lemos os endereços do Kafka e do MinIO a partir de variáveis de ambiente.
# Em Docker Compose, "kafka:29092" e "minio:9000" são os nomes dos serviços na rede interna.
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

# ============================================================
# DEFINIÇÃO DO SCHEMA DOS EVENTOS DE SENSOR
# ============================================================
# O Kafka envia as mensagens como bytes/string. Precisamos dizer ao Spark
# qual é a estrutura esperada do JSON para que ele consiga parsear corretamente.
# Sem isso, o Spark trataria tudo como texto — não conseguiríamos fazer cálculos.
SENSOR_SCHEMA = StructType([
    StructField("event_id", StringType()),        # ID único do evento
    StructField("sensor_id", StringType()),        # Qual sensor gerou o dado
    StructField("equipment_id", StringType()),     # Qual equipamento está sendo monitorado
    StructField("factory_id", StringType()),       # Em qual fábrica está o equipamento
    StructField("measurement_type", StringType()), # Tipo de medição: temperatura, pressão etc.
    StructField("value", DoubleType()),            # O valor medido (número decimal)
    StructField("unit", StringType()),             # Unidade: °C, bar, rpm etc.
    StructField("timestamp", StringType()),        # Timestamp como string — depois convertemos
    StructField("quality", StringType()),          # Qualidade do sinal do sensor
    StructField("is_anomaly", BooleanType()),      # Flag: o sensor já detectou anomalia?
])

# ============================================================
# CRIAÇÃO DA SESSÃO SPARK
# ============================================================
# A SparkSession é o coração de qualquer aplicação PySpark.
# Aqui configuramos tudo que o Spark precisa para:
# 1. Ler dados do Kafka (conector kafka-sql)
# 2. Gravar dados no MinIO via protocolo S3A (hadoop-aws + aws-java-sdk)
spark = (
    SparkSession.builder
    .appName("IoTAnomalyDetector")
    # spark.jars.packages instrui o Spark a baixar automaticamente as dependências necessárias.
    # spark-sql-kafka: conector para ler/escrever no Kafka com Spark Structured Streaming
    # hadoop-aws: implementação do sistema de arquivos S3A para o Hadoop/Spark
    # aws-java-sdk-bundle: SDK Java da AWS que o hadoop-aws precisa internamente
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )
    # Configurações do S3A para apontar para o MinIO local em vez da AWS real.
    # path.style.access=true é necessário para o MinIO: ele não suporta virtual-hosted style.
    # (Virtual-hosted seria: bucket.minio:9000 — o MinIO precisa: minio:9000/bucket)
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# Reduzimos o nível de log para WARN para não poluir o terminal com mensagens do Spark.
# Em produção isso é essencial: o Spark gera muito log no nível INFO/DEBUG.
spark.sparkContext.setLogLevel("WARN")

# ============================================================
# LEITURA DO KAFKA (SOURCE)
# ============================================================
# Aqui criamos um DataFrame de streaming que lê do tópico "iot-sensors".
# startingOffsets="latest" significa: ignorar mensagens antigas,
# processar apenas as que chegarem a partir de agora.
# failOnDataLoss=false: não quebra se o Kafka perder dados por retenção expirada.
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", "iot-sensors")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# ============================================================
# PARSING DO JSON
# ============================================================
# O Kafka entrega os dados no campo "value" como bytes.
# Precisamos:
# 1. Converter "value" de bytes para string (.cast("string"))
# 2. Parsear o JSON com from_json() usando o schema que definimos acima
# 3. "Explodir" a struct com select("d.*") para ter colunas individuais
# 4. Criar a coluna event_ts convertendo o timestamp string para TimestampType
#    (necessário para usar em janelas de tempo)
events = (
    raw
    .select(from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("d"))
    .select("d.*")
    .withColumn("event_ts", col("timestamp").cast(TimestampType()))
)

# ============================================================
# AGREGAÇÃO COM JANELA DE TEMPO DESLIZANTE
# ============================================================
# Aqui está o coração do detector: calculamos métricas por janelas de 5 minutos.
# Janelas de tempo são essenciais em streaming para agregar eventos que chegam
# continuamente — é como fazer um GROUP BY no tempo.
#
# Watermark de 10 minutos: diz ao Spark para aguardar até 10 minutos por eventos
# atrasados (late data). Eventos que chegarem com mais de 10 min de atraso
# são descartados. Sem watermark, o Spark guardaria estado na memória para sempre!
#
# Agrupamos por: janela de tempo + fábrica + equipamento + tipo de medição.
# Isso nos dá, por exemplo: "quantas anomalias o equipamento EQ-0012 da FAB-SP-01
# teve em medições de temperatura nos últimos 5 minutos?"
windowed = (
    events
    .withWatermark("event_ts", "10 minutes")
    .groupBy(
        window("event_ts", "5 minutes"),  # Janela deslizante de 5 minutos
        col("factory_id"),
        col("equipment_id"),
        col("measurement_type"),
    )
    .agg(
        # Contamos todos os eventos na janela
        count("*").alias("event_count"),
        # Somamos as anomalias: cast("int") converte True->1, False->0
        spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
        # Calculamos a média dos valores medidos na janela
        avg("value").alias("avg_value"),
    )
    # Taxa de anomalia = anomalias / total de eventos (valor entre 0 e 1)
    # Ex: 3 anomalias em 10 eventos = taxa 0.3 (30%)
    .withColumn("anomaly_rate", col("anomaly_count") / col("event_count"))
    # Classificamos o nível de alerta com base na taxa de anomalias:
    # >= 30% de anomalias -> CRÍTICO (precisa de ação imediata!)
    # >= 10% de anomalias -> AVISO (situação preocupante, monitorar)
    # < 10% de anomalias -> NORMAL (tudo bem, filtramos fora logo abaixo)
    .withColumn(
        "alert_level",
        when(col("anomaly_rate") >= 0.3, "critical")
        .when(col("anomaly_rate") >= 0.1, "warning")
        .otherwise("normal"),
    )
    # Filtramos apenas os alertas reais — não precisamos gravar o que está normal.
    # Isso reduz o volume de dados gravados e foca nos casos que precisam de atenção.
    .filter(col("alert_level") != "normal")
)

# ============================================================
# SINK 1: GRAVAÇÃO NO MINIO (persistência)
# ============================================================
# Aqui configuramos o streaming para gravar os alertas no MinIO.
# outputMode="append": só grava linhas novas (as finalizadas pela watermark).
# format="json": cada micro-batch vira arquivos JSON no MinIO.
# partitionBy="factory_id": os arquivos são organizados por fábrica,
#   o que acelera queries futuras quando filtramos por fábrica específica.
# trigger(processingTime="1 minute"): processa e grava a cada 1 minuto.
#   Sem trigger, o Spark processaria o mais rápido possível (modo "as fast as possible").
# checkpointLocation: guarda o estado do streaming no MinIO para que,
#   se o job reiniciar, ele continue de onde parou (fault tolerance).
alert_query = (
    windowed.writeStream
    .outputMode("append")
    .format("json")
    .option("path", "s3a://alerts/streaming/")
    .option("checkpointLocation", "s3a://checkpoints/alerts/")
    .partitionBy("factory_id")
    .trigger(processingTime="1 minute")
    .start()
)

# ============================================================
# SINK 2: EXIBIÇÃO NO CONSOLE (monitoramento ao vivo)
# ============================================================
# Além de gravar no MinIO, também exibimos os alertas no terminal
# para monitoramento em tempo real durante desenvolvimento e testes.
# truncate=False: exibe o conteúdo completo das colunas sem cortar.
# Esse sink é ótimo para debug, mas em produção normalmente não usamos.
console_query = (
    windowed.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .trigger(processingTime="1 minute")
    .start()
)

# awaitTermination() bloqueia o script principal até que o streaming seja interrompido.
# Sem isso, o script terminaria imediatamente sem processar nenhum dado.
# É a linha que mantém a aplicação "viva" enquanto o streaming está rodando.
# O streaming só para se houver um erro ou se o processo for morto manualmente (Ctrl+C).
alert_query.awaitTermination()
