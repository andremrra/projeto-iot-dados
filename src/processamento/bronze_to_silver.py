# =============================================================================
# CAMADA BRONZE → SILVER
# =============================================================================
# Aqui a gente faz a primeira limpeza dos dados brutos que chegaram do MinIO.
# No padrão Medalhão (Bronze/Silver/Gold), a camada Bronze guarda tudo como veio,
# e a Silver já tem os dados limpos, tipados e sem duplicatas.
# Pensa assim: Bronze é o dado "cru", Silver é o dado "lavado e organizado".
# =============================================================================

import os
import sys

# PySpark é o framework que permite processar grandes volumes de dados
# de forma distribuída. SparkSession é o ponto de entrada de qualquer job Spark.
from pyspark.sql import SparkSession

# Aqui importamos funções prontas do Spark para transformar colunas.
# col → referencia uma coluna pelo nome
# to_timestamp → converte string para tipo data/hora
# trim → remove espaços em branco das bordas (como .strip() do Python)
# lower → converte texto para minúsculas
# year, month, dayofmonth → extraem partes de uma data
from pyspark.sql.functions import col, to_timestamp, trim, lower, year, month, dayofmonth

# StructType e StructField servem para definir o schema (estrutura) do dado
# antes de ler — tipo um "contrato" dizendo quais colunas existem e seus tipos.
# Isso evita que o Spark tente adivinhar os tipos, o que pode ser lento e impreciso.
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, LongType,
)

# Lemos a URL do MinIO de uma variável de ambiente.
# O MinIO é um serviço de armazenamento compatível com S3 da AWS.
# Se a variável não existir, usamos o valor padrão "http://minio:9000"
# (que é o endereço do serviço dentro do Docker Compose).
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

# Definimos o schema do dado de sensor de forma explícita.
# Por que isso é importante? Sem schema definido, o Spark lê tudo como string
# e pode errar os tipos — por exemplo, tratar um número como texto.
# Com o schema, garantimos que "value" seja Double (decimal), "is_anomaly" seja Boolean, etc.
# O campo "metadata" é um objeto aninhado (nested), por isso usamos StructType dentro de StructType.
SENSOR_SCHEMA = StructType([
    StructField("event_id", StringType(), True),         # ID único do evento
    StructField("sensor_id", StringType(), True),        # Qual sensor gerou o dado
    StructField("equipment_id", StringType(), True),     # Qual equipamento está sendo monitorado
    StructField("factory_id", StringType(), True),       # Em qual fábrica
    StructField("measurement_type", StringType(), True), # Tipo de medição (temperatura, pressão, etc.)
    StructField("value", DoubleType(), True),            # Valor numérico medido (decimal de precisão dupla)
    StructField("unit", StringType(), True),             # Unidade da medição (graus, bar, etc.)
    StructField("timestamp", StringType(), True),        # Horário do evento — vem como string, vamos converter depois
    StructField("quality", StringType(), True),          # Qualidade do sinal (good, bad, uncertain...)
    StructField("is_anomaly", BooleanType(), True),      # True se o sensor detectou anomalia
    StructField("metadata", StructType([                 # Metadados do sensor (objeto aninhado)
        StructField("firmware_version", StringType(), True),   # Versão do firmware do sensor
        StructField("battery_level", LongType(), True),        # Nível de bateria
        StructField("signal_strength", LongType(), True),      # Força do sinal
    ]), True),
])

# Criamos a SparkSession — ela é o "motor" do job.
# O builder usa o padrão fluent (encadeamento de métodos), então cada .config() adiciona uma configuração.
# As configs de "hadoop" fazem o Spark conseguir ler arquivos do MinIO como se fosse um S3.
# O protocolo usado é s3a (versão mais nova e performática do conector S3 para Hadoop).
spark = (
    SparkSession.builder
    .appName("BronzeToSilver")  # Nome do job — aparece na interface do Spark
    # Pacotes Maven necessários para conectar ao MinIO via protocolo S3A
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)          # Endereço do MinIO
    .config("spark.hadoop.fs.s3a.access.key", "admin")               # Usuário do MinIO
    .config("spark.hadoop.fs.s3a.secret.key", "password")            # Senha do MinIO
    .config("spark.hadoop.fs.s3a.path.style.access", "true")         # Necessário para MinIO local (não usa subdomínios)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")  # Classe que implementa o conector
    .getOrCreate()  # Cria uma nova sessão ou reutiliza uma já existente
)

# Lemos todos os arquivos JSON do bucket "bronze" de forma recursiva.
# "recursiveFileLookup" faz o Spark entrar em todas as subpastas para encontrar arquivos.
# Usamos o schema que definimos acima — dados ERP ou outros formatos que não seguem
# esse schema simplesmente terão campos nulos, e serão filtrados logo abaixo.
df = (
    spark.read
    .option("recursiveFileLookup", "true")  # Lê subpastas automaticamente
    .schema(SENSOR_SCHEMA)                  # Aplica o schema de sensor
    .json("s3a://bronze/")                  # Caminho do bucket Bronze no MinIO
)

# Filtramos apenas registros que têm sensor_id — isso descarta dados de outros sistemas
# (como ERP) que possam estar no bucket Bronze mas não são dados de sensor.
# Se sensor_id for nulo, o registro não é do nosso pipeline de sensores IoT.
df = df.filter(col("sensor_id").isNotNull())

# Verificação de segurança: se não há dados de sensores, não faz sentido continuar.
# rdd.isEmpty() verifica se o DataFrame está vazio (cuidado: isso aciona uma ação no Spark,
# ou seja, é um ponto de materialização — pode ser lento em datasets grandes).
if df.rdd.isEmpty():
    print("Nenhum dado de sensores no Bronze.")
    sys.exit(0)  # Encerra o script com código 0 (sucesso, mas sem dados para processar)

# Aqui fazemos as transformações principais — o "coração" da camada Silver.
# Usamos withColumn() para criar ou sobrescrever colunas.
# Cada transformação tem um propósito específico de limpeza ou padronização.
df = (
    df
    # Converte o timestamp de string para tipo Timestamp do Spark.
    # Isso permite operações de data/hora como filtrar por período, agrupar por hora, etc.
    .withColumn("timestamp", to_timestamp(col("timestamp")))

    # Remove espaços extras dos IDs — dados de sensores às vezes vêm com espaços
    # no início/fim que causariam registros duplicados em joins e filtros.
    .withColumn("sensor_id", trim(col("sensor_id")))
    .withColumn("equipment_id", trim(col("equipment_id")))
    .withColumn("factory_id", trim(col("factory_id")))

    # Normaliza texto para minúsculas E remove espaços.
    # Assim "Temperatura", "TEMPERATURA" e "temperatura " viram todos "temperatura".
    # Isso é fundamental para consistência em agrupamentos e filtros.
    .withColumn("measurement_type", lower(trim(col("measurement_type"))))
    .withColumn("unit", lower(trim(col("unit"))))
    .withColumn("quality", lower(trim(col("quality"))))

    # "Desaninha" (flatten) os campos do objeto metadata para colunas separadas.
    # Trabalhar com colunas planas é muito mais simples do que com structs aninhados.
    # Usamos col("metadata.firmware_version") para acessar o campo dentro do struct.
    .withColumn("firmware_version", col("metadata.firmware_version"))
    .withColumn("battery_level", col("metadata.battery_level").cast("int"))   # Converte LongType → IntType (economia de espaço)
    .withColumn("signal_strength", col("metadata.signal_strength").cast("int"))

    # Remove a coluna metadata original — já extraímos o que precisávamos
    .drop("metadata")

    # Filtra registros inválidos: descartamos qualquer linha que não tenha
    # event_id, value ou timestamp — esses três campos são essenciais para o dado ser útil.
    .filter(
        col("event_id").isNotNull()
        & col("value").isNotNull()
        & col("timestamp").isNotNull()
    )

    # Remove duplicatas pelo event_id — cada evento deve aparecer uma única vez.
    # Sem isso, poderíamos ter o mesmo sensor registrado duas vezes, distorcendo agregações.
    .dropDuplicates(["event_id"])

    # Criamos colunas de particionamento (year, month, day) a partir do timestamp.
    # Isso vai ser usado na hora de salvar para organizar os arquivos em pastas por data.
    # Facilita muito buscas como "quero apenas dados de março de 2024".
    .withColumn("year", year("timestamp"))
    .withColumn("month", month("timestamp"))
    .withColumn("day", dayofmonth("timestamp"))
)

# Salvamos o DataFrame processado no bucket Silver em formato JSON.
# coalesce(4) reduz o número de partições para 4 antes de salvar.
# Por padrão o Spark pode gerar centenas de arquivos pequenos — isso é ruim para leitura.
# Forçar 4 partições gera 4 arquivos de tamanho mais razoável.
# mode("overwrite") substitui os dados existentes — cuidado: em produção isso apaga tudo que estava lá!
df.coalesce(4).write \
    .mode("overwrite") \
    .json("s3a://silver/sensor_data")

print("Silver salvo: s3a://silver/")
