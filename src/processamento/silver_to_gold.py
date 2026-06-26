# =============================================================================
# CAMADA SILVER → GOLD
# =============================================================================
# Esta etapa transforma dados limpos (Silver) em métricas agregadas (Gold).
# No padrão Medalhão, a camada Gold é a mais refinada: dados prontos para análise,
# dashboards e modelos de Machine Learning.
# Aqui calculamos estatísticas por equipamento, fábrica e tipo de medição,
# agrupando por hora — útil para detectar padrões e anomalias ao longo do tempo.
# =============================================================================

import os

# SparkSession é o ponto de entrada de qualquer aplicação Spark
from pyspark.sql import SparkSession

# Importamos funções de agregação e de manipulação de data/hora do Spark.
# avg → média
# min, max → mínimo e máximo
# stddev → desvio padrão (mede o quanto os valores variam em torno da média)
# count → contagem de registros
# sum → soma (importado com alias "spark_sum" para não conflitar com o built-in do Python)
# date_trunc → trunca um timestamp para uma granularidade (ex: "hour" zera minutos e segundos)
# dayofmonth, hour → extraem partes de uma data
# to_timestamp → converte string para tipo timestamp
from pyspark.sql.functions import (
    col, avg, min, max, stddev, count,
    sum as spark_sum, date_trunc, dayofmonth, hour, to_timestamp,
)

# Lemos o endpoint do MinIO de variável de ambiente.
# Isso é boa prática: configurações sensíveis não ficam hard-coded no código.
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

# Criamos a SparkSession com as configurações para acessar o MinIO.
# O appName "SilverToGold" ajuda a identificar este job na interface do Spark.
spark = (
    SparkSession.builder
    .appName("SilverToGold")
    # Pacotes para conectar ao MinIO via protocolo S3A (compatível com AWS S3)
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)        # URL do MinIO
    .config("spark.hadoop.fs.s3a.access.key", "admin")             # Credenciais de acesso
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")       # Formato de URL do MinIO (não usa subdomínio)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# Lemos os dados já limpos da camada Silver.
# O Spark infere o schema automaticamente a partir dos arquivos JSON salvos anteriormente.
df = spark.read.json("s3a://silver/sensor_data")

# Re-convertemos o timestamp para o tipo correto.
# Ao salvar como JSON e reler, timestamps podem virar strings novamente —
# por isso é necessário converter de volta antes de usar funções de data.
df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Criamos colunas auxiliares para facilitar o agrupamento por hora.
# date_trunc("hour", timestamp) → transforma "2024-03-15 14:37:22" em "2024-03-15 14:00:00"
# Isso é a chave de agrupamento temporal: queremos uma linha por hora por equipamento.
# day e hour em separado facilitam filtros futuros no dashboard (ex: "mostrar hora 14").
df = (
    df
    .withColumn("timestamp_hour", date_trunc("hour", col("timestamp")))  # Agrupa na hora exata
    .withColumn("day", dayofmonth("timestamp"))    # Dia do mês (1-31) — para filtros e particionamento
    .withColumn("hour", hour("timestamp"))         # Hora do dia (0-23) — para análise de padrões horários
)

# Aqui está o passo mais importante deste script: a AGREGAÇÃO.
# groupBy define as chaves de agrupamento — cada combinação única dessas colunas
# gera uma linha no resultado final.
# Por exemplo: equipamento "EQ001" + fábrica "FAB_SP" + medição "temperatura" + hora "14:00" = 1 linha
gold = df.groupBy(
    "equipment_id",      # Qual equipamento (ex: motor, bomba, compressor)
    "factory_id",        # Em qual fábrica está o equipamento
    "measurement_type",  # Tipo de medição (temperatura, pressão, vibração...)
    "timestamp_hour",    # Hora do evento (truncada)
    "year",              # Ano — herdado do Silver para manter particionamento
    "month",             # Mês — idem
    "day",               # Dia — calculado acima
    "hour",              # Hora — calculado acima
).agg(
    # Para cada grupo, calculamos um conjunto de estatísticas descritivas:
    avg("value").alias("avg_value"),       # Média dos valores no período — tendência central
    min("value").alias("min_value"),       # Menor valor registrado — útil para detectar quedas bruscas
    max("value").alias("max_value"),       # Maior valor registrado — útil para detectar picos
    stddev("value").alias("std_value"),    # Desvio padrão — mede a variabilidade/instabilidade do sensor
    count("*").alias("event_count"),       # Total de leituras no período — detecta falhas de comunicação

    # Conta quantas leituras foram marcadas como anomalia.
    # is_anomaly é Boolean, então precisamos converter para int (True=1, False=0) antes de somar.
    spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
)

# Calculamos a TAXA DE ANOMALIA: proporção de leituras anômalas no período.
# anomaly_rate = anomaly_count / event_count → valor entre 0 e 1
# Ex: 0.25 significa que 25% das leituras daquele equipamento naquela hora foram anomalias.
gold = gold.withColumn("anomaly_rate", col("anomaly_count") / col("event_count"))

# Calculamos o CRITICALITY SCORE: uma métrica composta que combina dois fatores de risco.
# Formula: (anomaly_rate * 0.6) + (coeficiente_de_variação * 0.4)
#
# Componente 1 — anomaly_rate * 0.6:
#   Taxa de anomalias tem peso maior (60%) porque indica problemas já detectados.
#
# Componente 2 — (std_value / avg_value) * 0.4:
#   É o Coeficiente de Variação (CV): mede a variabilidade relativa dos valores.
#   Um CV alto significa que os valores oscilam muito em relação à média,
#   o que pode indicar instabilidade mesmo sem anomalias explícitas.
#   Tem peso menor (40%) porque é um sinal mais indireto de problema.
#
# O resultado final é um score de criticidade para priorizar manutenção preventiva.
gold = gold.withColumn(
    "criticality_score",
    (col("anomaly_rate") * 0.6) + (col("std_value") / col("avg_value") * 0.4),
)

# Salvamos a camada Gold no MinIO em formato Parquet.
# Por que Parquet e não JSON?
#   - Parquet é um formato colunar: muito mais eficiente para leitura analítica
#   - Compressão automática: arquivos menores
#   - Schema embutido: não precisa re-inferir tipos na leitura
#
# partitionBy organiza os arquivos em pastas por ano/mês/fábrica:
#   Ex: gold/equipment_metrics_hourly/year=2024/month=3/factory_id=FAB_SP/part-0000.parquet
# Isso acelera consultas filtradas por período ou fábrica — o Spark só lê as partições necessárias.
gold.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "factory_id") \
    .parquet("s3a://gold/equipment_metrics_hourly/")

print("Camada Gold salva no MinIO: s3a://gold/equipment_metrics_hourly/")
