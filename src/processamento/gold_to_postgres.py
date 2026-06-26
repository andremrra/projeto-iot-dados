# =============================================================================
# CAMADA GOLD → POSTGRESQL
# =============================================================================
# Este é o último passo do pipeline: exportamos as métricas agregadas da camada
# Gold (que estão no MinIO em formato Parquet) para um banco de dados PostgreSQL.
#
# Por que exportar para o PostgreSQL se já temos os dados no MinIO?
#   - O PostgreSQL permite consultas SQL padrão, muito mais acessível para analistas
#   - Ferramentas de BI (Metabase, Grafana, Superset) se conectam facilmente a bancos SQL
#   - É mais fácil compartilhar acesso ao banco do que ao MinIO
#   - Permite criar índices para consultas rápidas em tempo real
#
# Pensa nisso como: MinIO/Parquet = arquivo histórico/backup, PostgreSQL = prateleira de acesso rápido.
# =============================================================================

import os

from pyspark.sql import SparkSession

# -------------------------------------------------------------------------
# CONFIGURAÇÕES DE CONEXÃO
# -------------------------------------------------------------------------
# Lemos as configurações de variáveis de ambiente — boa prática em projetos
# reais para não expor senhas diretamente no código-fonte.

# Endereço do MinIO (onde estão os arquivos Parquet da camada Gold)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

# Host do PostgreSQL — dentro do Docker Compose, o serviço se chama "postgres"
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")

# JDBC é o protocolo padrão do Java/JVM para conexão com bancos de dados.
# O Spark (que roda na JVM) usa JDBC para se conectar ao PostgreSQL.
# Formato: jdbc:postgresql://<host>:<porta>/<nome_do_banco>
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:5432/iot"

# Dicionário com as propriedades de autenticação e o driver JDBC.
# O "driver" indica qual classe Java implementa o protocolo PostgreSQL.
# Essas propriedades são passadas diretamente para o conector JDBC do Spark.
POSTGRES_PROPS = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"  # Driver oficial do PostgreSQL para Java
}

# -------------------------------------------------------------------------
# SPARK SESSION
# -------------------------------------------------------------------------
# Criamos a SparkSession com três conjuntos de dependências:
#   1. hadoop-aws + aws-java-sdk-bundle → para ler do MinIO (protocolo S3A)
#   2. postgresql → para escrever no PostgreSQL via JDBC
# É importante incluir o driver JDBC do PostgreSQL aqui, pois o Spark
# precisa encontrar a classe "org.postgresql.Driver" em tempo de execução.
spark = (
    SparkSession.builder
    .appName("GoldToPostgres")  # Nome do job no Spark UI
    .config("spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.postgresql:postgresql:42.6.0"   # Driver JDBC do PostgreSQL
    )
    # Configurações para acessar o MinIO como se fosse um S3 da AWS
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")       # Necessário para MinIO local
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# -------------------------------------------------------------------------
# LEITURA DA CAMADA GOLD
# -------------------------------------------------------------------------
# Lemos todos os arquivos Parquet da camada Gold.
# O Spark consegue ler partições automaticamente — mesmo que os arquivos estejam
# organizados em subpastas como year=2024/month=3/factory_id=FAB_SP/,
# o resultado é um único DataFrame com todas as linhas.
# As colunas de partição (year, month, factory_id) são reconstruídas automaticamente.
df = spark.read.parquet("s3a://gold/equipment_metrics_hourly/")

# -------------------------------------------------------------------------
# ESCRITA NO POSTGRESQL
# -------------------------------------------------------------------------
# Usamos o método .jdbc() para escrever no banco de dados.
# Parâmetros:
#   - POSTGRES_URL: string de conexão JDBC (definida acima)
#   - "gold_equipment_metrics": nome da tabela que será criada/sobrescrita no banco
#   - properties: dicionário com user, password e driver
#
# mode("overwrite") apaga a tabela existente e recria com os novos dados.
# Alternativas:
#   - "append" → adiciona os dados sem apagar o que já existe
#   - "ignore" → não faz nada se a tabela já existir
#   - "error" → lança erro se a tabela já existir (comportamento padrão)
#
# ATENÇÃO: em produção, "overwrite" pode ser arriscado pois apaga todo o histórico.
# Geralmente usa-se "append" com controle de duplicatas via chave primária no banco.
df.write \
    .mode("overwrite") \
    .jdbc(POSTGRES_URL, "gold_equipment_metrics", properties=POSTGRES_PROPS)

# Ao final, imprimimos quantos registros foram exportados.
# df.count() aciona uma ação no Spark — lê todos os dados para contar.
# Chamamos DEPOIS do write para não executar a ação duas vezes (o Spark é lazy).
print(f"Gold exportado para PostgreSQL: tabela gold_equipment_metrics ({df.count()} registros)")
