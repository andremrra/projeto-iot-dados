# Esse import é necessário para garantir compatibilidade com versões antigas do Python.
# O "from __future__ import annotations" faz com que as anotações de tipo sejam avaliadas
# de forma "lazy" (preguiçosa), ou seja, só quando realmente necessário. É boa prática
# incluir isso em projetos que precisam rodar em diferentes versões do Python.
from __future__ import annotations

# Importamos datetime para definir a data de início da DAG e timedelta para
# configurar o intervalo de retry (quanto tempo esperar antes de tentar de novo).
from datetime import datetime, timedelta

# Aqui importamos o objeto DAG do Airflow — ele é como o "esqueleto" do nosso pipeline.
# Sem ele, o Airflow não sabe o que executar nem quando.
from airflow import DAG

# BashOperator é o operador que nos permite rodar comandos de terminal (bash) dentro
# do Airflow. Vamos usá-lo para disparar o spark-submit, que executa nossos jobs Spark.
from airflow.operators.bash import BashOperator

# default_args são os argumentos padrão que serão herdados por todas as tasks da DAG.
# Isso evita repetição: em vez de configurar retry em cada task, definimos aqui uma vez.
# - "owner": quem é responsável por essa DAG (útil para organização em times)
# - "retries": quantas vezes o Airflow vai tentar reexecutar uma task que falhou
# - "retry_delay": quanto tempo esperar entre as tentativas (aqui, 5 minutos)
# - "email_on_failure": desabilitamos o envio de e-mail em caso de falha
default_args = {
    "owner": "iot-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# Aqui montamos o comando base do spark-submit. Usamos uma string longa dividida
# em partes para facilitar a leitura — Python une automaticamente strings adjacentes
# entre parênteses.
#
# O que cada parte faz:
# - "docker exec spark-master": entra no container Docker chamado "spark-master"
# - "/opt/spark/bin/spark-submit": caminho do executável do Spark dentro do container
# - "--master spark://spark-master:7077": diz ao Spark para usar o cluster no modo standalone
# - "--conf spark.jars.ivy=...": define onde o Spark vai baixar dependências (JARs)
# - "--conf spark.hadoop.fs.s3a.connection.timeout=...": timeout de conexão com o S3 (30s)
# - "--conf spark.hadoop.fs.s3a.socket.timeout=...": timeout do socket com o S3 (30s)
# - "--conf spark.hadoop.fs.s3a.retry.limit=5": tenta reconectar ao S3 até 5 vezes
# - "--packages ...": baixa os JARs necessários para o Spark ler/escrever no S3 (hadoop-aws)
SPARK_SUBMIT = (
    "docker exec spark-master /opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.jars.ivy=/tmp/ivy2 "
    "--conf spark.hadoop.fs.s3a.connection.timeout=30000 "
    "--conf spark.hadoop.fs.s3a.socket.timeout=30000 "
    "--conf spark.hadoop.fs.s3a.retry.limit=5 "
    "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
)

# Aqui criamos a DAG usando o gerenciador de contexto "with". Tudo que for definido
# dentro desse bloco pertence a essa DAG automaticamente — não precisa passar o objeto
# "dag" como argumento para cada task.
#
# Parâmetros da DAG:
# - dag_id: nome único da DAG no Airflow (aparece na interface web)
# - default_args: os argumentos padrão que definimos acima
# - description: descrição curta do que essa DAG faz (só para documentação)
# - schedule="@daily": executa uma vez por dia (à meia-noite UTC por padrão)
# - start_date: a partir de quando o Airflow considera essa DAG ativa
# - catchup=False: IMPORTANTE! Sem isso, o Airflow tentaria executar a DAG
#   retroativamente desde o start_date. Com False, ele ignora o passado.
# - tags: etiquetas para filtrar DAGs na interface web
with DAG(
    dag_id="iot_lakehouse_pipeline",
    default_args=default_args,
    description="Pipeline diário Bronze → Silver → Gold",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["iot", "lakehouse", "spark"],
) as dag:

    # Primeira task: transforma os dados brutos (Bronze) em dados limpos (Silver).
    # A camada Bronze contém os dados "crus" como vieram da fonte (IoT, ERP, API).
    # A camada Silver aplica limpeza, tipagem correta e remoção de duplicatas.
    # O BashOperator executa o script Python via spark-submit dentro do container Docker.
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=SPARK_SUBMIT + "/opt/spark/jobs/bronze_to_silver.py",
    )

    # Segunda task: transforma os dados limpos (Silver) em dados agregados (Gold).
    # A camada Gold contém métricas e indicadores prontos para consumo por dashboards
    # e modelos de Machine Learning. É aqui que o "valor de negócio" é gerado.
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=SPARK_SUBMIT + "/opt/spark/jobs/silver_to_gold.py",
    )

    # Aqui definimos a DEPENDÊNCIA entre as tasks usando o operador ">>".
    # Isso garante que o silver_to_gold só vai executar DEPOIS que o bronze_to_silver
    # terminar com sucesso. Se bronze_to_silver falhar, o silver_to_gold nem começa.
    # Essa é a forma mais intuitiva de definir fluxo no Airflow — parece uma seta!
    bronze_to_silver >> silver_to_gold
