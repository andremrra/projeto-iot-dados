# Import de compatibilidade entre versões do Python — boa prática incluir
# em todo arquivo que usa anotações de tipo modernas.
from __future__ import annotations

# Precisamos do sys para manipular o caminho de busca de módulos do Python.
# É o mesmo padrão usado na dag_erp_ingestion.py.
import sys

# datetime e timedelta para trabalhar com datas no Airflow.
from datetime import datetime, timedelta

# DAG é a classe principal do Airflow que representa nosso pipeline/fluxo de trabalho.
from airflow import DAG

# PythonOperator para executar funções Python como tasks do Airflow.
from airflow.operators.python import PythonOperator

# Adicionamos o diretório de código fonte ao path do Python dentro do container.
# Sem isso, o import "from ingestao.maintenance_api_ingestion import run" falharia,
# pois o Python não encontraria o pacote "ingestao" nesse ambiente.
sys.path.insert(0, "/opt/airflow/src")

# Configurações padrão para as tasks desta DAG.
# Aqui temos 3 retries com delay de apenas 5 minutos (metade do ERP!).
# Faz sentido: APIs REST geralmente se recuperam mais rápido do que
# bancos de dados legados ERP. Se a API caiu por um segundo, em 5 minutos
# já voltou. Para o ERP, uma janela maior faz mais sentido por ser mais pesado.
default_args = {
    "owner": "iot-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# Função que encapsula a lógica de ingestão da API de manutenção.
# O padrão **context é o mesmo da dag_erp_ingestion.py: o Airflow injeta
# automaticamente informações sobre a execução atual (data, run_id, etc.)
# nesse dicionário quando a task é chamada.
def run_api_ingestion(**context):
    # Lazy import: importamos o módulo apenas quando a task vai de fato executar,
    # não quando o Airflow está apenas "lendo" as DAGs para montar a interface.
    # Isso evita erros de importação durante o processo de descoberta de DAGs.
    from ingestao.maintenance_api_ingestion import run

    # Passamos a data de execução (context["ds"]) para que o módulo de ingestão
    # saiba qual período de dados buscar na API.
    # Ex: se a DAG rodar às 14h, context["ds"] = "2025-03-15" (a data corrente).
    # Isso garante que a ingestão seja RASTREÁVEL e REPRODUZÍVEL — sabemos
    # exatamente qual dado foi coletado em qual execução.
    run(execution_date=context["ds"])


# Criamos a DAG para ingestão horária da API de manutenção.
#
# A GRANDE diferença desta DAG em relação às outras é o schedule="@hourly"!
# Enquanto as DAGs de ERP e do pipeline Spark rodam uma vez por dia (@daily),
# esta roda a cada hora. Por que? Porque dados de manutenção de equipamentos
# IoT mudam com muita frequência — um sensor pode indicar falha a qualquer momento.
# Se esperássemos 24h para coletar, poderíamos perder alertas críticos.
#
# Implicação prática: essa DAG vai criar 24x mais execuções por dia do que as outras.
# É importante garantir que o módulo de ingestão seja eficiente e não sobrecarregue a API.
with DAG(
    dag_id="maintenance_api_ingestion",
    default_args=default_args,
    description="Ingestão horária da API de manutenção → camada Bronze",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["iot", "api", "maintenance"],
) as dag:

    # Task única desta DAG: consulta a API de manutenção e salva os dados brutos
    # na camada Bronze do nosso Data Lakehouse.
    #
    # O task_id "fetch_maintenance_data" é diferente dos outros ("ingest_erp_data",
    # "bronze_to_silver"), refletindo a natureza da operação: estamos "buscando" (fetch)
    # dados de uma API externa, não fazendo uma transformação ou ingestão batch.
    # Bons nomes de task_id ajudam muito na hora de depurar falhas na interface do Airflow!
    ingest_api = PythonOperator(
        task_id="fetch_maintenance_data",
        python_callable=run_api_ingestion,
    )
