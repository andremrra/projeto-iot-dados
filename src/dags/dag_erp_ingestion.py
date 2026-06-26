# Boa prática de compatibilidade entre versões do Python.
# Com isso, as anotações de tipo são tratadas como strings até serem necessárias,
# evitando erros em versões mais antigas do Python 3.
from __future__ import annotations

# sys é um módulo da biblioteca padrão do Python que nos dá acesso a configurações
# do interpretador, como a lista de caminhos onde ele busca módulos (sys.path).
import sys

# Módulos para trabalhar com datas — essenciais para pipelines de dados,
# onde é comum processar um intervalo de tempo específico por execução.
from datetime import datetime, timedelta

# Importamos o objeto DAG (Directed Acyclic Graph) do Airflow.
# Uma DAG é basicamente um fluxo de trabalho: define quais tarefas rodar,
# em qual ordem e com qual frequência.
from airflow import DAG

# PythonOperator é o operador que nos permite executar uma função Python
# diretamente como uma task do Airflow. É diferente do BashOperator,
# que executa comandos de terminal.
from airflow.operators.python import PythonOperator

# Aqui adicionamos o caminho "/opt/airflow/src" ao sys.path.
# Isso é necessário porque nossos módulos customizados (como "ingestao")
# ficam nesse diretório dentro do container Docker. Sem essa linha,
# o Python não saberia onde procurar esses módulos e daria ImportError.
sys.path.insert(0, "/opt/airflow/src")

# Configurações padrão herdadas por todas as tasks desta DAG.
# Diferente da dag_pipeline.py, aqui temos 3 retries (em vez de 2) e
# retry_delay de 10 minutos (em vez de 5), pois a ingestão do ERP pode
# depender de janelas de disponibilidade do banco de dados legado.
default_args = {
    "owner": "iot-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}


# Essa função é o "coração" da nossa task de ingestão.
# O Airflow passa automaticamente um dicionário "context" para funções
# usadas com PythonOperator quando declaramos **context como parâmetro.
# Dentro desse context, "ds" é a data de execução no formato "YYYY-MM-DD"
# (ex: "2025-03-15"). Isso é muito útil para fazer ingestão incremental —
# só busca os dados do dia que está sendo processado.
def run_erp_ingestion(**context):
    # Importamos o módulo de ingestão AQUI DENTRO da função, não no topo do arquivo.
    # Isso é uma técnica chamada "lazy import" (importação preguiçosa).
    # Por que fazer isso? Porque quando o Airflow carrega (parseia) as DAGs para
    # exibir na interface, ele importa todos os arquivos. Se colocássemos o import
    # no topo, o Airflow tentaria importar "ingestao.erp_ingestion" mesmo quando
    # só está listando as DAGs — e isso poderia falhar se o módulo não estiver
    # disponível naquele momento.
    from ingestao.erp_ingestion import run

    # Chamamos a função "run" passando a data de execução.
    # context["ds"] é a data da execução atual da DAG (no formato "YYYY-MM-DD").
    # Isso permite que nosso script de ingestão saiba exatamente qual dia processar,
    # tornando o pipeline IDEMPOTENTE (pode ser reexecutado sem duplicar dados).
    run(execution_date=context["ds"])


# Criamos a DAG de ingestão do ERP com contexto "with" para associar
# automaticamente todas as tasks criadas dentro deste bloco a esta DAG.
#
# Diferenças importantes em relação à dag_pipeline.py:
# - dag_id diferente: "erp_batch_ingestion" — cada DAG precisa de um ID único
# - description: deixa claro que isso é uma ingestão ERP → Bronze (dado bruto)
# - schedule="@daily": roda uma vez por dia, pois o ERP é um sistema transacional
#   que não muda em tempo real (diferente de sensores IoT)
with DAG(
    dag_id="erp_batch_ingestion",
    default_args=default_args,
    description="Ingestão diária do ERP (PostgreSQL) → camada Bronze",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["iot", "erp", "batch"],
) as dag:

    # Criamos a task usando PythonOperator.
    # - task_id: nome único da task dentro desta DAG (aparece na interface do Airflow)
    # - python_callable: a função Python que será executada quando essa task rodar
    #
    # Essa é a única task desta DAG — ela faz tudo sozinha: conecta no PostgreSQL
    # do ERP, extrai os dados do dia e salva na camada Bronze do Data Lakehouse.
    # DAGs simples com uma única task são perfeitamente normais quando o trabalho
    # está encapsulado em um único módulo bem organizado.
    ingest_erp = PythonOperator(
        task_id="ingest_erp_data",
        python_callable=run_erp_ingestion,
    )
