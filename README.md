# Plataforma IoT Industrial — Integração de Dados

Plataforma completa de integração de dados para monitoramento industrial. Ingere leituras de sensores em tempo real via Apache Kafka, processa dados via Apache Spark (batch e streaming), orquestra pipelines com Apache Airflow e disponibiliza métricas via Metabase.

## Arquitetura

```
Sensores (simulador Python)
        │
        ▼
   Apache Kafka ─────────────────────────────────────────┐
   tópico: iot-sensors                                   │ streaming
        │ consumer (kafka_to_bronze.py)                  ▼
        ▼                                     anomaly_detector.py
   MinIO / Bronze                             (janelas 5 min)
   factory_id / year / month / day                  │
        │                                           ▼
        │ Airflow @daily                    MinIO / alerts/
        ▼
   Spark: bronze_to_silver.py
        │
        ▼
   MinIO / Silver (year / month / factory_id)
        │
        ▼
   Spark: silver_to_gold.py
        │
        ▼
   MinIO / Gold — Parquet (year / month / factory_id)
        │
        ▼
   Metabase (dashboards)

PostgreSQL (ERP) ──── Airflow @daily ──► MinIO / Bronze / erp/
API Manutenção   ──── Airflow @hourly ──► MinIO / Bronze / maintenance/
MongoDB ──────── cadastro de equipamentos e ranges de sensores
```

## Stack

| Tecnologia | Versão | Papel |
|---|---|---|
| Apache Kafka | 7.5.0 (Confluent) | Streaming de eventos IoT |
| Apache Spark | 3.5 | Processamento batch e streaming |
| Apache Airflow | 2.9.3 | Orquestração de pipelines |
| MinIO | latest | Object storage (lakehouse Bronze/Silver/Gold) |
| MongoDB | 6 | Cadastro de equipamentos e sensores |
| PostgreSQL | 15 | ERP + metadados Airflow/Metabase |
| Metabase | 0.49 | Dashboards analíticos |

## Pré-requisitos

- Docker 24+ e Docker Compose 2.x
- Python 3.10+
- Dependências: `pip install kafka-python pymongo boto3 psycopg2-binary requests pytest`

## Subindo o Ambiente

```bash
docker-compose up -d
```

Aguardar todos os containers ficarem `healthy` (~3-5 minutos):
```bash
docker-compose ps
```

## Setup Inicial

### 1. Popular MongoDB
```bash
python src/ingestao/mongodb_setup.py
```

### 2. Criar buckets no MinIO
```bash
docker exec minio mc alias set local http://localhost:9000 admin password
docker exec minio mc mb local/bronze local/silver local/gold local/alerts local/checkpoints
```

## Executando o Pipeline

### Streaming: Kafka → Bronze
```bash
# Terminal 1: simulador de sensores
python src/ingestao/sensor_simulator.py

# Terminal 2: consumer Kafka → MinIO Bronze
python src/streaming/kafka_to_bronze.py
```

### Batch: Bronze → Silver → Gold
Via Airflow em http://localhost:8080 (admin/admin) — acione a DAG `iot_lakehouse_pipeline`.

Ou localmente:
```bash
MINIO_ENDPOINT=http://localhost:9000 python src/processamento/bronze_to_silver.py
MINIO_ENDPOINT=http://localhost:9000 python src/processamento/silver_to_gold.py
```

### Streaming de Anomalias
```bash
docker exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/bitnami/spark/jobs/anomaly_detector.py
```

## Testes de Qualidade

```bash
pytest tests/ -v
```

## Serviços e Portas

| Serviço | Porta | Acesso |
|---|---|---|
| Kafka | 9092 (ext) | localhost:9092 |
| MongoDB | 27017 | mongodb://localhost:27017 |
| PostgreSQL | 5432 | localhost:5432 (user/password, db: iot) |
| MinIO API | 9000 | http://localhost:9000 |
| MinIO Console | 9001 | http://localhost:9001 (admin/password) |
| Spark Master UI | 8081 | http://localhost:8081 |
| Airflow | 8080 | http://localhost:8080 (admin/admin) |
| Metabase | 3000 | http://localhost:3000 |

## Estrutura do Repositório

```
projeto-iot-dados/
├── README.md
├── docker-compose.yml
├── docker/
│   └── init-postgres.sql       # Cria databases airflow e metabase
├── docs/
│   ├── arquitetura.md          # ADRs e decisões técnicas
│   ├── modelo-dados.md         # Modelagem NoSQL MongoDB
│   ├── catalogo-dados.md       # Catálogo de schemas e linhagem
│   └── runbook.md              # Guia operacional
├── src/
│   ├── ingestao/
│   │   ├── sensor_simulator.py         # Gera eventos → Kafka
│   │   ├── mongodb_setup.py            # Setup MongoDB
│   │   ├── erp_ingestion.py            # Ingestão batch ERP
│   │   └── maintenance_api_ingestion.py # Ingestão API REST
│   ├── processamento/
│   │   ├── bronze_to_silver.py         # Spark: Bronze → Silver
│   │   ├── silver_to_gold.py           # Spark: Silver → Gold
│   │   ├── bronze_silver.ipynb         # Notebook exploratório
│   │   └── silver_to_gold.ipynb        # Notebook exploratório
│   ├── dags/
│   │   ├── dag_pipeline.py             # Airflow: pipeline diário
│   │   ├── dag_erp_ingestion.py        # Airflow: ERP @daily
│   │   └── dag_maintenance_api.py      # Airflow: API @hourly
│   └── streaming/
│       ├── kafka_to_bronze.py          # Consumer Kafka → Bronze
│       └── anomaly_detector.py         # Spark Streaming + alertas
├── data/
│   ├── sample/
│   │   └── events_bronze_sample.json   # Dados de exemplo para testes
│   └── schemas/
│       ├── sensor_event.json           # JSON Schema do evento de sensor
│       └── equipment.json              # JSON Schema do equipamento
└── tests/
    └── test_data_quality.py            # 10 assertions de qualidade
```

## Documentação

- [Arquitetura e ADRs](docs/arquitetura.md)
- [Modelagem de Dados](docs/modelo-dados.md)
- [Catálogo de Dados](docs/catalogo-dados.md)
- [Runbook Operacional](docs/runbook.md)
