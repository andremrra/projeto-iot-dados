# Arquitetura da Plataforma IoT Industrial

## Visão Geral

Plataforma de integração de dados para monitoramento industrial, implementada com arquitetura **Medallion Lakehouse** (Bronze / Silver / Gold) sobre MinIO, processamento batch via Apache Spark orquestrado pelo Airflow, e processamento streaming via Spark Structured Streaming consumindo Apache Kafka.

```
Sensores (simulador Python)
        │
        ▼
  Apache Kafka ──────────────────────────────────┐
  tópico: iot-sensors                            │ streaming
        │                                        ▼
        │ consumer               anomaly_detector.py
        ▼                        (janelas 5 min, alertas)
  MinIO / Bronze                         │
  factory_id / year / month / day        ▼
        │                         MinIO / alerts/
        │ Airflow @daily
        ▼
  Spark: bronze_to_silver.py
        │
        ▼
  MinIO / Silver
  year / month / factory_id
        │
        ▼
  Spark: silver_to_gold.py
        │
        ▼
  MinIO / Gold (Parquet)
  year / month / factory_id
        │
        ▼
  Metabase (dashboards)

PostgreSQL (ERP) ──── Airflow @daily ──► MinIO / Bronze / erp/
API Manutenção   ──── Airflow @hourly ──► MinIO / Bronze / maintenance/
MongoDB ──────────────────────────────── referência (equipamentos)
```

---

## ADR-001: Arquitetura Medallion Lakehouse

**Status:** Aceito

### Contexto
Precisamos armazenar dados brutos de sensores para auditoria (2 anos), ao mesmo tempo em que disponibilizamos dados limpos e agregados para dashboards em near real-time.

### Decisão
Arquitetura em três camadas no MinIO (S3-compatible):
- **Bronze**: dados brutos, imutáveis, append-only, particionados por factory/data
- **Silver**: dados limpos, tipados, deduplicados, em JSON particionado
- **Gold**: métricas horárias agregadas em Parquet, otimizado para leitura analítica

### Alternativas Consideradas
1. **Banco de dados único (PostgreSQL)**: simples mas não escala para volumes IoT e não suporta schema flexível
2. **Data Warehouse (Redshift/BigQuery)**: custo elevado, over-engineering para o escopo do projeto

### Consequências
- **Positivas**: separação clara de responsabilidades; Bronze preserva dados brutos para auditoria; Gold em Parquet é 10x mais rápido que JSON para queries analíticas
- **Negativas**: requer orquestração (Airflow) para manter as camadas sincronizadas

---

## ADR-002: Particionamento por factory_id + data

**Status:** Aceito

### Contexto
Os jobs Spark que transformam Bronze→Silver e Silver→Gold sempre filtram por fábrica. O particionamento impacta diretamente o tempo de leitura.

### Decisão
Particionamento Bronze: `factory_id / year / month / day`
Particionamento Silver: `year / month / factory_id`
Particionamento Gold: `year / month / factory_id`

### Consequências
- **Positivas**: Spark aplica partition pruning ao filtrar por fábrica — evita ler 100% dos dados. Com 3 fábricas, reduz I/O em ~67% por query de fábrica única
- **Negativas**: factory_id como primeira dimensão em Bronze fragmenta mais os arquivos por dia (aceitável com BATCH_SIZE=1000)

---

## ADR-003: Apache Kafka para Streaming

**Status:** Aceito

### Contexto
100+ eventos/segundo de sensores distribuídos por 3 fábricas precisam ser ingeridos de forma resiliente e ordenada por equipamento.

### Decisão
Kafka com tópico `iot-sensors` e particionamento por `equipment_id` (feito pelo simulador). Consumer Python (kafka-python) faz batch de 1.000 eventos antes de gravar no MinIO.

### Alternativas Consideradas
1. **REST API (FastAPI)**: sem durabilidade, sem replay, perde eventos se o consumer cair
2. **RabbitMQ**: sem replay nativo, modelo push dificulta batching

### Consequências
- **Positivas**: replay de eventos; particionamento por equipment_id garante ordem; desacoplamento produtor/consumidor
- **Negativas**: overhead operacional do ZooKeeper; single-broker sem replicação (adequado para dev/demo)

---

## ADR-004: Apache Airflow para Orquestração

**Status:** Aceito

### Contexto
Pipeline diário Bronze→Silver→Gold e ingestões periódicas (ERP diário, API horária) precisam ser agendados, monitorados e re-executáveis em caso de falha.

### Decisão
Airflow 2.9 com LocalExecutor (sem Redis/Celery, suficiente para o volume do projeto). DAGs:
- `iot_lakehouse_pipeline`: @daily, Bronze→Silver→Gold via SparkSubmitOperator
- `erp_batch_ingestion`: @daily, PostgreSQL→Bronze
- `maintenance_api_ingestion`: @hourly, API REST→Bronze

### Consequências
- **Positivas**: retry automático; interface web para monitoramento; catchup para reprocessar dias anteriores
- **Negativas**: LocalExecutor não paraleliza entre DAGs; em produção usaríamos CeleryExecutor

---

## ADR-005: Spark Structured Streaming para Detecção de Anomalias

**Status:** Aceito

### Contexto
Requisito: detectar anomalias em near real-time (< 1 min). Precisamos de janelas temporais para calcular taxa de anomalia por período.

### Decisão
Spark Structured Streaming consumindo Kafka, com:
- Janelas tumbling de 5 minutos sobre `event_timestamp`
- Watermark de 10 minutos para late data handling
- Threshold: `anomaly_rate >= 0.1` → warning; `>= 0.3` → critical
- Saída: MinIO `s3a://alerts/streaming/` + console para monitoramento

### Alternativas Consideradas
1. **Apache Flink**: mais eficiente para streaming puro, mas stack diferente do batch (Spark)
2. **KSQL/Kafka Streams**: limitado a operações SQL, sem integração com MinIO lakehouse

### Consequências
- **Positivas**: mesmo engine Spark para batch e streaming (unified API); watermarks evitam resultados incorretos por late data
- **Negativas**: micro-batch (não true-streaming); latência mínima de 1 minuto pelo trigger

---

## Stack de Serviços

| Serviço | Porta | Propósito |
|---|---|---|
| Kafka | 9092 (ext) / 29092 (int) | Streaming de eventos |
| MongoDB | 27017 | Cadastro de equipamentos |
| PostgreSQL | 5432 | ERP + metadados Airflow + Metabase |
| MinIO | 9000 (API) / 9001 (Console) | Object storage (lakehouse) |
| Spark Master | 7077 / 8081 (UI) | Processamento distribuído |
| Airflow | 8080 | Orquestração de pipelines |
| Metabase | 3000 | Dashboards analíticos |
