# Runbook Operacional — Plataforma IoT Industrial

## Pré-requisitos

- Docker 24+ e Docker Compose 2.x instalados
- Python 3.10+ (para executar scripts fora do Docker)
- Dependências Python: `pip install kafka-python pymongo boto3 psycopg2-binary requests pytest`

---

## 1. Subir o Ambiente

```bash
docker-compose up -d
```

Aguardar todos os serviços ficarem `healthy`:
```bash
docker-compose ps
```

Ordem de inicialização: Zookeeper → Kafka → MongoDB → PostgreSQL → MinIO → Spark → Airflow → Metabase

**Tempo estimado de inicialização completa:** ~3-5 minutos (Airflow instala dependências no primeiro boot).

---

## 2. Setup Inicial (primeira execução)

### 2.1 Popular MongoDB (cadastro de equipamentos)
```bash
python src/ingestao/mongodb_setup.py
```

### 2.2 Verificar Airflow
Acesse http://localhost:8080 (admin / admin) e confirme que as 3 DAGs aparecem:
- `iot_lakehouse_pipeline`
- `erp_batch_ingestion`
- `maintenance_api_ingestion`

### 2.3 Criar buckets no MinIO
Acesse http://localhost:9001 (admin / password) e crie os buckets manualmente:
- `bronze`
- `silver`
- `gold`
- `alerts`
- `checkpoints`

Ou via CLI:
```bash
docker exec minio mc alias set local http://localhost:9000 admin password
docker exec minio mc mb local/bronze local/silver local/gold local/alerts local/checkpoints
```

---

## 3. Gerar Dados de Sensores

Em um terminal separado:
```bash
# Gera 100 eventos/segundo por 60 segundos
python src/ingestao/sensor_simulator.py --duration 60

# Para execução contínua
python src/ingestao/sensor_simulator.py

# Para testar anomalias (30% de taxa)
python src/ingestao/sensor_simulator.py --anomaly-rate 0.30 --duration 30
```

---

## 4. Iniciar Consumer Kafka → Bronze

Em outro terminal:
```bash
python src/streaming/kafka_to_bronze.py
```

Confirmar que arquivos aparecem no MinIO: http://localhost:9001 → bucket `bronze`.

---

## 5. Executar Pipeline Batch

### Via Airflow (recomendado)
1. Acesse http://localhost:8080
2. Ative a DAG `iot_lakehouse_pipeline`
3. Clique em "Trigger DAG" para executar imediatamente
4. Acompanhe a execução no Graph View

### Via linha de comando (para teste local)
```bash
# Requer pyspark instalado e MinIO rodando
MINIO_ENDPOINT=http://localhost:9000 python src/processamento/bronze_to_silver.py
MINIO_ENDPOINT=http://localhost:9000 python src/processamento/silver_to_gold.py
```

---

## 6. Executar Pipeline Streaming (Detecção de Anomalias)

```bash
# Submeter job ao cluster Spark
docker exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/bitnami/spark/jobs/anomaly_detector.py
```

Ou via `spark-submit` local (com Kafka e MinIO acessíveis):
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 MINIO_ENDPOINT=http://localhost:9000 \
  spark-submit --packages ... src/streaming/anomaly_detector.py
```

---

## 7. Executar Testes de Qualidade

```bash
pip install pytest
pytest tests/ -v
```

Saída esperada: 10 testes passando com os dados de `data/sample/events_bronze_sample.json`.

---

## 8. Monitoramento

| Interface | URL | Credenciais |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Spark UI | http://localhost:8081 | — |
| MinIO Console | http://localhost:9001 | admin / password |
| Metabase | http://localhost:3000 | config na 1ª execução |

### Verificar logs de um serviço
```bash
docker-compose logs -f kafka
docker-compose logs -f airflow-scheduler
docker-compose logs -f spark-master
```

---

## 9. Troubleshooting

### Kafka não aceita conexão interna (de containers)
Use `kafka:29092` em vez de `localhost:9092` para conexões entre containers Docker.

### Spark não conecta ao MinIO
Verifique se os pacotes Hadoop-AWS foram baixados. Na primeira execução pode demorar.
O endpoint dentro do Docker é `http://minio:9000` (não `localhost`).

### Airflow não enxerga as DAGs
Confirme que o volume `./src/dags:/opt/airflow/dags` está montado:
```bash
docker exec airflow-webserver ls /opt/airflow/dags
```

### airflow-init falha ao criar a conexão Spark
Execute manualmente após o init:
```bash
docker exec airflow-webserver airflow connections add spark_default \
  --conn-type spark --conn-host spark://spark-master --conn-port 7077
```

### MinIO bucket não existe
Crie via console (http://localhost:9001) ou com o comando mc listado na seção 2.3.

---

## 10. Parar o Ambiente

```bash
# Para mas mantém os volumes (dados preservados)
docker-compose down

# Para e remove todos os dados
docker-compose down -v
```
