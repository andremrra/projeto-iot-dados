# Catálogo de Dados — Plataforma IoT Industrial

## Visão de Linhagem

```
[Sensores] ──Kafka──► [Bronze: iot/sensors]
                              │
                              ▼ Spark (bronze_to_silver.py)
                       [Silver: iot/sensors]
                              │
                              ▼ Spark (silver_to_gold.py)
                       [Gold: equipment_metrics_hourly]

[PostgreSQL ERP] ──Airflow──► [Bronze: erp/]
[API Manutenção] ──Airflow──► [Bronze: maintenance/]
[MongoDB]        ──referência──► consumer Kafka (ranges de anomalia)
```

---

## Camada Bronze

### bronze/factory_id=*/year=*/month=*/day=*/*.json

| Campo | Tipo | Descrição | Owner |
|---|---|---|---|
| event_id | string (UUID) | Identificador único do evento | Simulador |
| sensor_id | string | ID do sensor (ex: SENS-00074) | Simulador |
| equipment_id | string | ID do equipamento (ex: EQ-0023) | Simulador |
| factory_id | string | ID da fábrica (FAB-SP-01, FAB-RJ-01, FAB-MG-01) | Simulador |
| measurement_type | string | Tipo de medição (temperature, humidity, pressure, vibration, current) | Simulador |
| value | double | Valor lido pelo sensor | Simulador |
| unit | string | Unidade de medida (celsius, percent, bar, mm/s, ampere) | Simulador |
| timestamp | string (ISO 8601) | Timestamp da leitura em UTC | Simulador |
| quality | string | Qualidade da leitura (good, warning, bad) | Simulador |
| is_anomaly | boolean | True se o valor está fora do range normal | Simulador |
| metadata.firmware_version | string | Versão do firmware do sensor | Simulador |
| metadata.battery_level | integer [0-100] | Nível de bateria do sensor (%) | Simulador |
| metadata.signal_strength | integer | Força do sinal em dBm (negativo) | Simulador |

**Formato:** JSONL (um objeto JSON por linha)
**Particionamento:** factory_id / year / month / day
**Retenção:** 2 anos
**Schema:** data/schemas/sensor_event.json

---

## Camada Silver

### silver/year=*/month=*/factory_id=*/*.json

Mesmos campos do Bronze, com as seguintes transformações aplicadas:

| Campo | Tipo | Transformação |
|---|---|---|
| timestamp | timestamp | Convertido de string ISO para tipo timestamp nativo |
| sensor_id | string | Trim de espaços |
| equipment_id | string | Trim de espaços |
| factory_id | string | Trim de espaços |
| measurement_type | string | Lowercase + trim |
| unit | string | Lowercase + trim |
| quality | string | Lowercase + trim |
| firmware_version | string | Extraído de metadata (flattened) |
| battery_level | integer | Extraído de metadata (flattened) |
| signal_strength | integer | Extraído de metadata (flattened) |
| year | integer | Extraído do timestamp |
| month | integer | Extraído do timestamp |
| day | integer | Extraído do timestamp |

**Garantias:** sem duplicatas (dedup por event_id); sem nulos em event_id/sensor_id/value/timestamp
**Formato:** JSONL particionado
**Particionamento:** year / month / factory_id

---

## Camada Gold

### gold/equipment_metrics_hourly/year=*/month=*/factory_id=*/*.parquet

| Campo | Tipo | Descrição |
|---|---|---|
| equipment_id | string | ID do equipamento |
| factory_id | string | ID da fábrica |
| measurement_type | string | Tipo de medição |
| timestamp_hour | timestamp | Hora do bucket de agregação |
| year | integer | Ano |
| month | integer | Mês |
| day | integer | Dia |
| hour | integer | Hora |
| avg_value | double | Média das leituras na hora |
| min_value | double | Mínimo das leituras na hora |
| max_value | double | Máximo das leituras na hora |
| std_value | double | Desvio padrão das leituras |
| event_count | long | Total de eventos na hora |
| anomaly_count | long | Total de eventos anômalos |
| anomaly_rate | double | Taxa de anomalia (anomaly_count / event_count) |
| criticality_score | double | Score = anomaly_rate×0.6 + (std/avg)×0.4 |

**Formato:** Parquet (comprimido, columnar)
**Granularidade:** 1 hora por equipamento por tipo de medição
**Particionamento:** year / month / factory_id

---

## Fontes Externas

### Bronze: erp/ (PostgreSQL ERP)

| Campo | Tipo | Descrição |
|---|---|---|
| source | string | Sempre "erp" |
| id | integer | ID do registro |
| equipment_id | string | ID do equipamento |
| factory_id | string | ID da fábrica |
| event_type | string | preventive_maintenance, corrective_maintenance, inspection, replacement |
| description | string | Descrição do evento |
| technician | string | Responsável técnico |
| cost | double | Custo em R$ |
| event_date | date | Data do evento |

**Frequência:** @daily via DAG erp_batch_ingestion

### Bronze: maintenance/ (API REST)

| Campo | Tipo | Descrição |
|---|---|---|
| id | string | ID da ordem de manutenção |
| equipment_id | string | ID do equipamento |
| factory_id | string | ID da fábrica |
| maintenance_type | string | preventive, corrective, inspection |
| status | string | scheduled, in_progress, completed, cancelled |
| scheduled_date | date | Data agendada |
| technician | string | Técnico responsável |
| estimated_duration_hours | double | Duração estimada em horas |

**Frequência:** @hourly via DAG maintenance_api_ingestion

---

## Alertas de Streaming

### alerts/streaming/factory_id=*/*.json

| Campo | Tipo | Descrição |
|---|---|---|
| window.start | timestamp | Início da janela de 5 min |
| window.end | timestamp | Fim da janela de 5 min |
| factory_id | string | ID da fábrica |
| equipment_id | string | ID do equipamento |
| measurement_type | string | Tipo de medição |
| event_count | long | Total de eventos na janela |
| anomaly_count | long | Eventos anômalos na janela |
| anomaly_rate | double | Taxa de anomalia |
| avg_value | double | Média do valor na janela |
| alert_level | string | warning (>=10%) ou critical (>=30%) |

**Latência:** ~1-2 minutos (trigger do Spark Streaming)
