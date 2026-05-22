# Plataforma IoT Industrial — Integração de Dados

Plataforma de integração de dados para monitoramento industrial. Ingere leituras de sensores em tempo real via Apache Kafka, armazena dados brutos no MinIO (camada Bronze do lakehouse), e mantém o cadastro de equipamentos e fábricas no MongoDB.

## Arquitetura

```
Sensores (simulador)
        │
        ▼
   Apache Kafka  ──────────────────────────────────────────┐
        │                                                   │
        ▼                                                   ▼
  kafka_to_bronze.py                               MongoDB (equipamentos)
  (consumer Kafka)                                  cadastro + ranges dos sensores
        │
        ▼
   MinIO / Bronze
   data/bronze/year=.../month=.../day=.../
```

## Pré-requisitos

- Docker e Docker Compose instalados
- Python 3.10+ (para rodar o simulador e o setup do MongoDB localmente)
- Dependências Python: `pip install kafka-python pymongo`

## Subindo o ambiente

```bash
docker-compose up -d
```

Isso sobe em ordem: Zookeeper → Kafka → MongoDB → PostgreSQL → MinIO.

Todos os serviços têm health checks configurados. Aguarde todos aparecerem como `healthy` antes de prosseguir:

```bash
docker-compose ps
```

## Populando o MongoDB

Após os containers estarem saudáveis, rode o script de setup:

```bash
python src/ingestao/mongodb_setup.py
```

Isso cria as coleções `fabricas` e `equipamentos` com validação de schema, índices e seed de 50 equipamentos distribuídos entre as 3 fábricas.

Opções disponíveis:

```bash
# Conectar em URI diferente
python src/ingestao/mongodb_setup.py --uri mongodb://localhost:27017

# Mudar quantidade de equipamentos no seed
python src/ingestao/mongodb_setup.py --equipments 100

# Resetar tudo (apaga e recria)
python src/ingestao/mongodb_setup.py --drop
```

## Gerando eventos de sensores

Em um terminal separado, rode o simulador:

```bash
python src/ingestao/sensor_simulator.py
```

Por padrão gera 100 eventos/segundo para o tópico `iot-sensors` no Kafka local.

Opções:

```bash
# Ver eventos sem enviar para o Kafka
python src/ingestao/sensor_simulator.py --dry-run

# Configurar taxa e duração
python src/ingestao/sensor_simulator.py --events-per-second 200 --duration 60

# Aumentar taxa de anomalias (padrão: 5%)
python src/ingestao/sensor_simulator.py --anomaly-rate 0.15
```

## Consumindo eventos e gravando no Bronze

Em outro terminal:

```bash
python src/streaming/kafka_to_bronze.py
```

Os eventos são gravados particionados por data em `data/bronze/year=.../month=.../day=.../events.json`.

## Serviços e portas

| Serviço | Porta | Acesso |
|---|---|---|
| Kafka | 9092 | `localhost:9092` |
| MongoDB | 27017 | `mongodb://localhost:27017` |
| PostgreSQL | 5432 | `localhost:5432` (user/password, db: iot) |
| MinIO Console | 9001 | http://localhost:9001 (admin/password) |
| MinIO API | 9000 | `http://localhost:9000` |

## Estrutura do repositório

```
projeto-iot-dados/
├── README.md
├── docker-compose.yml
├── docs/
│   └── modelo-dados.md        # Modelagem NoSQL + ADR de decisões arquiteturais
├── src/
│   ├── ingestao/
│   │   ├── sensor_simulator.py  # Gera eventos de sensores → Kafka
│   │   └── mongodb_setup.py     # Cria coleções, índices e seed no MongoDB
│   └── streaming/
│       └── kafka_to_bronze.py   # Consome Kafka e grava na camada Bronze
└── data/
    └── bronze/                  # Dados brutos particionados por data
```

## Parando o ambiente

```bash
docker-compose down
```

Para remover também os volumes (apaga todos os dados):

```bash
docker-compose down -v
```
