#!/usr/bin/env python3
"""
Setup e Seed do MongoDB
=======================
Cria coleções com validação de schema, índices e popula dados iniciais
de equipamentos e fábricas, consistente com os IDs usados pelo sensor_simulator.py.

Uso:
    python mongodb_setup.py
    python mongodb_setup.py --uri mongodb://localhost:27017 --equipments 50
    python mongodb_setup.py --drop   # apaga e recria tudo

Requisitos:
    pip install pymongo
"""

# --- IMPORTS PADRÃO DA LINGUAGEM ---
# argparse: permite definir parâmetros de linha de comando (ex: --uri, --drop)
# random: usado para sortear tipos de equipamentos, sensores e datas de forma reproduzível
# date e timedelta: para calcular datas de instalação e próxima manutenção dos equipamentos
import argparse
import random
from datetime import date, timedelta

# --- IMPORT DO PYMONGO COM TRATAMENTO DE ERRO ---
# pymongo é a biblioteca oficial para conectar Python ao MongoDB.
# MongoClient: cria a conexão com o servidor MongoDB
# ASCENDING: constante que define a ordem crescente em índices (equivale a 1 no MongoDB)
# IndexModel: representação de um índice — usado para criar vários de uma vez
# CollectionInvalid: exceção lançada quando tentamos criar uma coleção que já existe
try:
    from pymongo import MongoClient, ASCENDING, IndexModel
    from pymongo.errors import CollectionInvalid
except ImportError:
    print("Erro: pymongo não instalado. Execute: pip install pymongo")
    exit(1)


# =============================================================================
# DADOS DE REFERÊNCIA
# =============================================================================

# Aqui definimos as fábricas que serão inseridas no MongoDB.
# O campo "_id" é o identificador padrão do MongoDB — ao usá-lo explicitamente,
# controlamos o ID (em vez de deixar o MongoDB gerar um ObjectId automático).
# A localização é um subdocumento (objeto aninhado) com lat/lng para buscas geoespaciais.
FACTORIES = [
    {
        "_id": "FAB-SP-01",
        "name": "Fábrica São Paulo",
        "location": {"lat": -23.5505, "lng": -46.6333},
        "address": "Av. Industrial, 1000, São Paulo, SP",
        "active": True,
    },
    {
        "_id": "FAB-RJ-01",
        "name": "Fábrica Rio de Janeiro",
        "location": {"lat": -22.9068, "lng": -43.1729},
        "address": "Rua das Máquinas, 500, Rio de Janeiro, RJ",
        "active": True,
    },
    {
        "_id": "FAB-MG-01",
        "name": "Fábrica Belo Horizonte",
        "location": {"lat": -19.9167, "lng": -43.9345},
        "address": "Rod. Industrial, 200, Belo Horizonte, MG",
        "active": True,
    },
]

# Lista dos tipos de equipamentos — precisa ser idêntica ao sensor_simulator.py
# para que os IDs gerados nos dois arquivos sejam consistentes entre si.
EQUIPMENT_TYPES = ["compressor", "pump", "motor", "conveyor", "turbine"]

# Configurações detalhadas de cada tipo de sensor com suas faixas de operação.
# Usamos dicionários aninhados (range e normal_range como sub-objetos)
# porque no MongoDB isso ficará como documento embutido, facilitando queries como:
# db.equipamentos.find({"sensors.normal_range.max": {$lt: 70}})
SENSOR_CONFIGS = {
    "temperature": {
        "unit": "celsius",
        "range": {"min": 0, "max": 150},
        "normal_range": {"min": 30, "max": 70},
    },
    "humidity": {
        "unit": "percent",
        "range": {"min": 0, "max": 100},
        "normal_range": {"min": 40, "max": 60},
    },
    "pressure": {
        "unit": "bar",
        "range": {"min": 0, "max": 20},
        "normal_range": {"min": 5, "max": 15},
    },
    "vibration": {
        "unit": "mm/s",
        "range": {"min": 0, "max": 50},
        "normal_range": {"min": 0, "max": 10},
    },
    "current": {
        "unit": "ampere",
        "range": {"min": 0, "max": 100},
        "normal_range": {"min": 10, "max": 50},
    },
}

# Frequências possíveis de manutenção preventiva dos equipamentos.
# SCHEDULE_DAYS mapeia cada frequência para quantos dias até a próxima manutenção —
# isso permite calcular next_maintenance somando a última manutenção com esse valor.
MAINTENANCE_SCHEDULES = ["weekly", "monthly", "quarterly", "semiannual"]
SCHEDULE_DAYS = {"weekly": 7, "monthly": 30, "quarterly": 90, "semiannual": 180}


# =============================================================================
# JSON SCHEMA VALIDATORS
# =============================================================================

# O MongoDB permite validar documentos com JSON Schema antes de inserir/atualizar.
# Isso é ótimo para garantir qualidade dos dados — o banco rejeita documentos malformados.
#
# FABRICAS_VALIDATOR define as regras para a coleção "fabricas":
# - bsonType: tipo BSON esperado (string, object, double, bool — tipos nativos do MongoDB)
# - required: campos obrigatórios — o documento é rejeitado se algum estiver faltando
# - properties: validação individual de cada campo
# O campo "location" é um subdocumento com suas próprias validações (lat e lng como double).
FABRICAS_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["_id", "name", "location", "active"],
        "properties": {
            "_id":  {"bsonType": "string"},
            "name": {"bsonType": "string"},
            "location": {
                "bsonType": "object",
                "required": ["lat", "lng"],
                "properties": {
                    "lat": {"bsonType": "double"},
                    "lng": {"bsonType": "double"},
                },
            },
            "address": {"bsonType": "string"},
            "active":  {"bsonType": "bool"},
        },
    }
}

# Validator mais complexo para equipamentos, porque cada equipamento tem um array de sensores.
# O campo "sensors" é um array de objetos, e validamos cada item do array também.
# minItems: 1 garante que todo equipamento tem ao menos um sensor.
# "enum" em "type" e "status" restringe os valores aceitos a uma lista fixa —
# é como uma constraint CHECK no SQL, impedindo valores inválidos como "motor2" ou "broken".
EQUIPAMENTOS_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["_id", "name", "type", "factory_id", "factory", "sensors", "status", "installed_at"],
        "properties": {
            "_id":        {"bsonType": "string"},
            "name":       {"bsonType": "string"},
            "type":       {"bsonType": "string", "enum": EQUIPMENT_TYPES},
            "factory_id": {"bsonType": "string"},
            # "factory" é um subdocumento desnormalizado — embutimos os dados da fábrica
            # no próprio documento do equipamento para evitar lookups (joins) frequentes.
            "factory": {
                "bsonType": "object",
                "required": ["id", "name", "location"],
                "properties": {
                    "id":       {"bsonType": "string"},
                    "name":     {"bsonType": "string"},
                    "location": {"bsonType": "object"},
                },
            },
            # Validação do array de sensores — cada item precisa ter id, type, unit e range
            "sensors": {
                "bsonType": "array",
                "minItems": 1,
                "maxItems": 50,
                "items": {
                    "bsonType": "object",
                    "required": ["id", "type", "unit", "range"],
                    "properties": {
                        "id":           {"bsonType": "string"},
                        "type":         {"bsonType": "string"},
                        "unit":         {"bsonType": "string"},
                        "range":        {"bsonType": "object"},
                        "normal_range": {"bsonType": "object"},
                        "active":       {"bsonType": "bool"},
                    },
                },
            },
            "maintenance":  {"bsonType": "object"},
            "installed_at": {"bsonType": "string"},
            # enum restringe status a apenas esses três valores válidos
            "status": {
                "bsonType": "string",
                "enum": ["active", "inactive", "maintenance"],
            },
        },
    }
}


# =============================================================================
# COLEÇÕES E ÍNDICES
# =============================================================================

def create_collection_safe(db, name: str, validator: dict):
    """Cria coleção com schema validator. Se já existir, atualiza o validator."""
    # Tentamos criar a coleção com o validator e nível de validação "moderate".
    # "moderate" = valida na inserção e atualização, mas não em documentos já existentes.
    # Se a coleção já existe, CollectionInvalid é lançado — capturamos e apenas atualizamos o validator.
    # Isso torna a função idempotente: pode ser chamada várias vezes sem problema.
    try:
        db.create_collection(name, validator=validator, validationLevel="moderate")
        print(f"  Coleção '{name}' criada.")
    except CollectionInvalid:
        db.command("collMod", name, validator=validator, validationLevel="moderate")
        print(f"  Coleção '{name}' já existe — validator atualizado.")


def setup_collections(db):
    # Passo 1 do setup: criamos as coleções com seus validators de schema.
    # A ordem importa: "fabricas" antes de "equipamentos" porque equipamentos referenciam fábricas.
    print("\n[1/3] Criando coleções e validators de schema...")
    create_collection_safe(db, "fabricas", FABRICAS_VALIDATOR)
    create_collection_safe(db, "equipamentos", EQUIPAMENTOS_VALIDATOR)


def setup_indexes(db):
    # Passo 2 do setup: criamos índices para acelerar as queries mais comuns.
    # Índices no MongoDB funcionam como no SQL — evitam varredura completa da coleção (COLLSCAN)
    # e permitem buscas eficientes (IXSCAN). Sem índices, cada query precisa ler TODOS os documentos.
    print("\n[2/3] Criando índices...")

    # Índice simples na coleção de fábricas: filtragem por fábricas ativas
    db.fabricas.create_indexes([
        IndexModel([("active", ASCENDING)], name="idx_active"),
    ])
    print("  fabricas  → idx_active")

    db.equipamentos.create_indexes([
        # Índices simples para buscas por campo único — query como:
        # db.equipamentos.find({"factory_id": "FAB-SP-01"})
        IndexModel([("factory_id", ASCENDING)],   name="idx_factory_id"),
        IndexModel([("type",        ASCENDING)],   name="idx_type"),
        IndexModel([("status",      ASCENDING)],   name="idx_status"),

        # Índices em campos de array: o MongoDB indexa CADA ELEMENTO do array "sensors".
        # Isso permite buscar qual equipamento tem um sensor específico sem varrer tudo:
        # db.equipamentos.find({"sensors.id": "SENS-00042"})
        IndexModel([("sensors.id",   ASCENDING)],  name="idx_sensor_id"),
        IndexModel([("sensors.type", ASCENDING)],  name="idx_sensor_type"),

        # Índice composto: combina dois campos para otimizar a query mais frequente do sistema.
        # "Quais equipamentos ativos tem na fábrica SP?" usa factory_id E status juntos.
        # Um índice composto é MUITO mais eficiente que dois índices simples para essa query.
        IndexModel(
            [("factory_id", ASCENDING), ("status", ASCENDING)],
            name="idx_factory_status",
        ),
    ])
    print("  equipamentos → idx_factory_id, idx_type, idx_status,")
    print("                  idx_sensor_id, idx_sensor_type, idx_factory_status")


# =============================================================================
# SEED DE DADOS
# =============================================================================

def build_equipment(eq_index: int, sensor_start: int, factory: dict) -> tuple:
    """
    Constrói documento de equipamento com seed determinístico.
    Retorna (documento, próximo_sensor_counter).
    O seed fixo por eq_index garante que os IDs gerados aqui
    são idênticos aos que o sensor_simulator.py produziria na mesma ordem.
    """
    # PONTO IMPORTANTE: usamos random.Random(eq_index) em vez do random global.
    # Isso cria um gerador de números aleatórios ISOLADO com semente fixa.
    # Resultado: para o equipamento 1 sempre serão gerados os mesmos valores,
    # para o equipamento 2 também, etc. — independente de quantas vezes rodarmos.
    # Isso garante que o MongoDB e o sensor_simulator.py sempre terão os mesmos IDs!
    rng = random.Random(eq_index)

    # Determinamos o tipo do equipamento e quais sensores ele terá.
    # rng.sample garante que não repetimos tipos de sensor no mesmo equipamento.
    eq_type    = rng.choice(EQUIPMENT_TYPES)
    num_sensors = rng.randint(2, 5)
    sensor_types = rng.sample(list(SENSOR_CONFIGS.keys()), num_sensors)

    # Geramos datas realistas de instalação e manutenção.
    # timedelta(days=N) soma N dias a uma data base — forma pythônica de trabalhar com datas.
    # Isso dá histórico variado: alguns equipamentos instalados em 2022, outros em 2023.
    installed  = date(2022, 1, 1) + timedelta(days=rng.randint(0, 700))
    schedule   = rng.choice(MAINTENANCE_SCHEDULES)
    last_maint = installed + timedelta(days=rng.randint(30, 300))
    next_maint = last_maint + timedelta(days=SCHEDULE_DAYS[schedule])

    # Construímos a lista de sensores deste equipamento.
    # O counter compartilhado entre equipamentos garante IDs únicos globais (SENS-00001, SENS-00002...)
    # sem repetição entre equipamentos diferentes — igual ao sensor_simulator.py.
    sensors = []
    counter = sensor_start
    for s_type in sensor_types:
        cfg = SENSOR_CONFIGS[s_type]
        sensors.append({
            "id":           f"SENS-{counter:05d}",
            "type":         s_type,
            "unit":         cfg["unit"],
            "range":        cfg["range"],
            "normal_range": cfg["normal_range"],
            "active":       True,
        })
        counter += 1

    # Montamos o documento final do equipamento.
    # Note a DESNORMALIZAÇÃO PARCIAL: embutimos nome e localização da fábrica diretamente
    # no documento do equipamento (campo "factory"). No MongoDB, isso é uma prática comum
    # para evitar operações de $lookup (equivalente ao JOIN do SQL) em queries frequentes.
    # A contrapartida é que se o nome da fábrica mudar, precisamos atualizar todos os equipamentos.
    doc = {
        "_id":        f"EQ-{eq_index:04d}",
        "name":       f"{eq_type.title()} {eq_index}",
        "type":       eq_type,
        "factory_id": factory["_id"],
        # Desnormalização parcial: nome e coordenadas embutidos para leitura sem join
        "factory": {
            "id":       factory["_id"],
            "name":     factory["name"],
            "location": factory["location"],
        },
        "sensors": sensors,
        "maintenance": {
            "schedule":         schedule,
            "last_maintenance": str(last_maint),
            "next_maintenance": str(next_maint),
        },
        "installed_at": str(installed),
        "status":       "active",
    }
    return doc, counter


def seed_data(db, num_equipments: int):
    # Passo 3 do setup: populamos o banco com dados iniciais (seed).
    # "Seed" é o termo para dados de exemplo/inicialização — comum em projetos de dados.
    print(f"\n[3/3] Populando seed ({num_equipments} equipamentos)...")

    # Inserimos as fábricas usando replace_one com upsert=True.
    # upsert = "update or insert": se o documento com esse _id já existir, atualiza;
    # se não existir, insere. Isso torna o seed idempotente — pode rodar várias vezes
    # sem duplicar dados ou lançar erros de chave duplicada.
    for factory in FACTORIES:
        db.fabricas.replace_one({"_id": factory["_id"]}, factory, upsert=True)
    print(f"  {len(FACTORIES)} fábricas inseridas/atualizadas.")

    # Distribuímos os equipamentos entre as fábricas usando round-robin.
    # O operador % (módulo) faz a distribuição cíclica: equipamento 1 vai para fábrica 0,
    # equipamento 2 para fábrica 1, equipamento 3 para fábrica 2, equipamento 4 para fábrica 0...
    # Isso garante distribuição uniforme independente do número de equipamentos.
    sensor_counter = 1
    for i in range(1, num_equipments + 1):
        factory = FACTORIES[(i - 1) % len(FACTORIES)]
        doc, sensor_counter = build_equipment(i, sensor_counter, factory)
        db.equipamentos.replace_one({"_id": doc["_id"]}, doc, upsert=True)

    # Usamos o framework de agregação do MongoDB para contar o total de sensores.
    # $project cria um campo "n" com o tamanho do array "sensors" de cada documento.
    # $group com $sum acumula esse valor em todos os documentos — resultado: total global.
    # É como fazer SELECT SUM(array_length(sensors)) em SQL.
    total_sensors = db.equipamentos.aggregate([
        {"$project": {"n": {"$size": "$sensors"}}},
        {"$group":   {"_id": None, "total": {"$sum": "$n"}}},
    ]).next()["total"]

    print(f"  {num_equipments} equipamentos inseridos/atualizados.")
    print(f"  {total_sensors} sensores no total.")


# =============================================================================
# MAIN
# =============================================================================

def main():
    # Interface de linha de comando para o script de setup.
    # Isso permite rodar o script com diferentes configurações sem editar o código:
    # python mongodb_setup.py --uri mongodb://mongo:27017 --db iot_prod --equipments 200
    parser = argparse.ArgumentParser(description="Setup do MongoDB — Plataforma IoT")
    parser.add_argument("--uri",        default="mongodb://localhost:27017")
    parser.add_argument("--db",         default="iot_platform")
    parser.add_argument("--equipments", type=int, default=50)
    # --drop é útil para desenvolvimento: apaga tudo e recria do zero.
    # Em produção, usar com MUITO cuidado pois apaga dados reais!
    parser.add_argument("--drop",       action="store_true",
                        help="Apaga as coleções antes de recriar (útil para reset)")
    args = parser.parse_args()

    # Criamos a conexão com o MongoDB e testamos imediatamente com um "ping".
    # serverSelectionTimeoutMS=5000 evita que o script fique travado por minutos
    # se o MongoDB não estiver acessível — falha rápido com mensagem clara.
    print(f"Conectando ao MongoDB: {args.uri}")
    client = MongoClient(args.uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        print("Conexão OK.")
    except Exception as e:
        print(f"Erro: não foi possível conectar — {e}")
        exit(1)

    # client[args.db] seleciona (ou cria, se não existir) o banco de dados.
    # No MongoDB, bancos e coleções são criados automaticamente na primeira inserção.
    db = client[args.db]

    # Se --drop foi passado, apagamos as coleções para um reset completo.
    # drop_collection remove todos os dados E os índices da coleção.
    if args.drop:
        print("Apagando coleções existentes...")
        db.drop_collection("fabricas")
        db.drop_collection("equipamentos")

    # Executamos os 3 passos do setup em sequência:
    # 1. Criar coleções com validators de schema
    # 2. Criar índices para performance
    # 3. Popular com dados iniciais (seed)
    setup_collections(db)
    setup_indexes(db)
    seed_data(db, args.equipments)

    print(f"\nSetup concluído. Banco '{args.db}' em {args.uri}")
    # Sempre fechamos a conexão ao final — libera recursos no servidor MongoDB.
    client.close()


# Ponto de entrada padrão: este bloco executa apenas quando o arquivo é rodado diretamente.
# Se outro módulo importar este arquivo, o main() NÃO será chamado automaticamente.
if __name__ == "__main__":
    main()
