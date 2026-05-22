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

import argparse
import random
from datetime import date, timedelta

try:
    from pymongo import MongoClient, ASCENDING, IndexModel
    from pymongo.errors import CollectionInvalid
except ImportError:
    print("Erro: pymongo não instalado. Execute: pip install pymongo")
    exit(1)


# =============================================================================
# DADOS DE REFERÊNCIA
# =============================================================================

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

EQUIPMENT_TYPES = ["compressor", "pump", "motor", "conveyor", "turbine"]

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

MAINTENANCE_SCHEDULES = ["weekly", "monthly", "quarterly", "semiannual"]
SCHEDULE_DAYS = {"weekly": 7, "monthly": 30, "quarterly": 90, "semiannual": 180}


# =============================================================================
# JSON SCHEMA VALIDATORS
# =============================================================================

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

EQUIPAMENTOS_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["_id", "name", "type", "factory_id", "factory", "sensors", "status", "installed_at"],
        "properties": {
            "_id":        {"bsonType": "string"},
            "name":       {"bsonType": "string"},
            "type":       {"bsonType": "string", "enum": EQUIPMENT_TYPES},
            "factory_id": {"bsonType": "string"},
            "factory": {
                "bsonType": "object",
                "required": ["id", "name", "location"],
                "properties": {
                    "id":       {"bsonType": "string"},
                    "name":     {"bsonType": "string"},
                    "location": {"bsonType": "object"},
                },
            },
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
    try:
        db.create_collection(name, validator=validator, validationLevel="moderate")
        print(f"  Coleção '{name}' criada.")
    except CollectionInvalid:
        db.command("collMod", name, validator=validator, validationLevel="moderate")
        print(f"  Coleção '{name}' já existe — validator atualizado.")


def setup_collections(db):
    print("\n[1/3] Criando coleções e validators de schema...")
    create_collection_safe(db, "fabricas", FABRICAS_VALIDATOR)
    create_collection_safe(db, "equipamentos", EQUIPAMENTOS_VALIDATOR)


def setup_indexes(db):
    print("\n[2/3] Criando índices...")

    db.fabricas.create_indexes([
        IndexModel([("active", ASCENDING)], name="idx_active"),
    ])
    print("  fabricas  → idx_active")

    db.equipamentos.create_indexes([
        # Buscas simples por atributo principal
        IndexModel([("factory_id", ASCENDING)],   name="idx_factory_id"),
        IndexModel([("type",        ASCENDING)],   name="idx_type"),
        IndexModel([("status",      ASCENDING)],   name="idx_status"),
        # Busca de sensor específico dentro de qualquer equipamento
        IndexModel([("sensors.id",   ASCENDING)],  name="idx_sensor_id"),
        IndexModel([("sensors.type", ASCENDING)],  name="idx_sensor_type"),
        # Query mais comum: equipamentos ativos de uma fábrica
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
    rng = random.Random(eq_index)

    eq_type    = rng.choice(EQUIPMENT_TYPES)
    num_sensors = rng.randint(2, 5)
    sensor_types = rng.sample(list(SENSOR_CONFIGS.keys()), num_sensors)

    installed  = date(2022, 1, 1) + timedelta(days=rng.randint(0, 700))
    schedule   = rng.choice(MAINTENANCE_SCHEDULES)
    last_maint = installed + timedelta(days=rng.randint(30, 300))
    next_maint = last_maint + timedelta(days=SCHEDULE_DAYS[schedule])

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
    print(f"\n[3/3] Populando seed ({num_equipments} equipamentos)...")

    # Fábricas
    for factory in FACTORIES:
        db.fabricas.replace_one({"_id": factory["_id"]}, factory, upsert=True)
    print(f"  {len(FACTORIES)} fábricas inseridas/atualizadas.")

    # Equipamentos distribuídos entre as fábricas em round-robin
    sensor_counter = 1
    for i in range(1, num_equipments + 1):
        factory = FACTORIES[(i - 1) % len(FACTORIES)]
        doc, sensor_counter = build_equipment(i, sensor_counter, factory)
        db.equipamentos.replace_one({"_id": doc["_id"]}, doc, upsert=True)

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
    parser = argparse.ArgumentParser(description="Setup do MongoDB — Plataforma IoT")
    parser.add_argument("--uri",        default="mongodb://localhost:27017")
    parser.add_argument("--db",         default="iot_platform")
    parser.add_argument("--equipments", type=int, default=50)
    parser.add_argument("--drop",       action="store_true",
                        help="Apaga as coleções antes de recriar (útil para reset)")
    args = parser.parse_args()

    print(f"Conectando ao MongoDB: {args.uri}")
    client = MongoClient(args.uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        print("Conexão OK.")
    except Exception as e:
        print(f"Erro: não foi possível conectar — {e}")
        exit(1)

    db = client[args.db]

    if args.drop:
        print("Apagando coleções existentes...")
        db.drop_collection("fabricas")
        db.drop_collection("equipamentos")

    setup_collections(db)
    setup_indexes(db)
    seed_data(db, args.equipments)

    print(f"\nSetup concluído. Banco '{args.db}' em {args.uri}")
    client.close()


if __name__ == "__main__":
    main()
