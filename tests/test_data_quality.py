import json
import os

import pytest

SAMPLE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "data", "sample", "events_bronze_sample.json"
)

REQUIRED_FIELDS = {"event_id", "sensor_id", "equipment_id", "factory_id",
                   "measurement_type", "value", "timestamp", "quality", "is_anomaly"}
VALID_MEASUREMENT_TYPES = {"temperature", "humidity", "pressure", "vibration", "current"}
VALID_QUALITY_VALUES = {"good", "warning", "bad"}
VALID_FACTORY_IDS = {"FAB-SP-01", "FAB-RJ-01", "FAB-MG-01"}
VALUE_RANGES = {
    "temperature": (0, 150),
    "humidity": (0, 100),
    "pressure": (0, 20),
    "vibration": (0, 50),
    "current": (0, 100),
}


@pytest.fixture(scope="session")
def records():
    with open(SAMPLE_PATH) as f:
        return [json.loads(line) for line in f if line.strip()]


# --- Assertion 1: campos obrigatórios ---
def test_required_fields_present(records):
    for r in records:
        missing = REQUIRED_FIELDS - set(r.keys())
        assert not missing, f"Campos ausentes em {r.get('event_id')}: {missing}"


# --- Assertion 2: tipo de medição válido ---
def test_measurement_type_valid(records):
    for r in records:
        assert r["measurement_type"] in VALID_MEASUREMENT_TYPES, (
            f"Tipo inválido: {r['measurement_type']} (evento {r['event_id']})"
        )


# --- Assertion 3: qualidade válida ---
def test_quality_valid(records):
    for r in records:
        assert r["quality"] in VALID_QUALITY_VALUES, (
            f"quality inválido: {r['quality']} (evento {r['event_id']})"
        )


# --- Assertion 4: factory_id reconhecido ---
def test_factory_id_known(records):
    for r in records:
        assert r["factory_id"] in VALID_FACTORY_IDS, (
            f"factory_id desconhecido: {r['factory_id']}"
        )


# --- Assertion 5: valor dentro do range físico do sensor ---
def test_value_within_physical_range(records):
    for r in records:
        mtype = r["measurement_type"]
        value = r["value"]
        if mtype in VALUE_RANGES:
            lo, hi = VALUE_RANGES[mtype]
            assert lo <= value <= hi, (
                f"{mtype}={value} fora do range [{lo},{hi}] (evento {r['event_id']})"
            )


# --- Assertion 6: taxa de duplicatas na Bronze abaixo de 1% ---
# Bronze é append-only e pode ter duplicatas — a Silver remove via dropDuplicates.
# Assertamos que a taxa é baixa (< 1%), não zero.
def test_no_duplicate_event_ids(records):
    ids = [r["event_id"] for r in records]
    dup_count = len(ids) - len(set(ids))
    dup_rate = dup_count / len(ids)
    assert dup_rate < 0.01, (
        f"Taxa de duplicatas muito alta na Bronze: {dup_rate:.1%} ({dup_count} de {len(ids)})"
    )


# --- Assertion 7: is_anomaly é booleano ---
def test_is_anomaly_is_boolean(records):
    for r in records:
        assert isinstance(r["is_anomaly"], bool), (
            f"is_anomaly não é boolean no evento {r['event_id']}: {type(r['is_anomaly'])}"
        )


# --- Assertion 8: valor é numérico ---
def test_value_is_numeric(records):
    for r in records:
        assert isinstance(r["value"], (int, float)), (
            f"value não numérico no evento {r['event_id']}: {r['value']}"
        )


# --- Assertion 9: timestamp não vazio ---
def test_timestamp_not_empty(records):
    for r in records:
        assert r["timestamp"] and isinstance(r["timestamp"], str), (
            f"timestamp inválido no evento {r['event_id']}"
        )


# --- Assertion 10: event_id é UUID válido (formato) ---
def test_event_id_format(records):
    import re
    uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    for r in records:
        assert uuid_re.match(r["event_id"]), (
            f"event_id não é UUID válido: {r['event_id']}"
        )
