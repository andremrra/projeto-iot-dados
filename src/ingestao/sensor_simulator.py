#!/usr/bin/env python3
"""
Simulador de Sensores IoT
=========================
Gera eventos de sensores industriais e envia para Apache Kafka.

Uso:
    python sensor_simulator.py --bootstrap-servers localhost:9092 --topic iot-sensors

Requisitos:
    pip install kafka-python faker
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    print("Erro: kafka-python não instalado. Execute: pip install kafka-python")
    exit(1)


# =============================================================================
# CONFIGURAÇÃO DOS SENSORES
# =============================================================================

# Fábricas disponíveis
FACTORIES = [
    {"id": "FAB-SP-01", "name": "Fábrica São Paulo", "lat": -23.5505, "lng": -46.6333},
    {"id": "FAB-RJ-01", "name": "Fábrica Rio de Janeiro", "lat": -22.9068, "lng": -43.1729},
    {"id": "FAB-MG-01", "name": "Fábrica Belo Horizonte", "lat": -19.9167, "lng": -43.9345},
]

# Tipos de equipamentos
EQUIPMENT_TYPES = ["compressor", "pump", "motor", "conveyor", "turbine"]

# Tipos de sensores e seus ranges
SENSOR_TYPES = {
    "temperature": {"unit": "celsius", "min": 20, "max": 100, "normal_range": (30, 70)},
    "humidity": {"unit": "percent", "min": 0, "max": 100, "normal_range": (40, 60)},
    "pressure": {"unit": "bar", "min": 0, "max": 20, "normal_range": (5, 15)},
    "vibration": {"unit": "mm/s", "min": 0, "max": 50, "normal_range": (0, 10)},
    "current": {"unit": "ampere", "min": 0, "max": 100, "normal_range": (10, 50)},
}

# Status possíveis
QUALITY_STATUS = ["good", "good", "good", "good", "warning", "bad"]  # 66% good, 17% warning, 17% bad


# =============================================================================
# GERAÇÃO DE DADOS
# =============================================================================

class SensorSimulator:
    """Simula sensores IoT gerando dados realistas."""
    
    def __init__(self, num_equipments: int = 50, anomaly_rate: float = 0.05):
        """
        Args:
            num_equipments: Número de equipamentos a simular
            anomaly_rate: Taxa de anomalias (0.0 a 1.0)
        """
        self.num_equipments = num_equipments
        self.anomaly_rate = anomaly_rate
        self.equipments = self._generate_equipments()
        self.sensors = self._generate_sensors()
        
    def _generate_equipments(self) -> List[Dict[str, Any]]:
        """Gera lista de equipamentos."""
        equipments = []
        for i in range(self.num_equipments):
            factory = random.choice(FACTORIES)
            equipment = {
                "id": f"EQ-{i+1:04d}",
                "name": f"{random.choice(EQUIPMENT_TYPES).title()} {i+1}",
                "type": random.choice(EQUIPMENT_TYPES),
                "factory_id": factory["id"],
                "factory_name": factory["name"],
            }
            equipments.append(equipment)
        return equipments
    
    def _generate_sensors(self) -> List[Dict[str, Any]]:
        """Gera lista de sensores para cada equipamento."""
        sensors = []
        sensor_id = 1
        for equipment in self.equipments:
            # Cada equipamento tem 2-5 sensores
            num_sensors = random.randint(2, 5)
            sensor_types = random.sample(list(SENSOR_TYPES.keys()), num_sensors)
            
            for sensor_type in sensor_types:
                sensor = {
                    "id": f"SENS-{sensor_id:05d}",
                    "equipment_id": equipment["id"],
                    "factory_id": equipment["factory_id"],
                    "type": sensor_type,
                    "config": SENSOR_TYPES[sensor_type],
                }
                sensors.append(sensor)
                sensor_id += 1
        return sensors
    
    def generate_event(self, sensor: Dict[str, Any]) -> Dict[str, Any]:
        """Gera um evento de leitura de sensor."""
        config = sensor["config"]
        
        # Decidir se é anomalia
        is_anomaly = random.random() < self.anomaly_rate
        
        if is_anomaly:
            # Valor fora do range normal
            if random.random() < 0.5:
                # Valor muito baixo
                value = random.uniform(config["min"], config["normal_range"][0] * 0.8)
            else:
                # Valor muito alto
                value = random.uniform(config["normal_range"][1] * 1.2, config["max"])
            quality = "bad" if random.random() < 0.7 else "warning"
        else:
            # Valor normal
            value = random.gauss(
                (config["normal_range"][0] + config["normal_range"][1]) / 2,
                (config["normal_range"][1] - config["normal_range"][0]) / 4
            )
            # Clamp to normal range
            value = max(config["normal_range"][0], min(config["normal_range"][1], value))
            quality = random.choice(QUALITY_STATUS)
        
        # Arredondar valor
        value = round(value, 2)
        
        event = {
            "event_id": str(uuid.uuid4()),
            "sensor_id": sensor["id"],
            "equipment_id": sensor["equipment_id"],
            "factory_id": sensor["factory_id"],
            "measurement_type": sensor["type"],
            "value": value,
            "unit": config["unit"],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "quality": quality,
            "is_anomaly": is_anomaly,
            "metadata": {
                "firmware_version": f"{random.randint(1, 3)}.{random.randint(0, 9)}.{random.randint(0, 9)}",
                "battery_level": random.randint(20, 100),
                "signal_strength": random.randint(-90, -30),
            }
        }
        
        return event
    
    def generate_batch(self, batch_size: int = 100) -> List[Dict[str, Any]]:
        """Gera um batch de eventos."""
        events = []
        for _ in range(batch_size):
            sensor = random.choice(self.sensors)
            event = self.generate_event(sensor)
            events.append(event)
        return events


# =============================================================================
# PRODUTOR KAFKA
# =============================================================================

class KafkaSensorProducer:
    """Envia eventos de sensores para Kafka."""
    
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
        )
        print(f"✅ Conectado ao Kafka: {bootstrap_servers}")
        print(f"📤 Tópico: {topic}")
    
    def send(self, event: Dict[str, Any]) -> None:
        """Envia um evento para Kafka."""
        key = event.get("equipment_id")  # Particionar por equipamento
        try:
            future = self.producer.send(self.topic, key=key, value=event)
            # Não esperar confirmação para cada mensagem (async)
        except KafkaError as e:
            print(f"❌ Erro ao enviar: {e}")
    
    def send_batch(self, events: List[Dict[str, Any]]) -> None:
        """Envia um batch de eventos."""
        for event in events:
            self.send(event)
        self.producer.flush()
    
    def close(self):
        """Fecha a conexão."""
        self.producer.close()


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Simulador de Sensores IoT")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)"
    )
    parser.add_argument(
        "--topic",
        default="iot-sensors",
        help="Tópico Kafka (default: iot-sensors-raw)"
    )
    parser.add_argument(
        "--equipments",
        type=int,
        default=50,
        help="Número de equipamentos (default: 50)"
    )
    parser.add_argument(
        "--events-per-second",
        type=int,
        default=100,
        help="Eventos por segundo (default: 100)"
    )
    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=0.05,
        help="Taxa de anomalias 0.0-1.0 (default: 0.05)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        help="Duração em segundos (0 = infinito)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Apenas imprime eventos, não envia para Kafka"
    )
    
    args = parser.parse_args()
    
    # Criar simulador
    print("🏭 Inicializando simulador de sensores IoT...")
    simulator = SensorSimulator(
        num_equipments=args.equipments,
        anomaly_rate=args.anomaly_rate
    )
    print(f"   - Equipamentos: {len(simulator.equipments)}")
    print(f"   - Sensores: {len(simulator.sensors)}")
    print(f"   - Taxa de anomalias: {args.anomaly_rate * 100:.1f}%")
    
    # Criar produtor Kafka (se não for dry-run)
    producer = None
    if not args.dry_run:
        try:
            producer = KafkaSensorProducer(args.bootstrap_servers, args.topic)
        except Exception as e:
            print(f"❌ Erro ao conectar ao Kafka: {e}")
            print("   Dica: Verifique se o Kafka está rodando e acessível.")
            return
    
    # Loop principal
    print(f"\n🚀 Iniciando geração de eventos ({args.events_per_second}/s)...")
    print("   Pressione Ctrl+C para parar.\n")
    
    start_time = time.time()
    total_events = 0
    total_anomalies = 0
    
    try:
        while True:
            batch_start = time.time()
            
            # Gerar batch de eventos
            events = simulator.generate_batch(args.events_per_second)
            
            # Contar anomalias
            anomalies_in_batch = sum(1 for e in events if e.get("is_anomaly"))
            total_anomalies += anomalies_in_batch
            total_events += len(events)
            
            # Enviar ou imprimir
            if args.dry_run:
                for event in events[:3]:  # Mostrar apenas primeiros 3
                    print(json.dumps(event, indent=2))
                print(f"... e mais {len(events) - 3} eventos\n")
            else:
                producer.send_batch(events)
            
            # Estatísticas
            elapsed = time.time() - start_time
            rate = total_events / elapsed if elapsed > 0 else 0
            print(f"\r📊 Eventos: {total_events:,} | "
                  f"Anomalias: {total_anomalies:,} ({total_anomalies/total_events*100:.1f}%) | "
                  f"Taxa: {rate:.0f}/s | "
                  f"Tempo: {elapsed:.0f}s", end="")
            
            # Verificar duração
            if args.duration > 0 and elapsed >= args.duration:
                print(f"\n\n⏱️ Duração de {args.duration}s atingida.")
                break
            
            # Esperar para manter a taxa desejada
            batch_duration = time.time() - batch_start
            sleep_time = max(0, 1.0 - batch_duration)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Interrompido pelo usuário.")
    finally:
        if producer:
            producer.close()
        
        # Estatísticas finais
        elapsed = time.time() - start_time
        print(f"\n📈 Resumo:")
        print(f"   - Total de eventos: {total_events:,}")
        print(f"   - Total de anomalias: {total_anomalies:,}")
        print(f"   - Tempo total: {elapsed:.1f}s")
        print(f"   - Taxa média: {total_events/elapsed:.0f} eventos/s")


if __name__ == "__main__":
    main()
