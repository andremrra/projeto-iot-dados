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

# --- IMPORTS PADRÃO DA LINGUAGEM ---
# Aqui importamos bibliotecas que já vêm com o Python, sem precisar instalar nada.
# argparse: lê os argumentos que passamos na linha de comando (ex: --topic iot-sensors)
# json: converte dicionários Python em texto JSON para enviar pelo Kafka
# random: gera valores aleatórios — essencial para simular dados reais de sensores
# time: usamos para controlar a velocidade de envio dos eventos (sleep/taxa por segundo)
# uuid: gera identificadores únicos para cada evento (como um "número de série" único)
# datetime: registra o momento exato em que cada leitura foi gerada (timestamp)
# typing: ajuda a documentar os tipos de dados esperados pelas funções
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List

# --- IMPORT DO KAFKA COM TRATAMENTO DE ERRO ---
# O kafka-python é uma biblioteca externa que precisa ser instalada separadamente.
# Usamos try/except para dar uma mensagem clara se o usuário esquecer de instalar.
# KafkaProducer: é o "enviador" de mensagens — o produtor no padrão produtor/consumidor do Kafka
# KafkaError: classe de exceção para capturar erros específicos do Kafka
try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    print("Erro: kafka-python não instalado. Execute: pip install kafka-python")
    exit(1)


# =============================================================================
# CONFIGURAÇÃO DOS SENSORES
# =============================================================================

# Aqui definimos as fábricas que existem no nosso sistema IoT.
# Cada fábrica tem um ID único, nome e coordenadas geográficas (latitude/longitude).
# Esses dados são usados para "localizar" de onde veio cada leitura de sensor.
FACTORIES = [
    {"id": "FAB-SP-01", "name": "Fábrica São Paulo", "lat": -23.5505, "lng": -46.6333},
    {"id": "FAB-RJ-01", "name": "Fábrica Rio de Janeiro", "lat": -22.9068, "lng": -43.1729},
    {"id": "FAB-MG-01", "name": "Fábrica Belo Horizonte", "lat": -19.9167, "lng": -43.9345},
]

# Tipos de equipamentos industriais que podem existir nas fábricas.
# Cada equipamento vai receber sensores associados a ele.
EQUIPMENT_TYPES = ["compressor", "pump", "motor", "conveyor", "turbine"]

# Aqui definimos os ranges normais de cada tipo de sensor para detectar anomalias.
# Cada sensor tem:
#   - unit: unidade de medida (celsius, bar, etc.)
#   - min/max: os limites físicos absolutos do sensor
#   - normal_range: a faixa esperada em operação normal — fora disso é suspeito!
# Esses valores são usados para decidir se uma leitura é "boa", "alerta" ou "ruim".
SENSOR_TYPES = {
    "temperature": {"unit": "celsius", "min": 20, "max": 100, "normal_range": (30, 70)},
    "humidity": {"unit": "percent", "min": 0, "max": 100, "normal_range": (40, 60)},
    "pressure": {"unit": "bar", "min": 0, "max": 20, "normal_range": (5, 15)},
    "vibration": {"unit": "mm/s", "min": 0, "max": 50, "normal_range": (0, 10)},
    "current": {"unit": "ampere", "min": 0, "max": 100, "normal_range": (10, 50)},
}

# Definimos os possíveis status de qualidade de uma leitura.
# A lista tem repetição intencional para simular a distribuição real:
# "good" aparece 4x → ~67% das leituras serão boas
# "warning" e "bad" aparecem 1x cada → ~17% cada uma
# É um truque simples para criar probabilidades sem precisar de numpy!
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
        # Guardamos os parâmetros de configuração como atributos da instância
        # para poder acessá-los depois nos outros métodos da classe.
        self.num_equipments = num_equipments
        self.anomaly_rate = anomaly_rate

        # Chamamos os métodos privados logo no __init__ para já inicializar
        # a lista de equipamentos e sensores. Assim, ao criar o objeto,
        # tudo já está pronto para gerar eventos.
        self.equipments = self._generate_equipments()
        self.sensors = self._generate_sensors()

    def _generate_equipments(self) -> List[Dict[str, Any]]:
        """Gera lista de equipamentos."""
        # Aqui criamos os N equipamentos que vão existir na simulação.
        # Cada equipamento é associado aleatoriamente a uma fábrica e recebe
        # um tipo (motor, bomba, etc.) e um ID no formato "EQ-0001".
        # O f"EQ-{i+1:04d}" garante que o número sempre tenha 4 dígitos (ex: EQ-0001, EQ-0050).
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
        # Para cada equipamento criado, geramos entre 2 e 5 sensores.
        # Usamos random.sample para escolher tipos SEM repetição — faz sentido
        # porque um equipamento não teria dois sensores do mesmo tipo.
        # O sensor_id global garante IDs únicos mesmo entre equipamentos diferentes.
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

        # Aqui está a lógica central de detecção de anomalia:
        # Geramos um número aleatório entre 0 e 1 e comparamos com anomaly_rate.
        # Se for menor que a taxa configurada (ex: 0.05 = 5%), é uma anomalia.
        # Isso simula falhas reais de equipamentos que acontecem com baixa frequência.
        is_anomaly = random.random() < self.anomaly_rate

        if is_anomaly:
            # Quando é uma anomalia, geramos um valor FORA da faixa normal.
            # Sorteamos se a anomalia é "por baixo" (sub-aquecimento, baixa pressão)
            # ou "por cima" (superaquecimento, pressão excessiva).
            # O fator 0.8/1.2 garante que o valor fique bem fora do normal range.
            if random.random() < 0.5:
                # Valor muito baixo
                value = random.uniform(config["min"], config["normal_range"][0] * 0.8)
            else:
                # Valor muito alto
                value = random.uniform(config["normal_range"][1] * 1.2, config["max"])
            # Anomalias são majoritariamente "bad" (70% das vezes) e às vezes "warning"
            quality = "bad" if random.random() < 0.7 else "warning"
        else:
            # Quando é uma leitura normal, usamos distribuição gaussiana (curva normal).
            # A média é o centro do range normal e o desvio padrão é 1/4 do intervalo.
            # Isso faz os valores se concentrarem no meio do range, como seria na vida real.
            value = random.gauss(
                (config["normal_range"][0] + config["normal_range"][1]) / 2,
                (config["normal_range"][1] - config["normal_range"][0]) / 4
            )
            # Clamp to normal range — garante que o valor gaussiano não extrapole os limites
            value = max(config["normal_range"][0], min(config["normal_range"][1], value))
            quality = random.choice(QUALITY_STATUS)

        # Arredondamos para 2 casas decimais para simular a precisão real dos sensores
        value = round(value, 2)

        # Montamos o dicionário do evento com todos os campos necessários.
        # uuid4() gera um ID único global (UUID) para cada evento — isso é importante
        # para rastrear e deduplicar mensagens no Kafka.
        # O timestamp em UTC com .isoformat() é o padrão para sistemas distribuídos.
        # Os metadados simulam informações reais de dispositivos IoT (firmware, bateria, sinal).
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
        # Um "batch" (lote) é um conjunto de eventos gerados de uma vez.
        # Isso é mais eficiente do que enviar um evento por vez,
        # pois reduz o overhead de comunicação com o Kafka.
        # Escolhemos sensores aleatórios a cada evento para simular
        # que diferentes equipamentos estão sendo lidos em paralelo.
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
        # Aqui configuramos o KafkaProducer, que é o componente responsável
        # por enviar mensagens ao Kafka. Cada parâmetro tem um propósito importante:
        #
        # bootstrap_servers: endereço do(s) servidor(es) Kafka para conectar
        # value_serializer: converte o dicionário Python em bytes JSON (o Kafka só aceita bytes)
        # key_serializer: serializa a chave da mensagem — usamos o equipment_id como chave
        #                 para garantir que eventos do mesmo equipamento vão para a mesma partição
        # acks='all': exige confirmação de TODOS os réplicas antes de considerar o envio OK
        #             (mais lento, mas mais seguro contra perda de dados)
        # retries=3: tenta reenviar até 3 vezes em caso de falha transitória de rede
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
        # Usamos o equipment_id como chave de particionamento.
        # No Kafka, mensagens com a mesma chave vão para a mesma partição,
        # garantindo que eventos de um mesmo equipamento sejam processados em ordem.
        key = event.get("equipment_id")  # Particionar por equipamento
        try:
            future = self.producer.send(self.topic, key=key, value=event)
            # Não esperar confirmação para cada mensagem (async)
            # O envio é assíncrono: não esperamos a confirmação aqui para não travar o loop.
            # O flush() no send_batch() é onde garantimos que tudo foi enviado.
        except KafkaError as e:
            print(f"❌ Erro ao enviar: {e}")

    def send_batch(self, events: List[Dict[str, Any]]) -> None:
        """Envia um batch de eventos."""
        # Enviamos todos os eventos do batch e depois chamamos flush().
        # O flush() bloqueia até que todas as mensagens pendentes sejam confirmadas pelo Kafka.
        # Sem o flush(), poderíamos perder mensagens ao encerrar o programa.
        for event in events:
            self.send(event)
        self.producer.flush()

    def close(self):
        """Fecha a conexão."""
        # Sempre fechar o producer ao terminar — libera recursos e garante
        # que nenhuma mensagem fique "presa" no buffer interno.
        self.producer.close()


# =============================================================================
# MAIN
# =============================================================================

def main():
    # argparse é a forma padrão em Python de criar interfaces de linha de comando.
    # Cada add_argument define um parâmetro opcional com nome, tipo, valor padrão e descrição.
    # Isso permite que qualquer pessoa use o script sem precisar editar o código!
    # Exemplo de uso: python sensor_simulator.py --equipments 100 --anomaly-rate 0.10
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
    # --dry-run é um modo de teste: gera e imprime eventos SEM enviar para o Kafka.
    # Útil para verificar se os dados gerados estão corretos antes de subir o Kafka.
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Apenas imprime eventos, não envia para Kafka"
    )

    args = parser.parse_args()

    # Criamos o simulador com os parâmetros passados pelo usuário.
    # Neste ponto os equipamentos e sensores já são gerados internamente no __init__.
    print("🏭 Inicializando simulador de sensores IoT...")
    simulator = SensorSimulator(
        num_equipments=args.equipments,
        anomaly_rate=args.anomaly_rate
    )
    print(f"   - Equipamentos: {len(simulator.equipments)}")
    print(f"   - Sensores: {len(simulator.sensors)}")
    print(f"   - Taxa de anomalias: {args.anomaly_rate * 100:.1f}%")

    # Só criamos o produtor Kafka se não estivermos em modo dry-run.
    # Isso evita erros de conexão quando só queremos testar a geração de dados.
    producer = None
    if not args.dry_run:
        try:
            producer = KafkaSensorProducer(args.bootstrap_servers, args.topic)
        except Exception as e:
            print(f"❌ Erro ao conectar ao Kafka: {e}")
            print("   Dica: Verifique se o Kafka está rodando e acessível.")
            return

    # Loop principal de geração de eventos.
    # Usamos while True para rodar indefinidamente (até Ctrl+C ou duração atingida).
    print(f"\n🚀 Iniciando geração de eventos ({args.events_per_second}/s)...")
    print("   Pressione Ctrl+C para parar.\n")

    start_time = time.time()
    total_events = 0
    total_anomalies = 0

    try:
        while True:
            batch_start = time.time()

            # Geramos um batch com o tamanho configurado (padrão: 100 eventos por segundo)
            events = simulator.generate_batch(args.events_per_second)

            # Contamos quantas anomalias estão neste batch para acompanhar as estatísticas.
            # Usamos sum() com generator expression — é pythônico e eficiente.
            anomalies_in_batch = sum(1 for e in events if e.get("is_anomaly"))
            total_anomalies += anomalies_in_batch
            total_events += len(events)

            # No modo dry-run mostramos apenas os 3 primeiros eventos para não poluir o terminal.
            # No modo normal, enviamos todos para o Kafka.
            if args.dry_run:
                for event in events[:3]:  # Mostrar apenas primeiros 3
                    print(json.dumps(event, indent=2))
                print(f"... e mais {len(events) - 3} eventos\n")
            else:
                producer.send_batch(events)

            # Calculamos e exibimos as estatísticas em tempo real.
            # O \r (carriage return) faz o cursor voltar ao início da linha,
            # sobrescrevendo a linha anterior — cria o efeito de "atualização no lugar".
            elapsed = time.time() - start_time
            rate = total_events / elapsed if elapsed > 0 else 0
            print(f"\r📊 Eventos: {total_events:,} | "
                  f"Anomalias: {total_anomalies:,} ({total_anomalies/total_events*100:.1f}%) | "
                  f"Taxa: {rate:.0f}/s | "
                  f"Tempo: {elapsed:.0f}s", end="")

            # Se o usuário passou --duration, verificamos se já atingimos o tempo limite.
            # duration=0 significa "rodar para sempre", por isso a checagem com > 0.
            if args.duration > 0 and elapsed >= args.duration:
                print(f"\n\n⏱️ Duração de {args.duration}s atingida.")
                break

            # Controlamos a taxa de envio dormindo o tempo restante do segundo.
            # Se o batch demorou 0.3s para gerar, dormimos 0.7s para completar 1 segundo.
            # max(0, ...) garante que não tentamos dormir tempo negativo.
            batch_duration = time.time() - batch_start
            sleep_time = max(0, 1.0 - batch_duration)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        # Capturamos o Ctrl+C aqui para encerrar de forma limpa,
        # sem imprimir a mensagem de erro padrão do Python.
        print("\n\n🛑 Interrompido pelo usuário.")
    finally:
        # O bloco finally SEMPRE executa, seja por Ctrl+C, duração atingida ou erro.
        # Isso garante que o producer Kafka sempre será fechado corretamente,
        # liberando conexões e enviando mensagens ainda no buffer.
        if producer:
            producer.close()

        # Exibimos o resumo final com as estatísticas totais da execução.
        elapsed = time.time() - start_time
        print(f"\n📈 Resumo:")
        print(f"   - Total de eventos: {total_events:,}")
        print(f"   - Total de anomalias: {total_anomalies:,}")
        print(f"   - Tempo total: {elapsed:.1f}s")
        print(f"   - Taxa média: {total_events/elapsed:.0f} eventos/s")


# Ponto de entrada padrão do Python: este bloco só executa se rodarmos o arquivo diretamente.
# Se outro módulo importar este arquivo, o main() NÃO será chamado automaticamente.
if __name__ == "__main__":
    main()
