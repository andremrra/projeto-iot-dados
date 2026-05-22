# Modelagem de Dados — Plataforma IoT Industrial

## ADR-001: Modelagem NoSQL para Cadastro de Equipamentos

### Status
Aceito

---

### Contexto

O sistema precisa armazenar o cadastro de equipamentos industriais. Cada equipamento possui:

- Informações próprias: nome, tipo, status, data de instalação, cronograma de manutenção
- Uma fábrica onde está instalado (com nome e coordenadas geográficas)
- Um conjunto de sensores acoplados (entre 2 e 50 por equipamento), cada um com tipo, unidade de medida e ranges de operação esperados

O pipeline de streaming consome eventos do Kafka que contêm `sensor_id` e `equipment_id`. Para detectar anomalias, o pipeline precisa consultar o MongoDB para obter o range normal do sensor correspondente. Essa é a leitura crítica do sistema.

---

### Decisão

#### Coleção `equipamentos` — sensores embutidos, fábrica parcialmente desnormalizada

Os sensores são armazenados como array **embutido** dentro do documento do equipamento. Os dados básicos da fábrica (id, nome e coordenadas) são **copiados** dentro do documento, mantendo também uma referência `factory_id` para a coleção `fabricas`.

```json
{
  "_id": "EQ-0001",
  "name": "Compressor 1",
  "type": "compressor",
  "factory_id": "FAB-SP-01",
  "factory": {
    "id": "FAB-SP-01",
    "name": "Fábrica São Paulo",
    "location": { "lat": -23.5505, "lng": -46.6333 }
  },
  "sensors": [
    {
      "id": "SENS-00001",
      "type": "temperature",
      "unit": "celsius",
      "range": { "min": 0, "max": 150 },
      "normal_range": { "min": 30, "max": 70 },
      "active": true
    },
    {
      "id": "SENS-00002",
      "type": "vibration",
      "unit": "mm/s",
      "range": { "min": 0, "max": 50 },
      "normal_range": { "min": 0, "max": 10 },
      "active": true
    }
  ],
  "maintenance": {
    "schedule": "monthly",
    "last_maintenance": "2024-08-10",
    "next_maintenance": "2024-09-10"
  },
  "installed_at": "2022-06-15",
  "status": "active"
}
```

#### Coleção `fabricas` — coleção de referência

Armazena os dados completos de cada fábrica. Serve como fonte de verdade para atualizações e como coleção de referência para relatórios agregados por fábrica.

```json
{
  "_id": "FAB-SP-01",
  "name": "Fábrica São Paulo",
  "location": { "lat": -23.5505, "lng": -46.6333 },
  "address": "Av. Industrial, 1000, São Paulo, SP",
  "active": true
}
```

---

### Alternativas Consideradas

#### Alternativa A — Coleção separada para sensores

Cada sensor seria um documento independente em uma coleção `sensores`, referenciado pelo `equipment_id`.

- **Prós:** normalização completa, fácil atualizar um sensor isoladamente
- **Contras:** a leitura crítica do pipeline (obter o range de um sensor) exigiria duas queries — primeiro buscar o sensor, depois o equipamento. Em um pipeline de streaming com alta frequência, esse custo se multiplica. Além disso, sensor e equipamento são sempre lidos juntos — não há caso de uso em que se lê sensor sem equipamento.

#### Alternativa B — Fábrica totalmente embutida (sem coleção separada)

Embutir todos os dados da fábrica dentro de cada equipamento, sem manter coleção `fabricas`.

- **Prós:** leitura em um único documento, sem nenhuma referência
- **Contras:** se o nome ou endereço de uma fábrica mudar, seria necessário atualizar dezenas de documentos de equipamento. A coleção `fabricas` existe justamente para que essa atualização seja feita em um único lugar.

#### Alternativa C — PostgreSQL com JSONB

Usar o PostgreSQL (já presente na stack) com coluna JSONB para os sensores.

- **Prós:** ACID completo, familiaridade com SQL
- **Contras:** o schema de sensores varia entre tipos de equipamento e pode crescer com novos tipos. MongoDB oferece flexibilidade de schema nativa, índices em campos de arrays embutidos (`sensors.id`, `sensors.type`) e validators JSON nativos que combinam melhor com dados hierárquicos e semi-estruturados.

---

### Consequências

#### Positivas

- A leitura crítica do pipeline (buscar ranges de um sensor pelo `equipment_id`) é resolvida em **uma única query**
- Schema flexível permite adicionar novos tipos de sensor sem alterar documentos existentes
- Índice em `sensors.id` permite localizar qualquer sensor diretamente, sem conhecer o equipamento previamente
- Dados da fábrica disponíveis na leitura do equipamento sem necessidade de join

#### Negativas

- Se o nome de uma fábrica mudar, é necessário atualizar também o campo `factory.name` em todos os equipamentos daquela fábrica (mitigado porque mudanças de nome são raras e podem ser feitas com `updateMany`)
- Documentos de equipamento crescem conforme sensores são adicionados (limitado a 50 sensores, mantendo o documento abaixo de 16 MB)

---

### Índices Criados

| Coleção | Índice | Justificativa |
|---|---|---|
| `equipamentos` | `factory_id` | Listar todos os equipamentos de uma fábrica |
| `equipamentos` | `type` | Filtrar equipamentos por tipo (compressor, pump...) |
| `equipamentos` | `status` | Listar apenas equipamentos ativos |
| `equipamentos` | `sensors.id` | Localizar sensor específico para detecção de anomalia |
| `equipamentos` | `sensors.type` | Buscar todos os equipamentos com sensor de temperatura |
| `equipamentos` | `factory_id + status` | Query composta mais frequente: ativos de uma fábrica |
| `fabricas` | `active` | Listar fábricas ativas |

---

### Evolução Planejada

| Cenário | Como o modelo se comporta |
|---|---|
| Novo tipo de sensor (ex: `flow`) | Adicionar entrada em `SENSOR_CONFIGS` no setup. Documentos existentes não precisam ser alterados — o novo campo é opcional. |
| Nova fábrica | Inserir documento em `fabricas`. Os próximos equipamentos referenciados a ela seguem o padrão atual. |
| Sensor desativado | Setar `sensors[n].active = false` no documento do equipamento. O pipeline filtra por `active: true`. |
| Equipamento em manutenção | Atualizar `status` para `"maintenance"`. O índice `idx_status` garante que filtros de ativos não retornem esse equipamento. |
| Histórico de manutenções | Adicionar campo `maintenance.history` como array no documento. O schema validator usa `validationLevel: moderate`, portanto campos novos não quebram validação. |
| Aumento de escala (10x equipamentos) | Os índices existentes sustentam o crescimento. Se a coleção ultrapassar 100k documentos, avaliar sharding por `factory_id`. |
