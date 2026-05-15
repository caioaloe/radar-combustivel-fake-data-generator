# Como funciona o streaming MongoDB -> Redis neste projeto

Este projeto usa um consumidor Python para capturar eventos em tempo real no MongoDB e atualizar rankings e metricas no Redis.

## Visao geral do fluxo

1. Eventos sao inseridos na colecao `marketplace.events` (ex.: `init/mongo_seed.py`).
2. O script `pipeline/mongodb_consumer.py` conecta no MongoDB e no Redis.
3. Ele processa um backfill opcional (eventos ja existentes).
4. Em seguida, abre um Change Stream com `col.watch(...)` para escutar novos `insert`.
5. Cada evento e normalizado (`pipeline/event_transformer.py`) e aplicado no Redis.

## Pre-requisito: Replica Set no MongoDB

O Change Stream so funciona com MongoDB em Replica Set (`rs0`).

No projeto:
- `docker-compose.yml` sobe o Mongo com `--replSet rs0`
- o servico `mongo-init` inicializa o ReplicaSet

## Entrada no MongoDB

O `init/mongo_seed.py` cria eventos fake e grava em `marketplace.events` com `insert_many`.
No modo stress, ele insere novos eventos para simular carga continua.

## Consumo em tempo real

No `pipeline/mongodb_consumer.py`:
- `MongoClient(MONGO_URI)` abre conexao com Mongo
- `Redis(...)` abre conexao com Redis
- `col.watch([{"$match": {"operationType": "insert"}}], full_document="updateLookup")` escuta inserts
- para cada `change`, processa `change["fullDocument"]`

## Transformacao de evento

No `pipeline/event_transformer.py`, o evento e padronizado:
- normaliza tipo (`view`, `search`, `order`, `rating`)
- converte tipos (`ts`, `lat`, `lon`, `stars`)
- extrai id numerico do restaurante para as chaves Redis

## Escrita no Redis

Para cada evento:
- Hash por restaurante: `resto:{id}`
- Rankings (Sorted Set):
  - `ranking:restaurants:views`
  - `ranking:restaurants:orders`
  - `ranking:dishes:searches`
- Series temporais (RedisTimeSeries):
  - `ts:resto:{id}:views`
  - `ts:resto:{id}:orders`
- Ratings atualizam media (`rating_sum` / `rating_count` -> `stars`)

## Resiliencia

O consumidor roda em loop infinito:
- se houver erro no stream, ele loga e tenta reconectar apos 2 segundos.

## Como executar

1. Suba infraestrutura:
   - `docker-compose up -d`
2. Popule dados:
   - `python init/mongo_seed.py`
3. Crie indices Redis:
   - `python init/redis_indexes.py`
4. Inicie consumer:
   - `python pipeline/mongodb_consumer.py`
5. Gere carga:
   - `python init/mongo_seed.py --stress --events 1000`