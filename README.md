# ⛽ Radar Combustível — Plataforma Completa

Plataforma de análise de preços de combustíveis com base documental no **MongoDB**, pipeline de transformação e sincronização com **Redis**, e dashboard interativo em **Streamlit**. Inclui seed de dados com **Faker** e orquestração via **Docker Compose**.

---

## 📋 Índice

- [Arquitetura](#arquitetura)
- [Estrutura Redis](#estrutura-redis)
- [Pré-requisitos](#pré-requisitos)
- [Execução](#execução)
- [Estrutura do Repositório](#estrutura-do-repositório)
- [Variáveis de Ambiente](#variáveis-de-ambiente)
- [Painéis do Dashboard](#painéis-do-dashboard)
- [Consultas Redis de Demonstração](#consultas-redis-de-demonstração)
- [Referência do Trabalho](#referência-do-trabalho)

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                      PLATAFORMA RADAR COMBUSTÍVEL               │
└─────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                    CAMADA DE DADOS (MongoDB)                      │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────┐│
│  │   postos        │  │  eventos_preco   │  │ buscas_usuarios  ││
│  │ (Cadastro)      │  │ (Preços em tempo)│  │ (Pesquisas)      ││
│  └─────────────────┘  └──────────────────┘  └──────────────────┘│
│                                                                   │
│  ┌──────────────────────────┐  ┌─────────────────────────────┐  │
│  │ avaliacoes_interacoes    │  │ localizacoes_postos         │  │
│  │ (Avaliações/Favoritos)   │  │ (Índices geoespaciais)      │  │
│  └──────────────────────────┘  └─────────────────────────────┘  │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────▼──────────┐
                    │  SEED INICIAL      │
                    │ (Faker + Batch)    │
                    └────────────────────┘
                              │
                    ┌─────────▼──────────────────────────┐
                    │   PIPELINE MongoDB → Redis         │
                    │  (Batch + Change Stream)           │
                    └───────────┬──────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────┐
│              CAMADA DE CACHE & ANÁLISE (Redis)                   │
├───────────────────────────────────────────────────────────────────┤
│                                                                    │
│  HASH       ZSET        GEO          TS (TimeSeries)             │
│  ├─ posto  ├─ ranking  ├─ postos   ├─ ts:preco_avg              │
│  │ :{id}   │ :preco    │   (geo)   │   :{comb}:{uf}             │
│  │         │ :buscas   │           │                             │
│  │         │ :variacao │           │                             │
│  │         │           │           │                             │
│  └─ stats  │ HASH      │           │                             │
│     :preco │ ├─ stats  │           │                             │
│     _medio │ │ :preco  │           │                             │
│            │ │ _medio  │           │                             │
│            └─────────────────────────                            │
│                                                                    │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                    ┌───────────▼────────────┐
                    │   DASHBOARD Streamlit   │
                    │  (Web UI Interativa)    │
                    └────────────────────────┘
```

**Fluxo:**
1. **Seed**: Script popula MongoDB com dados gerados (Faker)
2. **Batch**: Pipeline lê agregações e carrega no Redis
3. **Stream**: Change Stream monitora `eventos_preco` em tempo real
4. **Dashboard**: Streamlit consulta Redis e exibe em tempo real

---

## 💾 Estrutura Redis

O pipeline cria as seguintes estruturas Redis para otimizar consultas:

### **1. HASH — Cadastro Resumido de Postos**
```
posto:{posto_id}
├─ nome (string): nome_fantasia
├─ bandeira (string): BR, Shell, Ipiranga, etc
├─ cidade (string)
├─ uf (string): SP, RJ, MG, etc
└─ ativo (string): "true"/"false"
```
**Uso**: Resolver informações do posto rapidamente sem hit no MongoDB.

---

### **2. GEO — Índice Geoespacial**
```
geo:postos
├─ GEOADD membro:{posto_id} lng lat
└─ Operações: GEORADIUS, GEOPOS, GEODIST, etc
```
**Uso**: Busca de postos por proximidade geográfica.

---

### **3. ZSET — Ranking de Preços por Combustível e UF**
```
ranking:preco:{combustivel}:{uf}
├─ score: preço (em BRL/L)
├─ member: posto_id
└─ Exemplos:
   ├─ ranking:preco:GASOLINA_COMUM:SP
   ├─ ranking:preco:ETANOL:RJ
   └─ ranking:preco:DIESEL_S10:MG
```
**Uso**: Top N menores ou maiores preços por combustível/estado.

---

### **4. ZSET — Ranking de Buscas por Cidade**
```
ranking:buscas
├─ score: contagem de buscas (usuarios)
├─ member: "cidade|uf"
└─ Exemplo: "São Paulo|SP" → 1500
```
**Uso**: Cidades mais buscadas (volume de demanda).

---

### **5. ZSET — Ranking de Variação de Preço**
```
ranking:variacao:{combustivel}
├─ score: |variacao_pct| (valor absoluto)
├─ member: posto_id
└─ Exemplo: ranking:variacao:GASOLINA_COMUM
```
**Uso**: Top N postos com maior oscilação de preço.

---

### **6. HASH — Estatísticas Globais de Preço**
```
stats:preco_medio
├─ {combustivel}:min (string: "3.50")
├─ {combustivel}:avg (string: "5.25")
├─ {combustivel}:max (string: "7.80")
└─ Exemplos:
   ├─ GASOLINA_COMUM:min → "4.50"
   ├─ GASOLINA_COMUM:avg → "5.62"
   └─ GASOLINA_COMUM:max → "8.90"
```
**Uso**: Estatísticas globais por combustível.

---

### **7. TS (TimeSeries) — Preço Médio Diário**
```
ts:preco_avg:{combustivel}:{uf}
├─ timestamp: ts_ms (milissegundos)
├─ value: preco_avg (float)
└─ Exemplos:
   ├─ ts:preco_avg:GASOLINA_COMUM:SP
   ├─ ts:preco_avg:ETANOL:RJ
   └─ ts:preco_avg:DIESEL_S10:MG
```
**Uso**: Série histórica de preços para gráficos de evolução.

---

## 📦 Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) e Docker Compose
- Python 3.10 ou superior
- `pip` para instalar dependências
- Conexão com a internet (para download de imagens Docker)

---

## 🚀 Execução

### **Passo 1: Subir os containers (MongoDB, Redis, Streamlit)**

```bash
docker compose up -d
```

Isto irá:
- Iniciar MongoDB com ReplicaSet (`rs0`)
- Iniciar Redis Stack Server (com RedisStack UI em `http://localhost:8001`)
- Iniciar container de aplicação Python (idle, para rodar scripts)
- Iniciar Streamlit em `http://localhost:8501`

Verificar status:
```bash
docker compose ps
```

---

### **Passo 2: Popular o banco MongoDB (Seed)**

```bash
docker compose exec app python init/seed_radar_combustivel.py
```

O script:
1. Remove e recria as coleções a cada execução (seed reproduzível)
2. Gera **100.000 documentos por coleção** (configurável via `N`)
3. Cria índices (geoespacial, text, compound)
4. Leva alguns minutos dependendo do hardware

**Resultado esperado:**
```
Conectando ao MongoDB...
Limpando coleções...
[postos] 100000 documentos inseridos
[eventos_preco] 100000 documentos inseridos
[buscas_usuarios] 100000 documentos inseridos
[avaliacoes_interacoes] 100000 documentos inseridos
[localizacoes_postos] 100000 documentos inseridos
Índices criados com sucesso
```

---

### **Passo 3: Rodar o Pipeline (MongoDB → Redis)**

```bash
docker compose exec app python pipeline/pipeline_radar.py
```

O pipeline:
1. **Fase batch**: Lê agregações do MongoDB e popula estruturas Redis
2. **Fase stream**: Monitora `Change Stream` em `eventos_preco` para atualizações em tempo real

**Resultado esperado:**
```
==================================================
BATCH INICIO — MongoDB → Redis
==================================================
[batch_postos] 100000 postos → HASH posto:{id} + GEO geo:postos
[batch_rankings_preco] 600000 entradas → ZSET ranking:preco:*
[batch_rankings_buscas] 2500 cidades → ZSET ranking:buscas
[batch_rankings_variacao] 600000 entradas → ZSET ranking:variacao:*
[batch_stats_globais] stats:preco_medio atualizado (6 combustiveis)
[batch_timeseries] 18000 pontos → TS ts:preco_avg:*
BATCH CONCLUIDO em 45.2s
```

Deixar rodando (o Change Stream continua monitorando atualizações).

---

### **Passo 4: Acessar o Dashboard Streamlit**

Em outro terminal (enquanto pipeline roda):

```bash
# Abrir no navegador:
http://localhost:8501
```

Ou, via Docker:
```bash
docker compose logs -f streamlit
```

---

### **Passo 5: Executar Consultas Redis Manualmente**

Conectar ao Redis CLI:
```bash
docker compose exec redis redis-cli
```

Ver exemplos de consultas [aqui](#consultas-redis-de-demonstração).

---

## 📁 Estrutura do Repositório

```
radar-combustivel-fake-data-generator/
│
├── docker-compose.yml              # Orquestração de containers
├── requirements.txt                # Dependências Python
├── .env.local                      # Variáveis de ambiente (local)
├── README.md                       # Este arquivo
│
├── init/
│   ├── seed_radar_combustivel.py   # Gerador de dados (Faker + MongoDB)
│   └── redis_indexes.py            # Índices Redis (aux)
│
├── pipeline/
│   ├── pipeline_radar.py           # Pipeline MongoDB → Redis (batch + stream)
│   └── event_transformer.py        # Transformações de eventos (aux)
│
├── queries/
│   ├── dashboard_radar.py          # Dashboard Streamlit (UI)
│   └── redis_reader.py             # Helpers de leitura Redis (aux)
│
└── docs/
    ├── streaming-mongo-redis.md    # Documentação técnica
    └── trabalho_radar_combustivel.html  # Documento do trabalho
```

### **Responsabilidades dos arquivos principais:**

| Arquivo | Responsabilidade |
|---------|------------------|
| `seed_radar_combustivel.py` | Gera dados fake com Faker, popula MongoDB com lotes, cria índices |
| `pipeline_radar.py` | Lê MongoDB, calcula agregações, popula Redis, monitora Change Stream |
| `dashboard_radar.py` | UI Streamlit com 6 painéis interativos, consultas Redis em tempo real |
| `docker-compose.yml` | Orquestra MongoDB, Redis, containers de app e Streamlit |

---

## ⚙️ Variáveis de Ambiente

### **Arquivo `.env.local` (criar na raiz do repositório)**

```bash
# MongoDB Configuration
MONGO_URI=mongodb://localhost:27017/?directConnection=true
MONGO_DB=radar_combustivel

# Seed Configuration
SEED=42                    # Semente para reproduzibilidade
BATCH_SIZE=5000            # Tamanho do lote em insert_many
N=100000                   # Quantidade de docs por coleção

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=            # Vazio por padrão
REDIS_DB=0
```

### **Variáveis por script:**

#### **seed_radar_combustivel.py**
| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `MONGO_URI` | `mongodb://localhost:27017/?directConnection=true` | URI MongoDB |
| `MONGO_DB` | `radar_combustivel` | Nome do database |
| `SEED` | `42` | Semente Random (reproduzível) |
| `BATCH_SIZE` | `5000` | Lote de insert_many |
| `N` | `100000` | Docs **por coleção** |

#### **pipeline_radar.py**
| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `MONGO_URI` | (conforme seed) | URI MongoDB |
| `MONGO_DB` | `radar_combustivel` | Database |
| `REDIS_HOST` | `redis` (container) ou `localhost` | Host Redis |
| `REDIS_PORT` | `6379` | Porta Redis |

#### **dashboard_radar.py**
| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `REDIS_HOST` | `localhost` | Host Redis |
| `REDIS_PORT` | `6379` | Porta Redis |

### **Exemplos:**

**Seed com menos dados (testes rápidos):**
```bash
set N=1000
python init/seed_radar_combustivel.py
```

No Linux/macOS:
```bash
export N=1000
python init/seed_radar_combustivel.py
```

**Pipeline com MongoDB remoto:**
```bash
set MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net/
python pipeline/pipeline_radar.py
```

---

## 📊 Painéis do Dashboard

O dashboard Streamlit (`queries/dashboard_radar.py`) oferece **6 painéis interativos**:

### **1. 📊 Resumo Global**
- **O que mostra**: Preço médio, mínimo e máximo **por combustível**
- **Gráficos**: Gráfico de barras com faixa min/max e tabela resumida
- **Dados consultados**:
  - `stats:preco_medio` (HASH)
  - Todos os combustíveis (GASOLINA_COMUM, ETANOL, DIESEL_S10, etc)

---

### **2. ⛽ Rankings de Preço**
- **O que mostra**: Top N menores preços de um combustível em um estado específico
- **Filtros**: Seletores para combustível, estado (UF) e número de postos (5-50)
- **Dados consultados**:
  - `ranking:preco:{combustivel}:{uf}` (ZSET)
  - `posto:{posto_id}` (HASH) para bandeira e endereço
- **Gráfico**: Gráfico de barras horizontal com informações do posto

---

### **3. 🔍 Volume de Buscas**
- **O que mostra**: Top N cidades com maior volume de buscas (demanda)
- **Filtro**: Slider para quantidade de cidades (5-30)
- **Dados consultados**:
  - `ranking:buscas` (ZSET, score=contagem)
- **Gráfico**: Gráfico de barras + mapa com cidades mais buscadas

---

### **4. 📈 Variação de Preço**
- **O que mostra**: Top N postos com maior oscilação de preço (volatilidade)
- **Filtros**: Combustível e número de postos (5-30)
- **Dados consultados**:
  - `ranking:variacao:{combustivel}` (ZSET, score=|var%|)
  - `posto:{posto_id}` (HASH)
- **Gráfico**: Gráfico de barras horizontal com % de variação

---

### **5. 🕐 Série Temporal**
- **O que mostra**: Evolução do preço médio **ao longo do tempo** para um combustível/estado
- **Filtros**: Combustível e estado (UF)
- **Dados consultados**:
  - `ts:preco_avg:{combustivel}:{uf}` (TimeSeries)
- **Gráficos**:
  - Linha temporal com marcadores
  - Métricas: pontos na série, preço mais recente, variação total
  - Tabela com últimos 30 registros

---

### **6. 🗺️ Busca Geográfica**
- **O que mostra**: Postos dentro de um raio (km) de uma cidade de referência
- **Filtros**: Cidade de referência (São Paulo, Rio, Belo Horizonte, etc) e raio em km
- **Dados consultados**:
  - `geo:postos` (GEO index, GEORADIUS)
  - `posto:{posto_id}` (HASH)
- **Gráficos**:
  - Mapa com marcadores dos postos
  - Tabela com distância, bandeira, e informações

---

## 🔍 Consultas Redis de Demonstração

Conecte ao Redis CLI:
```bash
docker compose exec redis redis-cli
```

### **1. Ver todos os postos cadastrados (primeiros 5)**
```redis
SCAN 0 MATCH "posto:*" COUNT 5
HGETALL posto:<first_id>
```

Exemplo de output:
```
1) "nome"
2) "Posto Shell - Nova Iguaçu"
3) "bandeira"
4) "Shell"
5) "cidade"
6) "Rio de Janeiro"
7) "uf"
8) "RJ"
```

---

### **2. Ranking de menores preços — Gasolina Comum em SP**
```redis
ZRANGE ranking:preco:GASOLINA_COMUM:SP 0 10 WITHSCORES
```

Exemplo de output:
```
1) "63e7f1a2b8c9d0e1f2g3h4i5"
2) "4.529"
3) "63e7f1a2b8c9d0e1f2g3h4i6"
4) "4.531"
5) "63e7f1a2b8c9d0e1f2g3h4i7"
6) "4.533"
```

---

### **3. Ranking reverso — maiores preços de Etanol no RJ**
```redis
ZREVRANGE ranking:preco:ETANOL:RJ 0 5 WITHSCORES
```

---

### **4. Cidades mais buscadas**
```redis
ZREVRANGE ranking:buscas 0 10 WITHSCORES
```

Exemplo de output:
```
1) "São Paulo|SP"
2) "2500"
3) "Rio de Janeiro|RJ"
4) "1850"
5) "Belo Horizonte|MG"
6) "1200"
```

---

### **5. Postos com maior variação de preço — Gasolina**
```redis
ZREVRANGE ranking:variacao:GASOLINA_COMUM 0 10 WITHSCORES
```

Score = |variacao_pct|

---

### **6. Estatísticas globais de preço**
```redis
HGETALL stats:preco_medio
```

Exemplo de output:
```
 1) "GASOLINA_COMUM:min"
 2) "4.50"
 3) "GASOLINA_COMUM:avg"
 4) "5.62"
 5) "GASOLINA_COMUM:max"
 6) "8.90"
 7) "ETANOL:min"
 8) "3.10"
 9) "ETANOL:avg"
10) "4.75"
11) "ETANOL:max"
12) "7.20"
```

---

### **7. Série temporal — Preço médio diário (últimos 5 pontos)**
```redis
ZREVRANGE ts:preco_avg:GASOLINA_COMUM:SP 0 5 WITHSCORES
```

Exemplo de output:
```
1) "{\"ts_ms\":1715682000000,\"preco_avg\":5.623}"
2) "1715682000000"
3) "{\"ts_ms\":1715595600000,\"preco_avg\":5.618}"
4) "1715595600000"
```

Ou, acessando diretamente:
```redis
ZRANGE ts:preco_avg:GASOLINA_COMUM:SP 0 -1
```

---

### **8. Postos por proximidade — 10 km de São Paulo (lat, lon)**
```redis
GEORADIUS geo:postos -46.6333 -23.5505 10 km WITHCOORD WITHDIST WITHHASH LIMIT 10
```

Exemplo de output:
```
1) 1) "63e7f1a2b8c9d0e1f2g3h4i5"
   2) "2.4567"  (distância em km)
   3) (integer) 4069589017278...
   4) 1) "-46.6230"
      2) "-23.5410"
```

---

### **9. Quantos postos têm cada bandeira em SP**
```redis
# (Exemplo: itera entre os postos e conta)
SCAN 0 MATCH "posto:*"
HGETALL posto:<id>  (repetir para cada)
```

Ou usar agregação no MongoDB:
```bash
docker compose exec app python -c "
from pymongo import MongoClient
client = MongoClient('mongodb://mongo:27017/?directConnection=true')
db = client['radar_combustivel']
pipeline = [
    {'\$match': {'endereco.estado': 'SP'}},
    {'\$group': {'_id': '\$bandeira', 'count': {'\$sum': 1}}},
    {'\$sort': {'count': -1}}
]
for doc in db.postos.aggregate(pipeline):
    print(doc)
"
```

---

### **10. Contagem de estruturas Redis**
```redis
DBSIZE                    # Total de chaves
SCAN 0 MATCH "posto:*" COUNT 1000 | wc -l  (aprox)
```

Ver tipos de chaves:
```bash
redis-cli --scan --pattern "*" | cut -d: -f1 | sort | uniq -c
```

---

### **11. Manutenção — Limpar tudo**
```redis
FLUSHDB                   # Limpar database atual
FLUSHALL                  # Limpar TODOS os databases
```

---

## 🐳 Docker — Comandos Úteis

| Comando | Descrição |
|---------|-----------|
| `docker compose up -d` | Subir todos os containers em background |
| `docker compose down` | Derrubar todos os containers |
| `docker compose down -v` | Derrubar e remover volumes (⚠️ dados perdidos) |
| `docker compose ps` | Listar status dos containers |
| `docker compose logs <service>` | Ver logs (ex: `mongo`, `redis`, `streamlit`) |
| `docker compose logs -f streamlit` | Seguir logs em tempo real |
| `docker compose exec app bash` | Entrar em shell do container `app` |
| `docker compose exec redis redis-cli` | Abrir CLI do Redis |

---

## 📝 Observações Importantes

1. **Primeira execução**: A carga com `N=100000` em **cinco** coleções gera ~500k documentos e pode levar **5-15 minutos** dependendo do hardware.

2. **Reduzir volume para testes**:
   ```bash
   set N=1000    # Windows
   export N=1000 # Linux/macOS
   python init/seed_radar_combustivel.py
   ```

3. **Seed reproduzível**: Sempre usa `SEED=42` por padrão. Mudar apenas se quiser dados diferentes.

4. **Pipeline contínuo**: O script `pipeline_radar.py` roda indefinidamente monitorando mudanças via Change Stream. Para parar: `Ctrl+C`.

5. **Redis Stack UI**: Acesse `http://localhost:8001` para explorar dados Redis visualmente.

6. **MongoDB Compass**: Conecte a `mongodb://localhost:27017` para explorar MongoDB visualmente.

---

## 📚 Referência do Trabalho

O enunciado completo do trabalho final está em `docs/trabalho_radar_combustivel.html`.

Para mais detalhes técnicos, consulte `docs/streaming-mongo-redis.md`.
