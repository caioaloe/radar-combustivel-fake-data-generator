import re
import os
from typing import Dict

from dotenv import load_dotenv
from pymongo import MongoClient
from redis import Redis
from redis.commands.search.field import GeoField, NumericField, TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType

# Load .env.local first (for host development), fallback to .env (for Docker)
load_dotenv(".env.local")
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

DB_NAME = os.getenv("DB_NAME", "radar_combustivel")


def numeric_posto_id(value: str) -> str:
    """Extrai ID numérico de ObjectId em string."""
    match = re.search(r"([a-f0-9]+)$", value or "")
    return match.group(1)[:8] if match else value


def load_postos_snapshot() -> Dict[str, dict]:
    """Carrega snapshot de postos do MongoDB."""
    mongo = MongoClient(MONGO_URI)
    col = mongo[DB_NAME]["postos"]
    out = {}
    for doc in col.find({"ativo": True}).limit(10000):
        pid = str(doc["_id"])
        out[pid] = {
            "posto_id": pid,
            "nome_fantasia": doc.get("nome_fantasia", ""),
            "bandeira": doc.get("bandeira", ""),
            "bairro": doc.get("endereco", {}).get("bairro", ""),
            "cidade": doc.get("endereco", {}).get("cidade", ""),
            "estado": doc.get("endereco", {}).get("estado", ""),
            "lat": doc.get("location", {}).get("coordinates", [0, 0])[1],
            "lon": doc.get("location", {}).get("coordinates", [0, 0])[0],
        }
    mongo.close()
    return out


def load_precos_snapshot() -> Dict[str, dict]:
    """Carrega snapshot dos últimos preços por combustível/posto."""
    mongo = MongoClient(MONGO_URI)
    col = mongo[DB_NAME]["eventos_preco"]
    pipeline = [
        {"$sort": {"ocorrido_em": -1}},
        {
            "$group": {
                "_id": {"posto_id": "$posto_id", "combustivel": "$combustivel"},
                "preco_novo": {"$first": "$preco_novo"},
                "ocorrido_em": {"$first": "$ocorrido_em"},
            }
        },
    ]
    out = {}
    for row in col.aggregate(pipeline):
        key = f"{row['_id']['posto_id']}:{row['_id']['combustivel']}"
        out[key] = row
    mongo.close()
    return out


def main() -> None:
    redis = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    postos_snapshot = load_postos_snapshot()
    precos_snapshot = load_precos_snapshot()

    print(f"[REDIS] Carregando {len(postos_snapshot)} postos...")
    
    # Seed hash documents: posto:{id}
    for pid, item in postos_snapshot.items():
        simple_id = numeric_posto_id(pid)
        key = f"posto:{simple_id}"
        redis.hset(
            key,
            mapping={
                "posto_id": pid,
                "nome_fantasia": item.get("nome_fantasia", ""),
                "bandeira": item.get("bandeira", ""),
                "bairro": item.get("bairro", ""),
                "cidade": item.get("cidade", ""),
                "estado": item.get("estado", ""),
                "location": f"{item.get('lon', 0)},{item.get('lat', 0)}",
                "views": 0,
                "likes": 0,
            },
        )

        # TimeSeries para visualizações e buscas: ts:posto:{id}:views|searches
        for metric in ("views", "searches"):
            ts_key = f"ts:posto:{simple_id}:{metric}"
            try:
                redis.execute_command(
                    "TS.CREATE",
                    ts_key,
                    "RETENTION",
                    2592000000,  # 30 dias
                    "LABELS",
                    "posto_id",
                    simple_id,
                    "metric",
                    metric,
                )
            except Exception:
                # Already exists
                pass

    # Seed de preços: preco:{posto_id}:{combustivel}
    print(f"[REDIS] Carregando {len(precos_snapshot)} preços...")
    for key, preco in precos_snapshot.items():
        redis.hset(
            f"preco:{key}",
            mapping={
                "preco_novo": preco.get("preco_novo", 0.0),
                "atualizado_em": str(preco.get("ocorrido_em", "")),
            },
        )

    # Recreate RediSearch index para postos (idempotente)
    try:
        redis.execute_command("FT.DROPINDEX", "idx:postos", "DD")
    except Exception:
        pass

    redis.ft("idx:postos").create_index(
        fields=[
            TextField("nome_fantasia", weight=2.0),
            TagField("bandeira"),
            TagField("bairro"),
            TagField("cidade"),
            TagField("estado"),
            NumericField("views", sortable=True),
            GeoField("location"),
        ],
        definition=IndexDefinition(prefix=["posto:"], index_type=IndexType.HASH),
    )

    print(
        f"[REDIS] idx:postos criado com {len(postos_snapshot)} postos e "
        f"{len(precos_snapshot)} preços em cache."
    )


if __name__ == "__main__":
    main()
