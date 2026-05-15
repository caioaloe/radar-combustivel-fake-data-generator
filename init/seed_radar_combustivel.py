"""
Seed MongoDB — Plataforma Radar Combustível
==========================================
Coleções (conforme escopo do trabalho final):
  - postos: cadastro de postos com endereço e geo (GeoJSON Point).
  - eventos_preco: eventos de atualização de preço por posto/combustível.
  - buscas_usuarios: buscas e filtros utilizados pelos usuários.
  - avaliacoes_interacoes: avaliações, favoritos e outras interações.
  - localizacoes_postos: documento de localização indexável (geo + IBGE).

Uso:
  1) docker compose up -d
  2) pip install -r requirements.txt
  3) python seed_radar_combustivel.py

Variáveis de ambiente (opcional):
  MONGO_URI   (default: mongodb://localhost:27017/?directConnection=true)
  MONGO_DB     (default: radar_combustivel)
  SEED        (default: 42)
  BATCH_SIZE  (default: 5000)
  N           (default: 100000) — registros por coleção
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from bson import ObjectId
from faker import Faker
from dotenv import load_dotenv
from pymongo import ASCENDING, GEOSPHERE, MongoClient
from pymongo.collection import Collection
from pymongo.errors import OperationFailure, PyMongoError

load_dotenv()

# ---------------------------------------------------------------------------
# Constantes de domínio — Radar Combustível (Brasil)
# ---------------------------------------------------------------------------

COMBUSTIVEIS = (
    "GASOLINA_COMUM",
    "GASOLINA_ADITIVADA",
    "ETANOL",
    "DIESEL_S10",
    "DIESEL_COMUM",
    "GNV",
)

BANDEIRAS = (
    "Ipiranga",
    "Shell",
    "BR",
    "Raízen",
    "Ale",
    "Boxter",
    "Petrobras",
    "Rede independente",
)

UFS = (
    "SP",
    "RJ",
    "MG",
    "PR",
    "RS",
    "BA",
    "PE",
    "CE",
    "DF",
    "GO",
)

TIPOS_INTERACAO = ("avaliacao", "favorito", "compartilhamento", "denuncia", "check_in")

# Approx. bounding box Brasil (lng, lat) para pontos plausíveis
BR_LNG_RANGE = (-73.5, -34.8)
BR_LAT_RANGE = (-33.8, 5.3)

# Configuração de conexão MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
LOCALHOST_DIRECT_URI = "mongodb://localhost:27017/?directConnection=true"

# Random com seed controlado
SEED = int(os.environ.get("SEED", "42"))
RANDOM = random.Random(SEED)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def with_direct_connection(uri: str) -> str:
    """Adiciona directConnection=true à URI."""
    parts = urlsplit(uri)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["directConnection"] = "true"
    new_query = urlencode(query)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def without_replicaset(uri: str) -> str:
    """Remove replicaSet da URI."""
    parts = urlsplit(uri)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.pop("replicaSet", None)
    new_query = urlencode(query)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def candidate_uris() -> List[str]:
    """Gera lista de URIs candidatas (container DNS, localhost, fallbacks)."""
    candidates: List[str] = []
    primary = with_direct_connection(MONGO_URI)
    candidates.append(primary)

    # Running on host OS: container DNS name "mongo" is usually unresolved.
    if "mongo:27017" in primary:
        host_safe = primary.replace("mongo:27017", "localhost:27017")
        candidates.append(without_replicaset(host_safe))

    candidates.append(without_replicaset(LOCALHOST_DIRECT_URI))

    unique: List[str] = []
    for uri in candidates:
        if uri not in unique:
            unique.append(uri)
    return unique


def get_client_with_fallback() -> MongoClient:
    """Tenta conectar com URIs candidatas e fallbacks."""
    last_exc: Exception | None = None
    for uri in candidate_uris():
        client = MongoClient(uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
        try:
            client.admin.command("ping")
            print(f"[CONECTADO] MongoDB em: {uri}", flush=True)
            return client
        except PyMongoError as exc:
            client.close()
            last_exc = exc
    tried = " | ".join(candidate_uris())
    raise RuntimeError(f"Não foi possível conectar ao MongoDB. URIs testadas: {tried}") from last_exc


def ensure_replicaset(client: MongoClient) -> None:
    """Garante que ReplicaSet está inicializado."""
    admin = client.admin
    try:
        admin.command("replSetGetStatus")
        print("[REPLICASET] Já inicializado", flush=True)
    except OperationFailure:
        try:
            admin.command("replSetInitiate", {"_id": "rs0", "members": [{"_id": 0, "host": "localhost:27017"}]})
            time.sleep(2)
            print("[REPLICASET] Inicializado com sucesso", flush=True)
        except OperationFailure:
            # Replica set may already be initiating/running
            print("[REPLICASET] Já em processo de inicialização", flush=True)
            pass


def chunked(seq: Sequence[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(seq), size):
        yield list(seq[i : i + size])


def make_fake_geo(fake: Faker) -> dict[str, Any]:
    lng = RANDOM.uniform(*BR_LNG_RANGE)
    lat = RANDOM.uniform(*BR_LAT_RANGE)
    return {"type": "Point", "coordinates": [round(lng, 6), round(lat, 6)]}


def cnpj_like(fake: Faker) -> str:
    digits = "".join(str(RANDOM.randint(0, 9)) for _ in range(14))
    return f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:14]}"


# ---------------------------------------------------------------------------
# Estruturas de documento (MongoDB) — uma função por entidade
# ---------------------------------------------------------------------------


def doc_posto(fake: Faker, oid: ObjectId) -> dict[str, Any]:
    cidade = fake.city()
    estado = RANDOM.choice(UFS)
    geo = make_fake_geo(fake)
    return {
        "_id": oid,
        "cnpj": cnpj_like(fake),
        "nome_fantasia": f"Posto {fake.company()}",
        "bandeira": RANDOM.choice(BANDEIRAS),
        "endereco": {
            "logradouro": fake.street_name(),
            "numero": str(RANDOM.randint(1, 9999)),
            "bairro": fake.bairro() if hasattr(fake, "bairro") else fake.city_suffix(),
            "cep": fake.postcode(),
            "cidade": cidade,
            "estado": estado,
        },
        "telefone": fake.phone_number()[:20],
        "ativo": RANDOM.random() > 0.03,
        "location": geo,
        "created_at": fake.date_time_between(start_date="-5y", end_date="now", tzinfo=timezone.utc),
        "updated_at": utc_now(),
    }


def doc_evento_preco(
    fake: Faker,
    posto_ids: Sequence[ObjectId],
) -> dict[str, Any]:
    posto_id = RANDOM.choice(posto_ids)
    comb = RANDOM.choice(COMBUSTIVEIS)
    preco_novo = round(RANDOM.uniform(4.5, 8.9), 3)
    preco_ant = round(max(3.0, preco_novo + RANDOM.uniform(-0.8, 0.8)), 3)
    ocorrido = fake.date_time_between(start_date="-90d", end_date="now", tzinfo=timezone.utc)
    return {
        "_id": ObjectId(),
        "posto_id": posto_id,
        "combustivel": comb,
        "preco_anterior": preco_ant,
        "preco_novo": preco_novo,
        "variacao_pct": round((preco_novo - preco_ant) / preco_ant * 100, 4) if preco_ant else 0.0,
        "unidade": "BRL_L",
        "fonte": RANDOM.choice(("app_usuario", "api_anp", "operador_posto", "crawler")),
        "ocorrido_em": ocorrido,
        "revisado": random.random() > 0.15,
    }


def doc_busca(fake: Faker) -> dict[str, Any]:
    return {
        "_id": ObjectId(),
        "usuario_id": fake.uuid4(),
        "session_id": fake.uuid4(),
        "tipo_combustivel": RANDOM.choice(COMBUSTIVEIS),
        "cidade": fake.city(),
        "estado": RANDOM.choice(UFS),
        "raio_km": RANDOM.choice((1, 2, 3, 5, 10, 15)),
        "filtros": {
            "apenas_abertos": RANDOM.random() > 0.5,
            "ordenacao": RANDOM.choice(("preco", "distancia", "avaliacao")),
        },
        "geo_centro": make_fake_geo(fake),
        "consultado_em": fake.date_time_between(start_date="-180d", end_date="now", tzinfo=timezone.utc),
        "resultado_count": RANDOM.randint(0, 120),
        "latencia_ms": RANDOM.randint(8, 450),
    }


def doc_avaliacao_interacao(fake: Faker, posto_ids: Sequence[ObjectId]) -> dict[str, Any]:
    tipo = RANDOM.choice(TIPOS_INTERACAO)
    nota = RANDOM.randint(1, 5) if tipo == "avaliacao" else None
    return {
        "_id": ObjectId(),
        "posto_id": RANDOM.choice(posto_ids),
        "usuario_id": fake.uuid4(),
        "tipo": tipo,
        "nota": nota,
        "comentario": fake.text(max_nb_chars=180) if tipo == "avaliacao" and RANDOM.random() > 0.4 else None,
        "created_at": fake.date_time_between(start_date="-2y", end_date="now", tzinfo=timezone.utc),
        "util_count": RANDOM.randint(0, 42) if tipo == "avaliacao" else 0,
    }


def doc_localizacao_posto(
    fake: Faker,
    posto_id: ObjectId,
) -> dict[str, Any]:
    geo = make_fake_geo(fake)
    return {
        "_id": ObjectId(),
        "posto_id": posto_id,
        "municipio": fake.city(),
        "bairro": fake.bairro() if hasattr(fake, "bairro") else f"Bairro {RANDOM.randint(1, 200)}",
        "uf": RANDOM.choice(UFS),
        "codigo_ibge": str(RANDOM.randint(1100000, 5300000)),
        "geo": geo,
        "atualizado_em": utc_now() - timedelta(days=RANDOM.randint(0, 30)),
    }


# ---------------------------------------------------------------------------
# Índices sugeridos (consultas e pipeline)
# ---------------------------------------------------------------------------


def ensure_indexes(db) -> None:
    db.postos.create_index([("location", GEOSPHERE)])
    db.postos.create_index([("endereco.estado", ASCENDING), ("endereco.cidade", ASCENDING)])
    db.eventos_preco.create_index([("posto_id", ASCENDING), ("ocorrido_em", ASCENDING)])
    db.eventos_preco.create_index([("combustivel", ASCENDING), ("ocorrido_em", ASCENDING)])
    db.buscas_usuarios.create_index([("consultado_em", ASCENDING)])
    db.buscas_usuarios.create_index([("estado", ASCENDING), ("cidade", ASCENDING)])
    db.avaliacoes_interacoes.create_index([("posto_id", ASCENDING), ("created_at", ASCENDING)])
    db.localizacoes_postos.create_index([("posto_id", ASCENDING)], unique=True)
    db.localizacoes_postos.create_index([("geo", GEOSPHERE)])


def insert_batches(col: Collection, docs: List[dict[str, Any]], batch_size: int) -> int:
    n = 0
    for batch in chunked(docs, batch_size):
        col.insert_many(batch, ordered=False)
        n += len(batch)
        print(f"  {col.name}: {n} inseridos...", flush=True)
    return n


def seed_initial(fake: Faker, db, n_target: int, batch_size: int) -> None:
    """Seed completo: gera postos e todas as coleções."""
    # Limpa coleções para reexecução idempotente do seed
    for name in (
        "postos",
        "eventos_preco",
        "buscas_usuarios",
        "avaliacoes_interacoes",
        "localizacoes_postos",
    ):
        db[name].drop()

    print("Gerando IDs de postos e documentos...")
    posto_ids = [ObjectId() for _ in range(n_target)]

    postos = [doc_posto(fake, oid) for oid in posto_ids]
    localizacoes = [doc_localizacao_posto(fake, pid) for pid in posto_ids]

    print("Inserindo postos...")
    insert_batches(db.postos, postos, batch_size)
    postos.clear()

    print("Inserindo localizacoes_postos (1:1 com postos)...")
    insert_batches(db.localizacoes_postos, localizacoes, batch_size)
    localizacoes.clear()

    print("Gerando e inserindo eventos_preco...")
    eventos = [doc_evento_preco(fake, posto_ids) for _ in range(n_target)]
    insert_batches(db.eventos_preco, eventos, batch_size)
    eventos.clear()

    print("Gerando e inserindo buscas_usuarios...")
    buscas = [doc_busca(fake) for _ in range(n_target)]
    insert_batches(db.buscas_usuarios, buscas, batch_size)
    buscas.clear()

    print("Gerando e inserindo avaliacoes_interacoes...")
    avaliacoes = [doc_avaliacao_interacao(fake, posto_ids) for _ in range(n_target)]
    insert_batches(db.avaliacoes_interacoes, avaliacoes, batch_size)
    avaliacoes.clear()

    print("Criando índices...")
    ensure_indexes(db)

    total = sum(db[c].estimated_document_count() for c in (
        "postos",
        "eventos_preco",
        "buscas_usuarios",
        "avaliacoes_interacoes",
        "localizacoes_postos",
    ))
    print(f"[SEED] Concluído. Total aproximado de documentos: {total}", flush=True)


def stress_insert(fake: Faker, db, events_count: int, batch_size: int) -> None:
    """Modo stress: insere apenas eventos incrementais sem limpar banco."""
    # Obtém IDs de postos existentes
    distinct_ids = db.postos.distinct("_id")
    if not distinct_ids:
        print("[STRESS] Nenhum posto encontrado. Executando seed inicial primeiro...")
        seed_initial(fake, db, 10000, batch_size)
        distinct_ids = db.postos.distinct("_id")

    print(f"[STRESS] Inserindo {events_count} novos eventos...")
    
    # Insere apenas eventos incrementais
    eventos = [doc_evento_preco(fake, distinct_ids) for _ in range(events_count)]
    insert_batches(db.eventos_preco, eventos, batch_size)
    
    buscas = [doc_busca(fake) for _ in range(events_count)]
    insert_batches(db.buscas_usuarios, buscas, batch_size)
    
    avaliacoes = [doc_avaliacao_interacao(fake, distinct_ids) for _ in range(events_count)]
    insert_batches(db.avaliacoes_interacoes, avaliacoes, batch_size)
    
    print(f"[STRESS] Inseridos {events_count * 3} eventos no total.", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Popula MongoDB com dados fake para Radar Combustível.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python seed_radar_combustivel.py                    # Seed completo (100k docs por coleção)
  python seed_radar_combustivel.py --stress --events 5000    # Modo stress: insere 5k eventos incrementais
  python seed_radar_combustivel.py --n 50000                 # Seed com 50k docs por coleção
        """
    )
    parser.add_argument(
        "--stress",
        action="store_true",
        help="Modo stress: insere apenas eventos incrementais sem limpar banco."
    )
    parser.add_argument(
        "--events",
        type=int,
        default=1000,
        help="Quantidade de eventos para modo stress (default: 1000)."
    )
    parser.add_argument(
        "--n",
        type=int,
        help="Documentos por coleção (sobrescreve env var N)."
    )
    args = parser.parse_args()

    # Lê configurações: CLI args > env vars > defaults
    MONGO_DB = os.environ.get("MONGO_DB", "radar_combustivel")
    batch_size = int(os.environ.get("BATCH_SIZE", "5000"))
    n_target = args.n if args.n else int(os.environ.get("N", "100000"))

    # Inicializa random com seed
    Faker.seed(SEED)
    fake = Faker("pt_BR")

    print(f"[CONFIG] DB: {MONGO_DB} | SEED: {SEED} | Batch: {batch_size}")
    print(f"[CONFIG] Modo: {'STRESS' if args.stress else 'SEED COMPLETO'} | Documentos: {n_target}")

    try:
        client = get_client_with_fallback()
        ensure_replicaset(client)
    except RuntimeError as e:
        print(f"[ERRO] {e}", file=sys.stderr)
        return 1

    db = client[MONGO_DB]

    try:
        if args.stress:
            stress_insert(fake, db, args.events, batch_size)
        else:
            seed_initial(fake, db, n_target, batch_size)
    except PyMongoError as e:
        print(f"[ERRO] MongoDB: {e}", file=sys.stderr)
        return 1
    finally:
        client.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
