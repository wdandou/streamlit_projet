"""
s3_storage.py — Lecture et écriture des données sur AWS S3.
Utilisé par le pipeline ETL pour persister les données
et par Streamlit pour les lire au démarrage.
"""
import io
import os
import json
from datetime import datetime, timezone

import boto3
import pandas as pd

S3_BUCKET = os.environ.get("S3_BUCKET", "paris-bornes-etl")
AWS_REGION = os.environ.get("AWS_REGION", "eu-west-3").strip()


def _client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


# ---------------------------------------------------------------------------
# Écriture
# ---------------------------------------------------------------------------

def upload_df(df: pd.DataFrame, key: str) -> bool:
    """Upload un DataFrame en CSV sur S3."""
    try:
        buf = io.BytesIO()
        df.to_csv(buf, index=False, encoding="utf-8")
        buf.seek(0)
        _client().put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
        print(f"S3 ✅ upload : s3://{S3_BUCKET}/{key} ({len(df)} lignes)")
        return True
    except Exception as e:
        print(f"S3 ❌ upload échoué ({key}) : {e}")
        return False


def upload_meta(meta: dict, key: str = "processed/meta.json") -> bool:
    """Upload un fichier JSON de métadonnées sur S3."""
    try:
        body = json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8")
        _client().put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")
        print(f"S3 ✅ meta uploadée : s3://{S3_BUCKET}/{key}")
        return True
    except Exception as e:
        print(f"S3 ❌ meta upload échoué : {e}")
        return False


# ---------------------------------------------------------------------------
# Lecture
# ---------------------------------------------------------------------------

def download_df(key: str) -> pd.DataFrame | None:
    """Télécharge un CSV depuis S3 et retourne un DataFrame."""
    try:
        obj = _client().get_object(Bucket=S3_BUCKET, Key=key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()), low_memory=False)
        print(f"S3 ✅ lecture : s3://{S3_BUCKET}/{key} ({len(df)} lignes)")
        return df
    except Exception as e:
        print(f"S3 ❌ lecture échouée ({key}) : {e}")
        return None


def download_meta(key: str = "processed/meta.json") -> dict | None:
    """Télécharge le fichier de métadonnées depuis S3."""
    try:
        obj = _client().get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return None


def fichier_existe(key: str) -> bool:
    """Vérifie si un fichier existe sur S3."""
    try:
        _client().head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Helpers haut niveau
# ---------------------------------------------------------------------------

KEYS = {
    "bornes":      "processed/bornes.csv",
    "pression":    "processed/pression.csv",
    "energie":     "processed/energie.csv",
    "population":  "processed/population.csv",
    "projections_arrdt": "processed/projections_arrdt.csv",
    "projections_paris": "processed/projections_paris.csv",
}


def donnees_disponibles() -> bool:
    """Retourne True si les fichiers principaux existent sur S3."""
    return fichier_existe(KEYS["bornes"]) and fichier_existe(KEYS["pression"])


def charger_toutes_donnees() -> dict:
    """Charge tous les DataFrames depuis S3 en une seule fois."""
    data = {}
    for nom, key in KEYS.items():
        df = download_df(key)
        data[nom] = df
    data["meta"] = download_meta()
    return data


def sauvegarder_toutes_donnees(
    bornes, pression, energie, population,
    projections_arrdt, projections_paris,
    statut: str = "OK"
) -> None:
    """Upload tous les DataFrames transformés vers S3/processed/."""
    upload_df(bornes,            KEYS["bornes"])
    upload_df(pression,          KEYS["pression"])
    upload_df(energie,           KEYS["energie"])
    upload_df(population,        KEYS["population"])
    upload_df(projections_arrdt, KEYS["projections_arrdt"])
    upload_df(projections_paris, KEYS["projections_paris"])

    meta = {
        "last_run": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "nb_bornes": int(len(bornes)) if bornes is not None else 0,
        "nb_pdc": int(len(bornes)) if bornes is not None else 0,
        "status": statut,
    }
    upload_meta(meta)
    print("S3 ✅ toutes les données sauvegardées.")