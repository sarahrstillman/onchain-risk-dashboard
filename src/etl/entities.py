import os
from typing import List, Tuple

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv("src/config/.env")
engine = create_engine(os.getenv("DB_URL"))


def load_entities(csv_path: str) -> int:
    if not csv_path or not os.path.exists(csv_path):
        return 0

    df = pd.read_csv(csv_path)
    required = {"address", "label", "entity_type"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"entities CSV missing columns: {sorted(missing)}")

    df = df.copy()
    df["address"] = df["address"].astype(str).str.lower()
    df = df.dropna(subset=["address"])

    records: List[Tuple[str, str, str]] = list(
        df[["address", "label", "entity_type"]].itertuples(index=False, name=None)
    )
    addresses = [record[0] for record in records]

    with engine.begin() as conn:
        conn.exec_driver_sql("DELETE FROM entities")
        if records:
            conn.exec_driver_sql(
                "INSERT INTO entities (address, label, entity_type) VALUES (?, ?, ?)",
                records,
            )
        conn.exec_driver_sql("DELETE FROM risk_metrics")
        conn.exec_driver_sql("DELETE FROM daily_metrics")
        conn.exec_driver_sql("DELETE FROM transactions")

    return len(records)


def list_entities() -> List[dict]:
    query = "SELECT address, entity_type, label FROM entities"
    df = pd.read_sql(query, engine)
    if df.empty:
        return []
    return df.dropna(subset=["address"]).to_dict(orient="records")
