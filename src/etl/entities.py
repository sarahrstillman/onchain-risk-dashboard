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
    if df.empty:
        return 0

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
    if not records:
        return 0

    with engine.begin() as conn:
        conn.exec_driver_sql(
            "INSERT OR IGNORE INTO entities (address, label, entity_type) VALUES (?, ?, ?)",
            records,
        )

    return len(records)
