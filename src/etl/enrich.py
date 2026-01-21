import os
from functools import lru_cache
from typing import Optional

import pandas as pd
from web3 import Web3
from dotenv import load_dotenv

load_dotenv()
ALCHEMY_URL = os.getenv("ALCHEMY_URL")

w3 = Web3(Web3.HTTPProvider(ALCHEMY_URL)) if ALCHEMY_URL else None

def is_contract(address):
    if not w3:
        raise RuntimeError("ALCHEMY_URL is not set in your environment.")
    code = w3.eth.get_code(Web3.to_checksum_address(address))
    return code != b""

@lru_cache(maxsize=10000)
def _is_contract_cached(address_lower: str):
    if not w3:
        return None
    if not address_lower:
        return None
    try:
        return is_contract(address_lower)
    except ValueError:
        return None

def add_contract_flags(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    if not w3:
        df["is_contract_interaction"] = None
        return df

    def _lookup(address: Optional[str]) -> Optional[bool]:
        if not address:
            return None
        return _is_contract_cached(address.lower())

    df["is_contract_interaction"] = df["to_address"].map(_lookup)
    return df
