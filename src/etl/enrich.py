import os
from typing import Dict, Optional

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

def add_contract_flags(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    if not w3:
        df["is_contract_interaction"] = None
        return df

    cache: Dict[str, Optional[bool]] = {}

    def _is_contract_cached(address: Optional[str]) -> Optional[bool]:
        if not address:
            return None
        key = address.lower()
        if key in cache:
            return cache[key]
        try:
            cache[key] = is_contract(address)
        except ValueError:
            cache[key] = None
        return cache[key]

    df["is_contract_interaction"] = df["to_address"].map(_is_contract_cached)
    return df
