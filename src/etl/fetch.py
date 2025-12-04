import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ETHERSCAN_API_KEY")

def fetch_wallet_txs(address: str) -> pd.DataFrame:
    url = "https://api.etherscan.io/api"
    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": API_KEY
    }

    resp = requests.get(url, params=params)
    data = resp.json()

    if data.get("status") != "1":
        return pd.DataFrame()

    df = pd.DataFrame(data["result"])
    return df
