from web3 import Web3
import os
from dotenv import load_dotenv

load_dotenv()
ALCHEMY_URL = os.getenv("ALCHEMY_URL")

w3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))

def is_contract(address):
    code = w3.eth.get_code(Web3.to_checksum_address(address))
    return code != b""
