# Onchain Risk Dashboard

## Setup
- Create `src/config/.env` with `ETHERSCAN_API_KEY`, `DB_URL`, and optional `ALCHEMY_URL`.
- Initialize the database schema:
  - `sqlite3 path/to.db < src/etl/schema.sql`
- (Optional) Add entity labels in `data/entities.csv` with headers `address,label,entity_type`.

## Run
- `python main.py 0xYourWalletAddressHere --top 10`
- Use `--entities data/entities.csv` to point at a custom entity list.
