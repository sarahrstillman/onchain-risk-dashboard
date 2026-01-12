SELECT
  wallet_address,
  COUNT(*) AS tx_count_30d,
  SUM(value_eth) AS volume_30d,
  COUNT(DISTINCT CASE WHEN direction = 'out' THEN to_address ELSE from_address END)
      AS unique_counterparties_30d,
  AVG(value_eth) AS avg_tx_size
FROM transactions
WHERE timestamp >= datetime('now', '-30 days')
GROUP BY wallet_address;

-- Daily net flows by entity (requires entities table)
SELECT
  date(t.timestamp) AS metric_date,
  e.entity_type,
  e.label AS entity_label,
  SUM(CASE WHEN LOWER(t.to_address) = e.address THEN t.value_eth ELSE 0 END) AS inflow,
  SUM(CASE WHEN LOWER(t.from_address) = e.address THEN t.value_eth ELSE 0 END) AS outflow,
  SUM(CASE WHEN LOWER(t.to_address) = e.address THEN t.value_eth ELSE 0 END)
    - SUM(CASE WHEN LOWER(t.from_address) = e.address THEN t.value_eth ELSE 0 END)
    AS net_flow
FROM transactions t
JOIN entities e
  ON LOWER(t.to_address) = e.address OR LOWER(t.from_address) = e.address
GROUP BY metric_date, e.entity_type, e.label;

-- Large transfer counts by day
SELECT
  date(timestamp) AS metric_date,
  COUNT(*) AS large_tx_count,
  SUM(value_eth) AS large_tx_volume
FROM transactions
WHERE value_eth >= 1000
GROUP BY metric_date;
