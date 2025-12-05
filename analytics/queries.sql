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
