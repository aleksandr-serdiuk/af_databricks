-- Databricks notebook source
SELECT *
FROM gold.dm_trading_daily
LIMIT 100

-- COMMAND ----------

SELECT 
  date_format(trade_date, 'yyyy-MM') year_month,
  instrument,
  SUM(profit_usd) profit_usd,
  SUM(deal_count) deal_count,
  SUM(realized_pl_usd) realized_pl_usd,
  RANK() OVER(PARTITION BY date_format(trade_date, 'yyyy-MM') ORDER BY SUM(realized_pl_usd) DESC) month_rank
FROM gold.dm_trading_daily
GROUP BY 
  date_format(trade_date, 'yyyy-MM'),
  instrument
ORDER BY 
  year_month,
  realized_pl_usd DESC
