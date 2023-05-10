-- Databricks notebook source
SELECT 
  country,
  SUM(profit_usd) profit_usd,
  SUM(deal_count) deal_count,
  SUM(realized_pl_usd) realized_pl_usd,
  RANK() OVER(ORDER BY SUM(deal_count) DESC) rank
FROM gold.dm_trading_daily
GROUP BY 
  country
ORDER BY 
  rank
