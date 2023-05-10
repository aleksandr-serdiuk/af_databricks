-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####create DB bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Trade mt4

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####create table bronze.mt4_trade_record

-- COMMAND ----------

DROP TABLE IF EXISTS bronze.mt4_trade_record;
CREATE TABLE bronze.mt4_trade_record
(
	frs_RecOperation STRING,
	frs_ServerID SMALLINT,
	order INT,
	login INT,
	symbol STRING,
	digits SMALLINT,
	cmd SMALLINT,
	volume INT,
	open_time INT,
	state SMALLINT,
	open_price DECIMAL(18, 6),
	sl DECIMAL(18, 6),
	tp DECIMAL(18, 6),
	close_time INT,
	gw_volume INT,
	expiration INT,
	reason SMALLINT,
	conv_rates_open DECIMAL(18, 6),
	conv_rates_close DECIMAL(18, 6),
	commission DECIMAL(18, 4),
	commission_agent DECIMAL(18, 4),
	storage DECIMAL(18, 4),
	close_price DECIMAL(18, 6),
	profit DECIMAL(18, 4),
	taxes DECIMAL(18, 4),
	magic INT,
	comment STRING,
	gw_order INT,
	activation INT,
	gw_open_price INT,
	gw_close_price INT,
	margin_rate DECIMAL(18, 6),
	timestamp INT,
	book_type SMALLINT,
	open_spread INT,
	close_spread INT,
	parent_order INT,
	open_volume INT
)
USING parquet
OPTIONS (path "/mnt/serdiukstorage/bronze/trade/*/mt4_trade_record.parquet")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####create table bronze.mt4_con_group

-- COMMAND ----------

DROP TABLE IF EXISTS bronze.mt4_con_group;
CREATE TABLE bronze.mt4_con_group
(
  frs_Timestamp BIGINT,
  frs_RecOperation STRING,
  frs_ServerID SMALLINT,
  group STRING,
  enable SMALLINT,
  timeout INT,
  otp_mode INT,
  company STRING,
  signature STRING,
  support_page STRING,
  smtp_server STRING,
  smtp_login STRING,
  smtp_password STRING,
  support_email STRING,
  templates STRING,
  copies INT,
  reports INT,
  default_leverage INT,
  default_deposit DECIMAL(18, 6),
  maxsecurities INT,
  currency STRING,
  credit DECIMAL(18, 6),
  margin_call INT,
  margin_mode INT,
  margin_stopout INT,
  INTerestrate DECIMAL(18, 6),
  use_swap INT,
  news INT,
  rights INT,
  check_ie_prices INT,
  maxpositions INT,
  close_reopen INT,
  hedge_prohibited INT,
  close_fifo INT,
  hedge_largeleg INT,
  margin_type INT,
  archive_period INT,
  archive_max_balance INT,
  stopout_skip_hedged INT,
  archive_pending_period INT
)
USING parquet
OPTIONS (path "/mnt/serdiukstorage/bronze/dict/mt4_con_group.parquet")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####create table bronze.mt4_con_symbol

-- COMMAND ----------

DROP TABLE IF EXISTS bronze.mt4_con_symbol;
CREATE TABLE bronze.mt4_con_symbol
(
  frs_Timestamp BIGINT,
  frs_RecOperation STRING,
  frs_ServerID SMALLINT,
  group STRING,
  enable SMALLINT,
  timeout INT,
  otp_mode INT,
  company STRING,
  signature STRING,
  support_page STRING,
  smtp_server STRING,
  smtp_login STRING,
  smtp_password STRING,
  support_email STRING,
  templates STRING,
  copies INT,
  reports INT,
  default_leverage INT,
  default_deposit DECIMAL(18, 6),
  maxsecurities INT,
  currency STRING,
  credit DECIMAL(18, 6),
  margin_call INT,
  margin_mode INT,
  margin_stopout INT,
  INTerestrate DECIMAL(18, 6),
  use_swap INT,
  news INT,
  rights INT,
  check_ie_prices INT,
  maxpositions INT,
  close_reopen INT,
  hedge_prohibited INT,
  close_fifo INT,
  hedge_largeleg INT,
  margin_type INT,
  archive_period INT,
  archive_max_balance INT,
  stopout_skip_hedged INT,
  archive_pending_period INT
)
USING parquet
OPTIONS (path "/mnt/serdiukstorage/bronze/dict/mt4_con_symbol.parquet")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####create table bronze.mt4_con_symbol_group

-- COMMAND ----------

DROP TABLE IF EXISTS bronze.mt4_con_symbol_group;
CREATE TABLE bronze.mt4_con_symbol_group
(
  frs_Timestamp BIGINT,
  frs_RecOperation STRING,
  frs_ServerID SMALLINT,
	index INT,
	name STRING,
	description STRING
)
USING parquet
OPTIONS (path "/mnt/serdiukstorage/bronze/dict/mt4_con_symbol_group.parquet")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####create table bronze.mt4_user_record

-- COMMAND ----------

DROP TABLE IF EXISTS bronze.mt4_user_record;
CREATE TABLE bronze.mt4_user_record
(
	frs_Timestamp BIGINT,
	frs_RecOperation STRING,
	frs_ServerID SMALLINT,
	login INT,
	group STRING,
	enable SMALLINT,
	enable_change_password SMALLINT,
	enable_read_only SMALLINT,
	enable_otp INT,
	country STRING,
	state STRING,
	ClientID STRING,
	lead_source STRING,
	comment STRING,
	id STRING,
	status STRING,
	regdate INT,
	lastdate INT,
	leverage SMALLINT,
	agent_account INT,
	send_reports SMALLINT,
	mqid INT,
	user_color INT 
)
USING parquet
OPTIONS (path "/mnt/serdiukstorage/bronze/dict/mt4_user_record.parquet")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Trade mt5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####create table bronze.mt5_deal

-- COMMAND ----------

DROP TABLE IF EXISTS bronze.mt5_deal;
CREATE TABLE bronze.mt5_deal
(
	frs_RecOperation STRING,
	frs_ServerID SMALLINT,
	frs_CurrentSpread INT,
	Deal BIGINT,
	ExternalID STRING,
	Login BIGINT,
	Dealer BIGINT,
	Order BIGINT,
	Action INT,
	Entry INT,
	Reason INT,
	Digits INT,
	DigitsCurrency INT,
	ContractSize DECIMAL(18, 6),
	Time BIGINT,
	Symbol STRING,
	Price DECIMAL(18, 6),
	Volume BIGINT,
	Profit DECIMAL(18, 6),
	Storage DECIMAL(18, 6),
	Commission DECIMAL(18, 6),
	RateProfit DECIMAL(18, 6),
	RateMargin DECIMAL(18, 6),
	ExpertID DECIMAL(20, 0),
	Comment STRING,
	ProfitRaw DECIMAL(18, 6),
	PricePosition DECIMAL(18, 6),
	VolumeClosed BIGINT,
	TickValue DECIMAL(18, 6),
	TickSize DECIMAL(18, 6),
	Flags BIGINT,
	Gateway STRING,
	PriceGateway DECIMAL(18, 6),
	PositionID BIGINT,
	ModificationFlags INT,
	TimeMsc BIGINT,
	PriceSL DECIMAL(18, 6),
	PriceTP DECIMAL(18, 6),
	VolumeExt BIGINT,
	VolumeClosedExt BIGINT,
	exinity_Leverage BIGINT,
	exinity_InvestedAmount BIGINT,
	exinity_MoneyTakeProfit BIGINT,
	exinity_MoneyStopLoss BIGINT,
	exinity_TradingFlags BIGINT
)
USING parquet
OPTIONS (path "/mnt/serdiukstorage/bronze/trade/*/mt5_deal.parquet")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####create table bronze.mt5_con_group

-- COMMAND ----------

DROP TABLE IF EXISTS bronze.mt5_con_group;
CREATE TABLE bronze.mt5_con_group
(
	frs_Timestamp DECIMAL(20, 0),
	frs_RecOperation STRING,
	frs_ServerID SMALLINT,	
	Group STRING,
	Server BIGINT,
	PermissionsFlags BIGINT,
	AuthMode INT,
	AuthPasswordMin INT,
	Company STRING,
	CompanyPage STRING,
	CompanyEmail STRING,
	CompanySupportPage STRING,
	CompanySupportEmail STRING,
	CompanyCatalog STRING,
	Currency STRING,
	CurrencyDigits INT,
	ReportsMode INT,
	ReportsFlags BIGINT,
	NewsMode INT,
	NewsCategory STRING,
	MailMode INT,
	TradeFlags BIGINT,
	TradeINTerestrate DECIMAL(18, 6),
	TradeVirtualCredit DECIMAL(18, 6),
	MarginMode INT,
	MarginFlags BIGINT,
	MarginSOMode INT,
	MarginFreeMode INT,
	MarginCall DECIMAL(18, 6),
	MarginStopOut DECIMAL(18, 6),
	MarginFreeProfitMode INT,
	DemoLeverage INT,
	DemoDeposit DECIMAL(18, 6),
	LimitHistory INT,
	LimitOrders INT,
	LimitSymbols INT,
	AuthOTPMode INT,
	TradeTransferMode INT,
	LimitPositions INT,
	ReportsSMTP STRING,
	ReportsSMTPLogin STRING,
	ReportsSMTPPass STRING
)
USING parquet
OPTIONS (path "/mnt/serdiukstorage/bronze/dict/mt5_con_group.parquet")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####create table bronze.mt5_con_group_symbol

-- COMMAND ----------

DROP TABLE IF EXISTS bronze.mt5_con_group_symbol;
CREATE TABLE bronze.mt5_con_group_symbol
(
	frs_Timestamp DECIMAL(20, 0),
	frs_RecOperation STRING,
	frs_ServerID SMALLINT,
	Group STRING,
	Path STRING,
	ExecMode BIGINT,
	ExpirFlags BIGINT,
	FillFlags BIGINT,
	FreezeLevel INT,
	IECheckMode BIGINT,
	IESlipLosing BIGINT,
	IESlipProfit BIGINT,
	IETimeout DECIMAL(20, 0),
	IEVolumeMax DECIMAL(20, 0),
	MarginFlags BIGINT,
	MarginHedged DECIMAL(18, 6),
	MarginInitial DECIMAL(18, 6),
	MarginLimit DECIMAL(18, 6),
	MarginLong DECIMAL(18, 6),
	MarginMaINTenance DECIMAL(18, 6),
	MarginRateCurrency DECIMAL(18, 6),
	MarginRateLiquidity DECIMAL(18, 6),
	MarginShort DECIMAL(18, 6),
	MarginStop DECIMAL(18, 6),
	MarginStopLimit DECIMAL(18, 6),
	OrderFlags BIGINT,
	PermissionsFlags BIGINT,
	REFlags BIGINT,
	RETimeout BIGINT,
	SpreadDiff INT,
	SpreadDiffBalance INT,
	StopsLevel INT,
	Swap3Day INT,
	SwapLong DECIMAL(18, 6),
	SwapMode BIGINT,
	SwapShort DECIMAL(18, 6),
	TradeMode BIGINT,
	VolumeLimit DECIMAL(20, 0),
	VolumeMax DECIMAL(20, 0),
	VolumeMin DECIMAL(20, 0),
	VolumeStep DECIMAL(20, 0),
	BookDepthLimit BIGINT,
	VolumeMinExt DECIMAL(20, 0),
	VolumeMaxExt DECIMAL(20, 0),
	VolumeStepExt DECIMAL(20, 0),
	VolumeLimitExt DECIMAL(20, 0),
	IEVolumeMaxExt DECIMAL(20, 0)
)
USING parquet
OPTIONS (path "/mnt/serdiukstorage/bronze/dict/mt5_con_group_symbol.parquet")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####create table bronze.mt5_con_symbol

-- COMMAND ----------

DROP TABLE IF EXISTS bronze.mt5_con_symbol;
CREATE TABLE bronze.mt5_con_symbol
(
	frs_Timestamp DECIMAL(20, 0),
	frs_RecOperation STRING,
	frs_ServerID SMALLINT,
	Symbol STRING,
	Path STRING,
	ISIN STRING,
	Description STRING,
	International STRING,
	Basis STRING,
	Source STRING,
	Page STRING,
	CurrencyBase STRING,
	CurrencyProfit STRING,
	CurrencyMargin STRING,
	Color BIGINT,
	ColorBackground BIGINT,
	Digits INT,
	Point DECIMAL(18, 6),
	Multiply DECIMAL(18, 6),
	TickFlags DECIMAL(20, 0),
	TickBookDepth INT,
	FilterSoft INT,
	FilterSoftTicks INT,
	FilterHard INT,
	FilterHardTicks INT,
	FilterDiscard INT,
	FilterSpreadMax INT,
	FilterSpreadMin INT,
	TradeMode INT,
	TradeFlags BIGINT,
	CalcMode INT,
	ExecMode INT,
	GTCMode INT,
	FillFlags INT,
	ExpirFlags INT,
	OrderFlags INT,
	Spread INT,
	SpreadBalance INT,
	SpreadDiff INT,
	SpreadDiffBalance INT,
	TickValue DECIMAL(18, 6),
	TickSize DECIMAL(18, 6),
	ContractSize DECIMAL(18, 6),
	StopsLevel INT,
	FreezeLevel INT,
	QuotesTimeout INT,
	VolumeMin BIGINT,
	VolumeMax BIGINT,
	VolumeStep BIGINT,
	VolumeLimit BIGINT,
	MarginInitial DECIMAL(18, 6),
	MarginMaintenance DECIMAL(18, 6),
	SwapMode INT,
	SwapLong DECIMAL(18, 6),
	SwapShort DECIMAL(18, 6),
	Swap3Day INT,
	REFlags INT,
	RETimeout INT,
	IECheckMode INT,
	IETimeout INT,
	IESlipProfit INT,
	IESlipLosing INT,
	IEVolumeMax BIGINT,
	PriceSettle DECIMAL(18, 6),
	PriceLimitMax DECIMAL(18, 6),
	PriceLimitMin DECIMAL(18, 6),
	SpliceType INT,
	SpliceTimeType INT,
	SpliceTimeDays INT,
	MarginFlags INT,
	TimeStart BIGINT,
	TimeExpiration BIGINT,
	OptionsMode INT,
	FaceValue DECIMAL(18, 6),
	AccruedInterest DECIMAL(18, 6),
	ChartMode INT,
	FilterGap INT,
	FilterGapTicks INT,
	CurrencyBaseDigits INT,
	CurrencyProfitDigits INT,
	CurrencyMarginDigits INT,
	MarginRateLiqudity DECIMAL(18, 6),
	MarginHedged DECIMAL(18, 6),
	MarginRateCurrency DECIMAL(18, 6),
	MarginLimit DECIMAL(18, 6),
	MarginLong DECIMAL(18, 6),
	MarginShort DECIMAL(18, 6),
	MarginStop DECIMAL(18, 6),
	MarginStopLimit DECIMAL(18, 6),
	PriceStrike DECIMAL(18, 6),
	VolumeMinExt DECIMAL(20, 0),
	VolumeMaxExt DECIMAL(20, 0),
	VolumeStepExt DECIMAL(20, 0),
	VolumeLimitExt DECIMAL(20, 0),
	IEVolumeMaxExt DECIMAL(20, 0)
)
USING parquet
OPTIONS (path "/mnt/serdiukstorage/bronze/dict/mt5_con_symbol.parquet")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####create table bronze.mt5_user

-- COMMAND ----------

DROP TABLE IF EXISTS bronze.mt5_user;
CREATE TABLE bronze.mt5_user
(
	frs_Timestamp BIGINT,
	frs_RecOperation STRING,
	frs_ServerID SMALLINT,
	Login BIGINT,
	Group STRING,
	Rights decimal(20, 0),
	MQID STRING,
	Registration BIGINT,
	LastAccess BIGINT,
	Company STRING,
	Account STRING,
	Country STRING,
	State STRING,
	ClientID STRING,
	ID STRING,
	Status STRING,
	Comment STRING,
	Color BIGINT,
	Leverage BIGINT,
	Agent decimal(20, 0),
	LeadSource STRING,
	LeadCampaign STRING,
	CommissionAgentDaily decimal(18, 6),
	CommissionAgentMonthly decimal(18, 6)
)
USING parquet
OPTIONS (path "/mnt/serdiukstorage/bronze/dict/mt5_user.parquet")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####CRM

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####create table bronze.client

-- COMMAND ----------

DROP TABLE IF EXISTS bronze.client;
CREATE TABLE bronze.client
(
  id INT,
  date_reg TIMESTAMP,
  title_id INT,
  birth_date DATE,
  state_reg_id INT,
  city_reg STRING,
  change_status_ts STRING,
  status_id INT,
  country_reg_id INT,
  email STRING,
  sex_id INT,
  company_id SMALLINT,
  reg_mail_is_send SMALLINT,
  agent_id INT,
  agent_source STRING,
  affiliate_id STRING,
  lang_id SMALLINT,
  branch_id INT,
  citizenship_country_id INT,
  last_activity TIMESTAMP,
  company_reg_number STRING,
  company_reg_city STRING,
  company_reg_country INT,
  is_legal_form SMALLINT,
  blacklist_id SMALLINT,
  blacklist_bo_id SMALLINT,
  automatic SMALLINT,
  parent_id INT,
  old_id INT,
  old_table STRING,
  client_type_id SMALLINT,
  created_boffice_user_id INT,
  is_premium SMALLINT,
  use_servers_rules SMALLINT,
  strategy_explanation STRING,
  strategy_id SMALLINT,
  strategy_ts TIMESTAMP,
  pap_affiliate_id STRING,
  pap_ref_id STRING,
  pap_tmp_id STRING,
  ready_for_approval SMALLINT,
  api SMALLINT,
  promotions_restriction SMALLINT,
  referrer_id STRING,
  is_raf_tc_accepted SMALLINT,
  appropriateness_score SMALLINT,
  appropriateness_type_id SMALLINT,
  is_ngn SMALLINT,
  nationality_country_id INT,
  asm STRING,
  concat STRING,
  mifir STRING,
  due_diligence SMALLINT,
  last_modify TIMESTAMP,
  disable_mobile_trading SMALLINT,
  last_trading_app_login_date TIMESTAMP,
  appropriateness_maxscore_ts SMALLINT,
  is_name_empty SMALLINT,
  test SMALLINT,
  agent_campaign_id STRING
)
USING parquet
OPTIONS (path "/mnt/serdiukstorage/bronze/crm/client.parquet")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####create table bronze.country

-- COMMAND ----------

DROP TABLE IF EXISTS bronze.country;
CREATE TABLE bronze.country
(
  id INT,
  name STRING,
  code STRING,
  code_2l STRING,
  code_alpha2 STRING,
  phone_code SMALLINT,
  eu SMALLINT,
  ft SMALLINT,
  uk SMALLINT,
  formatted_name STRING,
  iso_code INT
)
USING parquet
OPTIONS (path "/mnt/serdiukstorage/bronze/crm/country.parquet")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Rates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####create table bronze.currency_rates

-- COMMAND ----------

DROP TABLE IF EXISTS bronze.currency_rates;
CREATE TABLE bronze.currency_rates
(
	currency_date DATE,
	src_currency STRING,
	dst_currency STRING,
	value DECIMAL(30, 18),
	source_id INT,
	load_ts TIMESTAMP
)
USING parquet
OPTIONS (path "/mnt/serdiukstorage/bronze/ssor/currency_rates.parquet")

-- COMMAND ----------

select * from bronze.currency_rates

-- COMMAND ----------

DESCRIBE EXTENDED bronze.mt5_con_group

-- COMMAND ----------


