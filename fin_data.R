library(tidyverse)
library(scales)
library(frenchdata)
library(tidyquant)
library(dbplyr)
library(readxl)
library(googledrive)
library(DBI)

start_date <- ymd("1960-01-01")
end_date <- ymd("2021-12-31")

# FF data --------------
factors_ff_monthly_raw <- download_french_data("Fama/French 3 Factors")
factors_ff_monthly <-
  factors_ff_monthly_raw$subsets$data[[1]] |>
  transmute(
    month = floor_date(ymd(str_c(date, "01")), "month"),
    rf = as.numeric(RF) / 100,
    mkt_excess = as.numeric(`Mkt-RF`) / 100,
    smb = as.numeric(SMB) / 100,
    hml = as.numeric(HML) / 100
  ) |>
  filter(month >= start_date & month <= end_date)

factors_ff_daily_raw <- download_french_data("Fama/French 3 Factors [Daily]")
factors_ff_daily <- 
  factors_ff_daily_raw$subsets$data[[1]] |>
  transmute(
    date = ymd(date),
    rf = as.numeric(RF) / 100,
    mkt_excess = as.numeric(`Mkt-RF`) / 100,
    smb = as.numeric(SMB) / 100,
    hml = as.numeric(HML) / 100
  ) |>
  filter(date >= start_date & date <= end_date)

industries_ff_monthly_raw <- download_french_data("10 Industry Portfolios")
industries_ff_monthly <- 
  industries_ff_monthly_raw$subsets$data[[1]] |>
  mutate(month = floor_date(ymd(str_c(date, "01")), "month")) |>
  mutate(across(where(is.numeric), ~ . / 100)) |>
  select(month, everything(), -date) |>
  filter(month >= start_date & month <= end_date)

factors_q_monthly_link <-
  "http://global-q.org/uploads/1/2/2/6/122679606/q5_factors_monthly_2021.csv"

factors_q_monthly <- 
  read_csv(factors_q_monthly_link) |>
  mutate(month = ymd(str_c(year, month, "01", sep = "-"))) |>
  select(-R_F, -R_MKT, -year) |>
  rename_with(~ str_remove(., "R_")) |>
  rename_with(~ str_to_lower(.)) |>
  mutate(across(-month, ~ . / 100)) |>
  filter(month >= start_date & month <= end_date)

# macro_predictors -----------------------------------------------------------
drive_deauth()

macro_predictors_link <-
  "https://docs.google.com/spreadsheets/d/1g4LOaRj4TvwJr9RIaA_nwrXXWTOy46bP"

if (!dir.exists("data")) dir.create("data")

if (!file.exists("data/macro_predictors.xlsx")) {
  drive_download(
    macro_predictors_link,
    path = "data/macro_predictors.xlsx")
}

macro_predictors <- read_xlsx(
  "data/macro_predictors.xlsx",
  sheet = "Monthly"
) |>
  mutate(month = ym(yyyymm)) |>
  mutate(across(where(is.character), as.numeric)) |>
  mutate(
    IndexDiv = Index + D12,
    logret = log(IndexDiv) - log(lag(IndexDiv)),
    Rfree = log(Rfree + 1),
    rp_div = lead(logret - Rfree, 1), # Future excess market return
    dp = log(D12) - log(Index), # Dividend Price ratio
    dy = log(D12) - log(lag(Index)), # Dividend yield
    ep = log(E12) - log(Index), # Earnings price ratio
    de = log(D12) - log(E12), # Dividend payout ratio
    tms = lty - tbl, # Term spread
    dfy = BAA - AAA # Default yield spread
  ) |>
  select(month, rp_div, dp, dy, ep, de, svar,
         bm = `b/m`, ntis, tbl, lty, ltr,
         tms, dfy, infl
  ) |>
  filter(month >= start_date & month <= end_date) |>
  drop_na()

cpi_monthly <- tq_get("CPIAUCNS",
                      get = "economic.data",
                      from = start_date,
                      to = end_date
) |>
  transmute(
    month = floor_date(date, "month"),
    cpi = price / price[month == max(month)]
  )

# SQLite ------------------------------------------------------------------
tidy_finance <- dbConnect(
  RSQLite::SQLite(),
  "data/tidy_finance.sqlite",
  extended_types = TRUE
)

dbWriteTable(tidy_finance,
             "factors_ff_daily",
             value = factors_ff_daily,
             overwrite = TRUE)

dbWriteTable(tidy_finance,
             "factors_ff_monthly",
             value = factors_ff_monthly,
             overwrite = TRUE)

dbWriteTable(tidy_finance,
             "industries_ff_monthly",
             value = industries_ff_monthly,
             overwrite = TRUE)

dbWriteTable(tidy_finance,
             "factors_q_monthly",
             value = factors_q_monthly,
             overwrite = TRUE)

dbWriteTable(tidy_finance,
             "macro_predictors",
             value = macro_predictors,
             overwrite = TRUE)

dbWriteTable(tidy_finance,
             "cpi_monthly",
             value = cpi_monthly,
             overwrite = TRUE)

res <- dbSendQuery(tidy_finance, "VACUUM")
res
dbClearResult(res)
dbListTables(tidy_finance)
dbDisconnect(tidy_finance)

# DuckDB ------------------------------------------------------------------
tidy_finance <- dbConnect(
  duckdb::duckdb(),
  "data/tidy_finance.duckdb",
  read_only = FALSE)

dbWriteTable(tidy_finance,
             "factors_ff_daily",
             value = factors_ff_daily,
             overwrite = TRUE)

dbWriteTable(tidy_finance,
             "factors_ff_monthly",
             value = factors_ff_monthly,
             overwrite = TRUE)

dbWriteTable(tidy_finance,
             "industries_ff_monthly",
             value = industries_ff_monthly,
             overwrite = TRUE)

dbWriteTable(tidy_finance,
             "factors_q_monthly",
             value = factors_q_monthly,
             overwrite = TRUE)

dbWriteTable(tidy_finance,
             "macro_predictors",
             value = macro_predictors,
             overwrite = TRUE)

dbWriteTable(tidy_finance,
             "cpi_monthly",
             value = cpi_monthly,
             overwrite = TRUE)

res <- dbSendQuery(tidy_finance, "VACUUM")
res
dbClearResult(res)
dbListTables(tidy_finance)
dbDisconnect(tidy_finance, shutdown = TRUE)