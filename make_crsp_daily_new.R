library(tidyverse)
library(DBI)
library(dbplyr)
library(farr)

start_date <- ymd("1960-01-01")
end_date <- ymd("2021-12-31")

db <- dbConnect(duckdb::duckdb())

dsf <- load_parquet(db, "dsf", "crsp")
factors_ff_daily <- load_parquet(db, "factors_ff_daily", "tidy_finance")

rs <- dbExecute(db, "DROP TABLE IF EXISTS crsp_daily")

crsp_daily <- 
  dsf_db |>
  filter(date >= start_date & date <= end_date) |>
  select(permno, date, ret) |>
  filter(!is.na(ret)) |>
  mutate(month = as.Date(floor_date(date, "month"))) |>
  select(permno, date, month, ret) |>
  left_join(factors_ff_daily |>
              select(date, rf), by = "date") |>
  mutate(
    ret_excess = ret - rf,
    ret_excess = pmax(ret_excess, -1, na.rm = TRUE)
  ) |>
  select(permno, date, month, ret_excess) |>
  compute(name = "crsp_daily", temporary = FALSE)

dbDisconnect(db, shutdown = TRUE)
