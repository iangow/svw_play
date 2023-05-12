library(tidyverse)
library(DBI)
library(dbplyr)

start_date <- ymd("1960-01-01")
end_date <- ymd("2021-12-31")

wrds <- dbConnect(RPostgres::Postgres(), bigint = "integer")

tidy_finance <- dbConnect(
  duckdb::duckdb(),
  "data/tidy_finance.duckdb",
  read_only = FALSE)

dsf_db <- tbl(wrds, in_schema("crsp", "dsf"))

factors_ff_daily <- 
  tbl(tidy_finance, "factors_ff_daily") |>
  collect() |>
  copy_inline(wrds, df = _)

rs <- dbExecute(tidy_finance, "DROP TABLE IF EXISTS crsp_daily")

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
  copy_to(tidy_finance, df = _, name = "crsp_daily", temporary = FALSE)

dbDisconnect(tidy_finance, shutdown = TRUE)
