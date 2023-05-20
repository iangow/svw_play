library(tidyverse)
library(DBI)
library(dbplyr)

start_date <- ymd("1960-01-01")
end_date <- ymd("2021-12-31")

tidy_finance <- dbConnect(
  duckdb::duckdb(),
  "data/tidy_finance.duckdb",
  read_only = FALSE)

# dsf_db <- tbl(wrds, in_schema("crsp", "dsf"))
#wrds <- dbConnect(RPostgres::Postgres(), bigint = "integer")

dbExecute(tidy_finance, "LOAD postgres_scanner")
dbExecute(tidy_finance, "SET threads TO 3")

wrds_string <- paste0("dbname=wrds user=iangow host=wrds-pgdata.wharton.upenn.edu",
                     " port=9737 sslmode=require")

#wrds_string <- paste0("dbname=igow user=igow host=iangow.me",
#                      " port=54320")
#wrds_string <- ''
schema <- 'crsp_a_stock'
# schema <- 'crsp'
dsf_db <- tbl(tidy_finance, 
              sql(paste0("(SELECT * FROM postgres_scan_pushdown('",
                    wrds_string,"', '", schema,"', 'dsf'))")))
dsf_db

factors_ff_daily <- 
  tbl(tidy_finance, "factors_ff_daily") 

rs <- dbExecute(tidy_finance, "DROP TABLE IF EXISTS crsp_daily")

system.time({
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
})

dbDisconnect(tidy_finance, shutdown = TRUE)
