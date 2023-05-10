library(tidyverse)
library(DBI)
library(dbplyr)

start_date <- ymd("1960-01-01")
end_date <- ymd("2021-12-31")

# WRDS, SQLite ----
wrds <- dbConnect(RPostgres::Postgres(), bigint = "integer")

tidy_finance <- dbConnect(
  RSQLite::SQLite(),
  "data/tidy_finance.sqlite",
  extended_types = TRUE)

dsf_db <- tbl(wrds, in_schema("crsp", "dsf"))

factors_ff_daily <- tbl(tidy_finance, "factors_ff_daily") |>
  collect()

permnos <- tbl(tidy_finance, "crsp_monthly") |>
  distinct(permno) |>
  pull()

progress <- txtProgressBar(
  min = 0,
  max = length(permnos),
  initial = 0,
  style = 3
)

for (j in 1:length(permnos)) {
  permno_sub <- permnos[j]
  crsp_daily_sub <- dsf_db |>
    filter(permno == permno_sub &
             date >= start_date & date <= end_date) |>
    select(permno, date, ret) |>
    collect() |>
    drop_na()
  
  if (nrow(crsp_daily_sub) > 0) {
    crsp_daily_sub <- crsp_daily_sub |>
      mutate(month = floor_date(date, "month")) |>
      left_join(factors_ff_daily |>
                  select(date, rf), by = "date") |>
      mutate(
        ret_excess = ret - rf,
        ret_excess = pmax(ret_excess, -1)
      ) |>
      select(permno, date, month, ret_excess)
    
    dbWriteTable(tidy_finance,
                 "crsp_daily",
                 value = crsp_daily_sub,
                 overwrite = ifelse(j == 1, TRUE, FALSE),
                 append = ifelse(j != 1, TRUE, FALSE)
    )
  }
  setTxtProgressBar(progress, j)
}

close(progress)

res <- dbSendQuery(tidy_finance, "VACUUM")
res
dbClearResult(res)
dbDisconnect(tidy_finance)

# DuckDB ----
tidy_finance <- dbConnect(
  RSQLite::SQLite(),
  "data/tidy_finance.sqlite",
  extended_types = TRUE)

crsp_daily_db <- tbl(tidy_finance, "crsp_daily")

ddb <- dbConnect(
  duckdb::duckdb(),
  "data/tidy_finance.duckdb",
  read_only = FALSE
)

copy_to(ddb, crsp_daily_db, "crsp_daily",
        temporary = FALSE, overwrite = TRUE)

dbDisconnect(ddb, shutdown = TRUE)
dbDisconnect(tidy_finance)
