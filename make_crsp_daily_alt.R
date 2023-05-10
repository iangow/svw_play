library(tidyverse)
library(DBI)
library(dbplyr)

start_date <- ymd("1960-01-01")
end_date <- ymd("2021-12-31")

wrds <- dbConnect(RPostgres::Postgres(), bigint = "integer")

tidy_finance <- dbConnect(
  RSQLite::SQLite(),
  "data/tidy_finance.sqlite",
  extended_types = TRUE)

dsf_db <- tbl(wrds, in_schema("crsp", "dsf"))

factors_ff_daily <- tbl(tidy_finance, "factors_ff_daily") 
  #collect() |>
  #copy_inline(wrds, df = _) %>%
  #compute()

permnos <- c(10000:10003, 10005:10030)
#tbl(tidy_finance, "crsp_monthly") |>
#  distinct(permno) |>
#  arrange(permno) |>
#  collect(n = 30) |>
#  pull()

progress <- txtProgressBar(
  min = 0,
  max = length(permnos),
  initial = 0,
  style = 3
)

for (j in 1:length(permnos)) {
  
  permno_sub <- permnos[j]
  crsp_daily_sub <- 
    dsf_db |>
    filter(permno == permno_sub &
             date >= start_date & date <= end_date) |>
    select(permno, date, ret) |>
    filter(!is.na(ret)) |>
    mutate(month = as.Date(floor_date(date, "month"))) |>
    select(permno, date, month, ret) |>
    copy_to(tidy_finance, df = _, name = "temp", overwrite = TRUE) |>
    left_join(factors_ff_daily |>
                select(date, rf), by = "date") |>
    mutate(
      ret_excess = ret - rf,
      ret_excess = pmax(ret_excess, -1, na.rm = TRUE)
    ) |>
    select(permno, date, month, ret_excess) 
    
    if (j == 1) { 
      dbExecute(tidy_finance, "DROP TABLE IF EXISTS crsp_daily")
      compute(crsp_daily_sub, name = "crsp_daily", 
              temporary = FALSE)
    } else {
      dbExecute(tidy_finance,
                sql_query_append(tidy_finance, 
                                 x_name = "crsp_daily", 
                                 y = crsp_daily_sub))
    }
  
  setTxtProgressBar(progress, j)
}

close(progress)

crsp_daily_db <- tbl(tidy_finance, "crsp_daily")


