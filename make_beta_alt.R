library(tidyverse)
library(DBI)
library(dbplyr)

tidy_finance <- dbConnect(
  duckdb::duckdb(),
  "data/tidy_finance.duckdb",
  read_only = FALSE)

crsp_monthly <- tbl(tidy_finance, "crsp_monthly") 
crsp_daily <- tbl(tidy_finance, "crsp_daily") 

factors_ff_monthly <- tbl(tidy_finance, "factors_ff_monthly")
factors_ff_daily <- tbl(tidy_finance, "factors_ff_daily")

window <- "OVER (PARTITION BY permno ORDER BY month
           RANGE BETWEEN INTERVAL 59 MONTHS PRECEDING
              AND INTERVAL 0 MONTHS FOLLOWING)"

beta_sql <- sql(paste("regr_slope(ret_excess, mkt_excess)", window))
n_sql <- sql(paste("count(*)", window))
max_sql <- sql(paste("max(month)", window))
min_sql <- sql(paste("min(month)", window))

beta_monthly <- 
  crsp_monthly |>
  left_join(factors_ff_monthly, by = "month") |>
  mutate(beta_monthly = beta_sql,
         n = n_sql,
         max_month = max_sql,
         min_month = min_sql) |>
  ungroup() |>
  # Check that months are consecutive
  mutate(n_months = date_diff('month', min_month, max_month) + 1) |>
  filter(n >= 48, n_months == n) |>
  select(permno, month, beta_monthly) |>
  filter(!is.na(beta_monthly)) %>%
  compute()

window <- "OVER (PARTITION BY permno ORDER BY month
           RANGE BETWEEN INTERVAL 2 MONTHS PRECEDING
              AND INTERVAL 0 MONTHS FOLLOWING)"

beta_sql <- sql(paste("regr_slope(ret_excess, mkt_excess)", window))
n_sql <- sql(paste("count(*)", window))

beta_daily <- 
  crsp_daily |>
  left_join(factors_ff_daily, by = "date") |>
  select(permno, month, ret_excess, mkt_excess) |>
  filter(!is.na(ret_excess)) |>
  mutate(beta_daily = beta_sql, n = n_sql) |>
  filter(n >= 50) |>
  select(permno, month, beta_daily) |>
  distinct() |>
  compute()

dbExecute(tidy_finance, "DROP TABLE IF EXISTS beta_alt")

beta_alt <- 
  beta_monthly |>
  full_join(beta_daily, by = c("permno", "month")) |>
  arrange(permno, month) %>%
  compute(name = "beta_alt", temporary = FALSE)

dbDisconnect(tidy_finance, shutdown = TRUE)
