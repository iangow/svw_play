library(tidyverse)
library(DBI)
library(scales)
library(slider)
library(furrr)
library(dbplyr)

roll_capm_estimation <- function(data, days, min_obs) {
  beta_sql <- win_over(sql("regr_slope(ret_excess, mkt_excess)"), 
                      frame = c(-days, -1), 
                      order = "date", con = tidy_finance)
  
  n_sql <- win_over(sql("count(*)"), 
                       frame = c(-days, -1), 
                       order = "date", con = tidy_finance)
  
  data |>
    mutate(beta = beta_sql,
           num_obs = n_sql) |>
    filter(num_obs >= min_obs) %>%
    select(permno, date, beta)
}

tidy_finance <- dbConnect(
  duckdb::duckdb(),
  "data/tidy_finance.duckdb",
  read_only = TRUE
)

crsp_daily <- tbl(tidy_finance, "crsp_daily")
factors_ff_daily <- tbl(tidy_finance, "factors_ff_daily")

examples <- tribble(
  ~permno, ~company,
  14593, "Apple",
  10107, "Microsoft",
  93436, "Tesla",
  17778, "Berkshire Hathaway"
)

example_betas <-
  crsp_daily |>
  left_join(factors_ff_daily, by = "date") |>
  select(permno, date, ret_excess, mkt_excess) |>
  inner_join(examples, by = "permno", copy = TRUE) |>
  roll_capm_estimation(days = 90, min_obs = 50) |>
  select(permno, date, beta_daily = beta) |>
  collect() %>%
  drop_na() %>%
  arrange(permno, date)

beta_daily <- 
  crsp_daily |>
  left_join(factors_ff_daily, by = "date") |>
  select(permno, date, ret_excess, mkt_excess) |>
  roll_capm_estimation(days = 90, min_obs = 50) |>
  select(permno, date, beta_daily = beta) |>
  collect() %>%
  drop_na() %>%
  arrange(permno, date)

dbDisconnect(tidy_finance, shutdown = TRUE)