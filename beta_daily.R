library(tidyverse)
library(DBI)
library(slider)
library(furrr)

tidy_finance <- dbConnect(
  RSQLite::SQLite(),
  "data/tidy_finance.sqlite",
  extended_types = TRUE
)

crsp_daily <- tbl(tidy_finance, "crsp_daily") |>
  collect()

factors_ff_daily <- tbl(tidy_finance, "factors_ff_daily") |>
  collect()

crsp_daily <- 
  crsp_daily |>
  inner_join(factors_ff_daily, by = "date") |>
  select(permno, month, ret_excess, mkt_excess)

crsp_daily_nested <-
  crsp_daily |>
  nest(data = c(month, ret_excess, mkt_excess))

estimate_capm <- function(data, min_obs = 1) {
  if (nrow(data) < min_obs) {
    beta <- as.numeric(NA)
  } else {
    fit <- lm(ret_excess ~ mkt_excess, data = data)
    beta <- as.numeric(coefficients(fit)[2])
  }
  return(beta)
}

examples <- tribble(
  ~permno, ~company,
  14593, "Apple",
  10107, "Microsoft",
  93436, "Tesla",
  17778, "Berkshire Hathaway"
)

crsp_daily_nested |>
  inner_join(examples, by = "permno") |>
  mutate(beta_daily = map(
    data,
    ~ roll_capm_estimation(., months = 3, min_obs = 50)
  )) |>
  unnest(c(beta_daily)) |>
  select(permno, month, beta_daily = beta) |>
  drop_na()

plan(multisession, workers = availableCores())

beta_daily <- 
  crsp_daily_nested |>
  mutate(beta_daily = future_map(
    data, ~ roll_capm_estimation(., months = 3, min_obs = 50)
  )) |>
  unnest(c(beta_daily)) |>
  select(permno, month, beta_daily = beta) |>
  drop_na()