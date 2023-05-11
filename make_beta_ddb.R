library(tidyverse)
library(DBI)
library(slider)
library(furrr)

tidy_finance <- dbConnect(
  duckdb::duckdb(),
  "data/tidy_finance.duckdb",
  read_only = FALSE
)

crsp_monthly <- tbl(tidy_finance, "crsp_monthly") |>
  collect()

factors_ff_monthly <- tbl(tidy_finance, "factors_ff_monthly") |>
  collect()

crsp_monthly <- crsp_monthly |>
  left_join(factors_ff_monthly, by = "month") |>
  select(permno, month, industry, ret_excess, mkt_excess)

crsp_monthly_nested <- crsp_monthly |>
  nest(data = c(month, ret_excess, mkt_excess))

plan(multisession, workers = availableCores())

estimate_capm <- function(data, min_obs = 1) {
  if (nrow(data) < min_obs) {
    beta <- as.numeric(NA)
  } else {
    fit <- lm(ret_excess ~ mkt_excess, data = data)
    beta <- as.numeric(coefficients(fit)[2])
  }
  return(beta)
}

roll_capm_estimation <- function(data, months, min_obs) {
  data <- data |>
    arrange(month)
  
  betas <- slide_period_vec(
    .x = data,
    .i = data$month,
    .period = "month",
    .f = ~ estimate_capm(., min_obs),
    .before = months - 1,
    .complete = FALSE
  )
  
  return(tibble(
    month = unique(data$month),
    beta = betas
  ))
}

beta_monthly <- crsp_monthly_nested |>
  mutate(beta = future_map(
    data, ~ roll_capm_estimation(., months = 60, min_obs = 48)
  )) |>
  unnest(c(beta)) |>
  select(permno, month, beta_monthly = beta) |>
  drop_na()

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

beta_daily <- 
  crsp_daily_nested |>
  mutate(beta_daily = future_map(
    data, ~ roll_capm_estimation(., months = 3, min_obs = 50)
  )) |>
  unnest(c(beta_daily)) |>
  select(permno, month, beta_daily = beta) |>
  drop_na()

beta <- beta_monthly |>
  full_join(beta_daily, by = c("permno", "month")) |>
  arrange(permno, month)

dbWriteTable(tidy_finance,
             "beta",
             value = beta,
             overwrite = TRUE)

dbDisconnect(tidy_finance, shutdown = TRUE)