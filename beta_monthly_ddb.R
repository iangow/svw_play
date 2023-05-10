library(tidyverse)
library(DBI)
library(scales)
library(slider)
library(furrr)

ddb <- dbConnect(
  duckdb::duckdb(),
  "data/tidy_finance.duckdb",
  read_only = TRUE
)

crsp_monthly <- tbl(ddb, "crsp_monthly") 

factors_ff_monthly <- tbl(ddb, "factors_ff_monthly")

crsp_monthly <- 
  crsp_monthly |>
  left_join(factors_ff_monthly, by = "month") |>
  select(permno, month, industry, ret_excess, mkt_excess)

fit <- lm(ret_excess ~ mkt_excess,
          data = crsp_monthly |>
            filter(permno == "14593")
)

summary(fit)

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
  data <- 
    data |>
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

examples <- tribble(
  ~permno, ~company,
  14593, "Apple",
  10107, "Microsoft",
  93436, "Tesla",
  17778, "Berkshire Hathaway"
)

beta_example <- 
  crsp_monthly |>
  filter(permno == !!examples$permno[1]) |>
  collect() |>
  mutate(roll_capm_estimation(pick(everything()), months = 60, min_obs = 48)) |>
  drop_na()
beta_example

beta_examples <- 
  crsp_monthly |>
  inner_join(examples, by = "permno", copy = TRUE) |>
  collect() |>
  group_by(permno) |>
  mutate(roll_capm_estimation(pick(everything()), months = 60, min_obs = 48)) |>
  ungroup() |>
  select(permno, company, month, beta) |>
  drop_na()

beta_examples |>
  ggplot(aes(
    x = month, 
    y = beta, 
    color = company,
    linetype = company)) +
  geom_line() +
  labs(
    x = NULL, y = NULL, color = NULL, linetype = NULL,
    title = "Monthly beta estimates for example stocks using 5 years of data"
  )

crsp_monthly_nested <- 
  crsp_monthly |>
  collect() |>
  nest(data = c(month, ret_excess, mkt_excess))
crsp_monthly_nested

crsp_monthly_nested |>
  inner_join(examples, by = "permno") |>
  mutate(beta = map(
    data,
    ~ roll_capm_estimation(., months = 60, min_obs = 48)
  )) |>
  unnest(beta) |>
  select(permno, month, beta_monthly = beta) |>
  drop_na()

plan(multisession, workers = availableCores())

beta_monthly <- crsp_monthly_nested |>
  mutate(beta = future_map(
    data, ~ roll_capm_estimation(., months = 60, min_obs = 48)
  )) |>
  unnest(c(beta)) |>
  select(permno, month, beta_monthly = beta) |>
  drop_na() %>%
  arrange(permno, month)

beta_monthly
dbDisconnect(ddb, shutdown = TRUE)

print(beta_monthly)
