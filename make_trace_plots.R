library(tidyverse)
library(DBI)

tidy_finance <- dbConnect(
  duckdb::duckdb(),
  "data/tidy_finance.duckdb",
  read_only = TRUE)

mergent <- tbl(tidy_finance, "mergent") |> collect()

cusips <- mergent %>% pull(complete_cusip)

bonds_outstanding <- 
  expand_grid(date = seq(ymd("2014-01-01"),
                           ymd("2016-11-30"), 
                           by = "quarter"), 
              complete_cusip = cusips) |> 
  # copy_to(tidy_finance, df = _, name = "cusips") |>
  left_join(mergent |> select(complete_cusip, 
                              offering_date,
                              maturity), 
            by = "complete_cusip") |> 
  mutate(offering_date = floor_date(offering_date),
         maturity = floor_date(maturity)) |> 
  filter(date >= offering_date & date <= maturity) |> 
  count(date) |> 
  mutate(type = "Outstanding")

trace_enhanced <- tbl(tidy_finance, "trace_enhanced") |> collect()

bonds_traded <- 
  trace_enhanced |> 
  mutate(date = floor_date(trd_exctn_dt, "quarters")) |> 
  group_by(date) |> 
  summarize(n = n_distinct(cusip_id),
            type = "Traded",
            .groups = "drop") 

bonds_outstanding |> 
  union_all(bonds_traded) |> 
  ggplot(aes(
    x = date, 
    y = n, 
    color = type, 
    linetype = type
  )) +
  geom_line() +
  labs(
    x = NULL, y = NULL, color = NULL, linetype = NULL,
    title = "Number of bonds outstanding and traded each quarter"
  )

mergent |>
  mutate(maturity = as.numeric(maturity - offering_date) / 365,
         offering_amt = offering_amt / 10^3) |> 
  pivot_longer(cols = c(maturity, coupon, offering_amt),
               names_to = "measure") |>
  group_by(measure) |>
  summarize(
    mean = mean(value, na.rm = TRUE),
    sd = sd(value, na.rm = TRUE),
    min = min(value, na.rm = TRUE),
    q05 = quantile(value, 0.05, na.rm = TRUE),
    q50 = quantile(value, 0.50, na.rm = TRUE),
    q95 = quantile(value, 0.95, na.rm = TRUE),
    max = max(value, na.rm = TRUE)
  )

trace_enhanced |> 
  group_by(trd_exctn_dt) |> 
  summarize(trade_size = sum(entrd_vol_qt * rptd_pr / 100, na.rm = TRUE) / 10^6,
            trade_number = n(),
            .groups = "drop") |> 
  pivot_longer(cols = c(trade_size, trade_number),
               names_to = "measure") |> 
  group_by(measure) |>
  summarize(
    mean = mean(value, na.rm = TRUE),
    sd = sd(value, na.rm = TRUE),
    min = min(value, na.rm = TRUE),
    q05 = quantile(value, 0.05, na.rm = TRUE),
    q50 = quantile(value, 0.50, na.rm = TRUE),
    q95 = quantile(value, 0.95, na.rm = TRUE),
    max = max(value, na.rm = TRUE)
  )

dbDisconnect(tidy_finance, shutdown = TRUE)
