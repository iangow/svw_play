library(tidyverse)
library(scales)

crsp_monthly <- read_rds("data/crsp_monthly.rds")
compustat <- read_rds("data/compustat.rds")

crsp_monthly |>
  group_by(permno, year = year(month)) |>
  filter(date == max(date)) |>
  ungroup() |>
  left_join(compustat, by = c("gvkey", "year")) |>
  mutate(permno_be = if_else(!is.na(be), permno, NA)) |>
  group_by(exchange, year) |>
  summarize(
    share = 1.0 * n_distinct(permno_be) / n_distinct(permno),
    .groups = "drop"
  ) |>
  ggplot(aes(
    x = year, 
    y = share, 
    color = exchange,
    linetype = exchange
  )) +
  geom_line() +
  labs(
    x = NULL, y = NULL, color = NULL, linetype = NULL,
    title = "Share of securities with book equity values by exchange"
  ) +
  scale_y_continuous(labels = percent) +
  coord_cartesian(ylim = c(0, 1))
