library(tidyverse)
library(DBI)
library(dbplyr)

start_date <- ymd("1960-01-01")
end_date <- ymd("2021-12-31")

# WRDS ----
wrds <- dbConnect(RPostgres::Postgres(), bigint = "integer")

funda_db <- tbl(wrds, in_schema("comp", "funda"))
compustat <- 
  funda_db |>
  filter(
    indfmt == "INDL" &
      datafmt == "STD" &
      consol == "C" &
      datadate >= start_date & datadate <= end_date
  ) |>
  select(
    gvkey, # Firm identifier
    datadate, # Date of the accounting data
    seq, # Stockholders' equity
    ceq, # Total common/ordinary equity
    at, # Total assets
    lt, # Total liabilities
    txditc, # Deferred taxes and investment tax credit
    txdb, # Deferred taxes
    itcb, # Investment tax credit
    pstkrv, # Preferred stock redemption value
    pstkl, # Preferred stock liquidating value
    pstk, # Preferred stock par value
    capx, # Capital investment
    oancf # Operating cash flow
  ) |>
  collect()

compustat <- 
  compustat |>
  mutate(
    be = coalesce(seq, ceq + pstk, at - lt) +
      coalesce(txditc, txdb + itcb, 0) -
      coalesce(pstkrv, pstkl, pstk, 0),
    be = if_else(be <= 0, as.numeric(NA), be)
  )

compustat <- 
  compustat |>
  mutate(year = year(datadate)) |>
  group_by(gvkey, year) |>
  filter(datadate == max(datadate)) |>
  ungroup()

# DuckDB ----
tidy_finance <- dbConnect(
  duckdb::duckdb(),
  "data/tidy_finance.duckdb",
  read_only = FALSE
)

dbWriteTable(tidy_finance,
             "compustat",
             value = compustat,
             overwrite = TRUE
)

dbDisconnect(tidy_finance, shutdown = TRUE)

# RDS ----
write_rds(compustat, "data/compustat.rds")
