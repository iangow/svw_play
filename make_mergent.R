library(tidyverse)
library(dbplyr)
library(RSQLite)
library(RPostgres)
library(devtools)

wrds <- dbConnect(
  Postgres(),
  host = "wrds-pgdata.wharton.upenn.edu",
  dbname = "wrds",
  port = 9737,
  sslmode = "require",
  bigint = "integer")

tidy_finance <- dbConnect(
  duckdb::duckdb(),
  "data/tidy_finance.duckdb",
  read_only = FALSE
)

mergent <- tbl(
  wrds,
  in_schema("fisd", "fisd_mergedissue")
) |>
  filter(
    security_level == "SEN", # senior bonds
    slob == "N", # secured lease obligation
    is.na(security_pledge), # unsecured bonds
    asset_backed == "N", # not asset backed
    defeased == "N", # not defeased
    bond_type %in% c(
      "CDEB", # US Corporate Debentures
      "CMTN", # US Corporate MTN (Medium Term Note)
      "CMTZ", # US Corporate MTN Zero
      "CZ", # US Corporate Zero,
      "USBN" # US Corporate Bank Note
    ), 
    pay_in_kind != "Y", # not payable in kind
    yankee == "N", # no foreign issuer
    canadian == "N", # not Canadian
    foreign_currency == "N", # USD
    coupon_type %in% c(
      "F", # fixed coupon
      "Z" # zero coupon
    ), 
    is.na(fix_frequency),
    coupon_change_indicator == "N",
    interest_frequency %in% c(
      "0", # per year
      "1",
      "2",
      "4",
      "12"
    ),
    rule_144a == "N", # publicly traded
    private_placement == "N",
    defaulted == "N", # not defaulted
    is.na(filing_date),
    is.na(settlement),
    convertible == "N", # not convertible
    is.na(exchange),
    putable == "N", # not putable
    unit_deal == "N", # not issued with another security
    exchangeable == "N", # not exchangeable
    perpetual == "N", # not perpetual
    preferred_security == "N" # not preferred
  ) |> 
  select(
    complete_cusip, maturity,
    offering_amt, offering_date,
    dated_date, 
    interest_frequency, coupon,
    last_interest_date, 
    issue_id, issuer_id
  )

mergent_issuer <- 
  tbl(wrds, in_schema("fisd", "fisd_mergedissuer")) |>
  select(issuer_id, sic_code, country_domicile) 

mergent <- 
  mergent |>
  inner_join(mergent_issuer, by = "issuer_id") |>
  filter(country_domicile == "USA") |>
  select(-country_domicile)

copy_to(tidy_finance, mergent, "mergent",
        temporary = FALSE, overwrite = TRUE)

copy_to(tidy_finance, mergent_issuer, "mergent_issuer",
        temporary = FALSE, overwrite = TRUE)

dbDisconnect(wrds)
dbDisconnect(tidy_finance, shutdown = TRUE)