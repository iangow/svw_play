library(tidyverse)
library(DBI)
# Packages (required)
library(dbplyr)

clean_enhanced_trace <- function(cusips, 
                                 connection, 
                                 start_date = as.Date("2002-01-01"), 
                                 end_date = today()) {
  
  nrow <- function(df) {
    df %>% count() %>% pull(n)
  }
  

  
  # Function checks ---------------------------------------------------------
  # Input parameters
  ## Cusips
  if(length(cusips) == 0 | any(is.na(cusips))) stop("Check cusips.")
  
  ## Dates
  if(!is.Date(start_date) | !is.Date(end_date)) stop("Dates needed")
  if(start_date < as.Date("2002-01-01")) stop("TRACE starts later.")
  if(end_date > today()) stop("TRACE does not predict the future.")
  if(start_date >= end_date) stop("Date conflict.")
  
  ## Connection
  if(!dbIsValid(connection)) stop("Connection issue.")
  
  # Enhanced Trace ----------------------------------------------------------
  # Main file
  trace_all <- tbl(connection, 
                   in_schema("trace", "trace_enhanced")) |> 
    semi_join(mergent_cusips, by = "cusip_id") |>
    filter(trd_exctn_dt >= start_date & trd_exctn_dt <= end_date) |> 
    select(cusip_id, msg_seq_nb, orig_msg_seq_nb,
           entrd_vol_qt, rptd_pr, yld_pt, rpt_side_cd, cntra_mp_id,
           trd_exctn_dt, trd_exctn_tm, trd_rpt_dt, trd_rpt_tm, 
           pr_trd_dt, trc_st, asof_cd, wis_fl, 
           days_to_sttl_ct, stlmnt_dt, spcl_trd_fl)
  
  # Enhanced Trace: Post 06-02-2012 -----------------------------------------
  # Trades (trc_st = T) and correction (trc_st = R)
  print("Creating trace_post_TR ...")
  trace_post_TR <- 
    trace_all |> 
    filter((trc_st == "T" | trc_st == "R"),
           trd_rpt_dt >= as.Date("2012-02-06"))
  
  # Cancellations (trc_st = X) and correction cancellations (trc_st = C)
  print("Creating trace_post_XC ...")
  trace_post_XC <- 
    trace_all |>
    filter((trc_st == "X" | trc_st == "C"),
           trd_rpt_dt >= as.Date("2012-02-06")) |>
    compute()
  
  # Cleaning corrected and cancelled trades
  print("Filtering trace_post_TR ...")
  trace_post_TR <- 
    trace_post_TR |>
    anti_join(trace_post_XC,
              by = c("cusip_id", "msg_seq_nb", "entrd_vol_qt", 
                     "rptd_pr", "rpt_side_cd", "cntra_mp_id", 
                     "trd_exctn_dt", "trd_exctn_tm")) |>
    compute()
  
  # Reversals (trc_st = Y)
  print("Creating trace_post_Y ...")
  trace_post_Y <- 
    trace_all |>
    filter(trc_st == "Y",
           trd_rpt_dt >= as.Date("2012-02-06")) |>
    compute()
  
  ## the msg_seq_nb of the main message
  print("Creating trace_post ...")
  trace_post <- 
    trace_post_TR |>
    anti_join(trace_post_Y,
              by = c("cusip_id", "msg_seq_nb" = "orig_msg_seq_nb", 
                     "entrd_vol_qt", "rptd_pr", "rpt_side_cd", 
                     "cntra_mp_id", "trd_exctn_dt", "trd_exctn_tm")) |>
    compute()
  
  # Enhanced TRACE: Pre 06-02-2012 ------------------------------------------
  # Cancelations (trc_st = C)
  print("Creating trace_pre_C ...")
  trace_pre_C <- 
    trace_all |>
    filter(trc_st == "C",
           trd_rpt_dt < as.Date("2012-02-06")) |>
    compute()
  
  # Trades w/o cancelations
  ## match the orig_msg_seq_nb of the C-message 
  ## to the msg_seq_nb of the main message
  print("Creating trace_pre_T ...")
  trace_pre_T <- 
    trace_all |>
    filter(trc_st == "T",
           trd_rpt_dt < as.Date("2012-02-06")) |>
    anti_join(trace_pre_C, 
              by = c("cusip_id", "msg_seq_nb" = "orig_msg_seq_nb", 
                     "entrd_vol_qt", "rptd_pr", "rpt_side_cd", 
                     "cntra_mp_id", "trd_exctn_dt", "trd_exctn_tm")) |>
    compute()
  
  # Corrections (trc_st = W) - W can also correct a previous W
  print("Creating trace_pre_W ...")
  trace_pre_W <- 
    trace_all |>
    filter(trc_st == "W",
           trd_rpt_dt < as.Date("2012-02-06")) |>
    compute()
  
  # Implement corrections in a loop
  ## Correction control
  correction_control <- nrow(trace_pre_W)
  correction_control_last <- nrow(trace_pre_W)
  
  ## Correction loop
  while(correction_control > 0) {
    cat("nrow(trace_pre_W):", correction_control, "\n")
    
    # Corrections that correct some msg
    trace_pre_W_correcting <- trace_pre_W |>
      semi_join(trace_pre_T, 
                by = c("cusip_id", "trd_exctn_dt",
                       "orig_msg_seq_nb" = "msg_seq_nb")) |>
      compute()
    
    # Corrections that do not correct some msg
    trace_pre_W <- trace_pre_W |>
      anti_join(trace_pre_T, 
                by = c("cusip_id", "trd_exctn_dt",
                       "orig_msg_seq_nb" = "msg_seq_nb")) |>
      compute()
    
    # Delete msgs that are corrected and add correction msgs
    trace_pre_T <- trace_pre_T |>
      anti_join(trace_pre_W_correcting, 
                by = c("cusip_id", "trd_exctn_dt",
                       "msg_seq_nb" = "orig_msg_seq_nb")) |>
      union_all(trace_pre_W_correcting) |>
      compute()
    
    # Escape if no corrections remain or they cannot be matched
    correction_control <- nrow(trace_pre_W)
    if(correction_control == correction_control_last) {
      correction_control <- 0 
    }
    correction_control_last <- nrow(trace_pre_W)
  }
  
  
  # Clean reversals
  ## Record reversals
  trace_pre_R <- trace_pre_T |>
    filter(asof_cd == 'R') |>
    group_by(cusip_id, trd_exctn_dt, entrd_vol_qt, 
             rptd_pr, rpt_side_cd, cntra_mp_id) |>
    window_order(trd_exctn_tm, trd_rpt_dt, trd_rpt_tm) |>
    mutate(seq = row_number()) |>
    ungroup()
  
  ## Remove reversals and the reversed trade
  trace_pre <- trace_pre_T |> 
    filter(is.na(asof_cd) | !(asof_cd %in% c('R', 'X', 'D'))) |> 
    group_by(cusip_id, trd_exctn_dt, entrd_vol_qt, 
             rptd_pr, rpt_side_cd, cntra_mp_id) |> 
    window_order(trd_exctn_tm, trd_rpt_dt, trd_rpt_tm) |> 
    mutate(seq = row_number()) |> 
    ungroup() |> 
    anti_join(trace_pre_R,
              by = c("cusip_id", "trd_exctn_dt", "entrd_vol_qt", 
                     "rptd_pr", "rpt_side_cd", "cntra_mp_id", "seq")) |> 
    select(-seq)
  
  
  # Agency trades -----------------------------------------------------------
  # Combine pre and post trades
  trace_clean <- trace_post |> 
    union_all(trace_pre)
  
  # Keep angency sells and unmatched agency buys
  ## Agency sells
  trace_agency_sells <- trace_clean |> 
    filter(cntra_mp_id == "D",
           rpt_side_cd == "S")
  
  # Agency buys that are unmatched
  trace_agency_buys_filtered <- trace_clean |> 
    filter(cntra_mp_id == "D",
           rpt_side_cd == "B") |> 
    anti_join(trace_agency_sells, 
              by = c("cusip_id", "trd_exctn_dt", 
                     "entrd_vol_qt", "rptd_pr"))
  
  # Agency clean
  trace_clean <- trace_clean |> 
    filter(cntra_mp_id == "C")  |> 
    union_all(trace_agency_sells) |> 
    union_all(trace_agency_buys_filtered) 
  
  
  # Additional Filters ------------------------------------------------------
  trace_add_filters <- trace_clean |> 
    mutate(days_to_sttl_ct2 = stlmnt_dt - trd_exctn_dt) |> 
    filter(is.na(days_to_sttl_ct) | as.numeric(days_to_sttl_ct) <= 7,
           is.na(days_to_sttl_ct2) | as.numeric(days_to_sttl_ct2) <= 7,
           wis_fl == "N",
           is.na(spcl_trd_fl) | spcl_trd_fl == "",
           is.na(asof_cd) | asof_cd == "")
  
  
  # Output ------------------------------------------------------------------
  # Only keep necessary columns
  trace_final <- 
    trace_add_filters |> 
    select(cusip_id, trd_exctn_dt, trd_exctn_tm, 
           rptd_pr, entrd_vol_qt, yld_pt, rpt_side_cd, cntra_mp_id) 
  
  # Return
  return(trace_final |> 
           arrange(cusip_id, trd_exctn_dt, trd_exctn_tm) %>%
           compute(name = "trace_all", temporary = FALSE))
}

wrds <- dbConnect(
  RPostgres::Postgres(),
  bigint = "integer")

dbExecute(wrds, "SET work_mem = '8GB'")

tidy_finance <- dbConnect(
  duckdb::duckdb(),
  "data/tidy_finance.duckdb",
  read_only = FALSE
)

mergent_cusips <- 
  tbl(tidy_finance, "mergent") |>
  select(cusip_id = complete_cusip) |>
  copy_to(wrds, df = _) |>
  compute(name = "mergent_cusips")
  
trace_enhanced_all <- 
  clean_enhanced_trace(cusips = mergent_cusips, connection = wrds)

dbWriteTable(
  conn = tidy_finance,
  name = "trace_enhanced_all",
  value = trace_enhanced_all,
  overwrite = ifelse(j == 1, TRUE, FALSE),
  append = ifelse(j != 1, TRUE, FALSE))

dbDisconnect(wrds)
dbDisconnect(tidy_finance, shutdown = TRUE)