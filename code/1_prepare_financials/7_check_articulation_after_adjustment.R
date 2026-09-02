library(fst)
library(data.table)
source("code/1_prepare_financials/helpers/check_articulation_functions.R")

# Load adjusted financials =======================================================
russian_financials_adj <- read_fst(file.path("temp", "combined_financials_impFrNY_negLinesCorr_adjusted.fst"), as.data.table = T)

# ================================================================================
russian_financials_adj_full <- russian_financials_adj[simplified == 0]
russian_financials_adj_simple <- russian_financials_adj[simplified == 1]
rm(russian_financials_adj)
gc()

## Full statements
check_balance_full(russian_financials_adj_full)
check_finres_full(russian_financials_adj_full)
check_cashflow_full(russian_financials_adj_full)
## Simplified statements
check_balance_simple(russian_financials_adj_simple)
check_finres_simple(russian_financials_adj_simple)

# Check articulation ===============================================================

russian_financials_adj_full[, articulation_adj := as.numeric(balance_check == 1 & finres_check == 1 & cashflow_check == 1)]
russian_financials_adj_simple[, articulation_adj := as.numeric(balance_check == 1 & finres_check == 1)]

articulation_adj <- rbindlist(list(russian_financials_adj_full[, .(inn, year, articulation_adj)], 
                                   russian_financials_adj_simple[, .(inn, year, articulation_adj)]), 
                              fill = T, use.names = T)
rm(russian_financials_adj_full, russian_financials_adj_simple); gc()

# Load articulation panel calculated on original values ============================
articulation_orig <- read_fst(file.path("output", "articulation", "articulation_panel.fst"), as.data.table = T)

# Mark cases where adjustment breaks articulation
use_adj_or_orig <- articulation_adj[articulation_orig, on = c("inn", "year"), .(inn, year, use = fifelse(articulation_adj == 0 & i.articulation == 1, "orig", "adj"))]
use_adj_or_orig[, .N, keyby = .(year, use)] |> dcast(year ~ use)

# Save =============================================================================
write_fst(use_adj_or_orig, file.path("temp", "use_adj_or_orig.fst"))
