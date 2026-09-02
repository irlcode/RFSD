library(fst)
library(data.table)

use_adj_or_orig <- read_fst(file.path("temp", "use_adj_or_orig.fst"), as.data.table = T)

financials_orig <- read_fst(file.path("temp", "combined_financials_impFrNY_negLinesCorr.fst"), as.data.table = T)
financials_fin <- financials_orig[use_adj_or_orig[use == "orig"], on = c("inn", "year")] 
rm(financials_orig); gc()

financials_adj <- read_fst(file.path("temp", "combined_financials_impFrNY_negLinesCorr_adjusted.fst"), as.data.table = T)
financials_fin <- rbindlist(list(financials_fin, financials_adj[use_adj_or_orig[use == "adj"], on = c("inn", "year")]), use.names = T, fill = T)

setorderv(financials_fin, c("inn", "year"))

write_fst(financials_fin, file.path("temp", "financials_fin.fst"))
