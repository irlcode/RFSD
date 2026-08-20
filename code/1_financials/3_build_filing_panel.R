library(data.table)
library(fst)

rosstat <- read_fst(file.path("temp", "rosstat_financials.fst"), as.data.table = T)
firm_info <- c("year", "inn", "okved", "okpo", "okopf", "okfs", "simplified")
fin_vars_cur <- grep("line_.*?3$", names(rosstat), value = T)
rosstat <- rosstat[, c(firm_info, fin_vars_cur), with = F]
setnames(rosstat, fin_vars_cur, gsub("3$", "", fin_vars_cur))

fns <- read_fst(file.path("temp", "fns_financials.fst"), as.data.table = T)

filing_panel <- rbindlist(list(rosstat, fns), use.names = T, fill = T)
setorderv(filing_panel, c("inn", "year"))

filing_panel[, all_na := as.numeric(rowSums(!is.na(.SD)) == 0), .SDcols = patterns("line_")]

save_vars <- c(firm_info, "all_na") 
write_fst(filing_panel[, ..save_vars], file.path("temp", "filing_panel.fst"))
