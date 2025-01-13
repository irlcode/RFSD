library(data.table)
library(fst)

rosstat <- read_fst("data/rosstat/processed_rosstat_data_2012_2018.fst", as.data.table = T)
firm_info <- c("year", "inn", "okved", "okpo", "okopf", "okfs", "simplified")
fin_vars_cur <- grep("line_.*?3$", names(rosstat), value = T)
rosstat <- rosstat[, c(firm_info, fin_vars_cur), with = F]
setnames(rosstat, fin_vars_cur, gsub("3$", "", fin_vars_cur))

fns <- read_fst("data/fns/processed_fns_data_cur.fst", as.data.table = T)

filing_panel <- rbindlist(list(rosstat, fns), use.names = T, fill = T)
setorderv(filing_panel, c("inn", "year"))

filing_panel[, all_na := as.numeric(rowSums(!is.na(.SD)) == 0), .SDcols = patterns("line_")]

firm_info <- c(firm_info, "all_na") 
write_fst(filing_panel[, ..firm_info], "output/filing_panel.fst")
