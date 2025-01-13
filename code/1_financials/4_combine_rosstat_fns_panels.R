library(fst)
library(data.table)
setDTthreads(0)

# Load data ==============================================================================
rosstat_financials <- read_fst("temp/rosstat_financials_impFrNY.fst", as.data.table = T)
fns_financials <- read_fst("temp/fns_financials_impFrNY.fst", as.data.table = T)
fns_financials_20172018 <- fns_financials[year < 2019]

rosstat_financials_20172018_new_obs <- fns_financials_20172018[!rosstat_financials, on = c("inn", "year")]

rosstat_financials <- rbindlist(list(
                                     rosstat_financials,
                                     rosstat_financials_20172018_new_obs
                                                                          ),
                                use.names = T, fill = T
)


# Combine Rosstat and GIR BO (BFO) ==========================================================
combined_financials <- rbindlist(list(
                                      rosstat_financials,
                                      fns_financials[year >= 2019]
                                      ),
                                 fill = T, use.names = T

# Check
print(dcast(combined_financials[, .N, keyby = .(year, all_na)], year ~ paste0("all_na_", all_na)))
print(dcast(combined_financials[, .N, keyby = .(year, new_obs)], year ~ paste0("new_obs_", new_obs)))
print(dcast(combined_financials[, .N, keyby = .(new_obs, all_na)], new_obs ~ paste0("all_na_", all_na)))
print(dcast(combined_financials[, .N, keyby = .(simplified, all_na)], simplified ~ paste0("all_na_", all_na)))
print(combined_financials[, lapply(.SD, mean), .SDcols = patterns("imp_"), keyby = year])

# Negative lines values to positive =================================================

neg_lines <- paste0("line_", c(1320:1323, 
                               2120:2123,
                               2210:2213,
                               2220:2223,
                               2330:2333,
                               2350:2353,
                               2411,
                               3220:3227,
                               3320:3327,
                               4120:4129,
                               4220:4229,
                               4320:4329,
                               6310:6313,
                               6320:6326,
                               6330,
                               6350:6359,
                               6300
                               ))

# Not all the lines are present in data so we repack them as regex
neg_lines_pattern <- paste(neg_lines, collapse="|")
neg_lines_present <- grep(neg_lines_pattern, names(combined_financials), value = T)

combined_financials[, (neg_lines_present) := lapply(.SD, function(l) fifelse(l < 0, -l, l)), .SDcols = neg_lines_present]

# Save
setorderv(combined_financials, c("inn", "year"))
setcolorder(combined_financials, c("inn", "year", "okved", "okpo", "okopf", "okfs", "simplified", 
                                   "new_obs", "all_na", "imp_any_from_future", 
                                   sort(grep("line_", names(combined_financials), value = T))))

write_fst(combined_financials, "temp/combined_financials_impFrNY_negLinesCorr.fst")
