library(fst)
library(data.table)
setDTthreads(0)

# Load data ==============================================================================
rosstat_financials <- read_fst("temp/rosstat_financials_impFrNY.fst", as.data.table = T)
fns_financials <- read_fst("temp/fns_financials_impFrNY.fst", as.data.table = T)

# Combine Rosstat and GIR BO (BFO) ==========================================================
combined_financials <- rbindlist(list(
                                      rosstat_financials,
                                      fns_financials[year < 2019][!rosstat_financials, on = c("inn", "year")],
                                      fns_financials[year >= 2019]
                                      ),
                                 fill = T, use.names = T
)
rm(rosstat_financials)
rm(fns_financials)
gc()

# Check
# print(dcast(combined_financials[, .N, keyby = .(year, all_na)], year ~ paste0("all_na_", all_na)))
# print(dcast(combined_financials[, .N, keyby = .(year, new_obs)], year ~ paste0("new_obs_", new_obs)))
# print(dcast(combined_financials[, .N, keyby = .(new_obs, all_na)], new_obs ~ paste0("all_na_", all_na)))
# print(dcast(combined_financials[, .N, keyby = .(simplified, all_na)], simplified ~ paste0("all_na_", all_na)))
# print(combined_financials[, lapply(.SD, mean), .SDcols = patterns("imp_"), keyby = year])

# Bracketed negative lines values to positive =================================================

neg_lines_full <- paste0("line_", c(
                                    1320:1323,
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
neg_lines_pattern_full <- paste(neg_lines_full, collapse="|")
neg_lines_present_full <- grep(neg_lines_pattern_full, names(combined_financials), value = T)
combined_financials[simplified == 0, (neg_lines_present_full) := lapply(.SD, function(l) fifelse(l > 0, -l, l)), .SDcols = neg_lines_present_full]

combined_financials[simplified == 0 & year < 2019 & line_2410 > 0, line_2410 := -line_2410]
combined_financials[simplified == 0 & year >= 2019 
                   & line_2410 > 0 
                   & (abs(line_2411) > line_2412 
                      | (!is.na(line_2411) & line_2411 != 0 & is.na(line_2412))),
                   line_2410 := -line_2410]

neg_lines_simple <- paste0("line_", c(
                                      2120,
                                      2330,
                                      2350,
                                      2410,
                                      3220:3227,
                                      3320:3327,
                                      4120:4129,
                                      4220:4229,
                                      4320:4329,
                                      6310,
                                      6320,
                                      6330,
                                      6350,
                                      6300
                                      ))

# Not all the lines are present in data so we repack them as regex
neg_lines_pattern_simple <- paste(neg_lines_simple, collapse="|")
neg_lines_present_simple <- grep(neg_lines_pattern_simple, names(combined_financials), value = T)
combined_financials[simplified == 1, (neg_lines_present_simple) := lapply(.SD, function(l) fifelse(l > 0, -l, l)), .SDcols = neg_lines_present_simple]

# Save
setorderv(combined_financials, c("inn", "year"))
setcolorder(combined_financials, c("inn", "year", "okved", "okpo", "okopf", "okfs", "simplified", 
                                   "new_obs", "all_na", "imp_any_from_future", 
                                   sort(grep("line_", names(combined_financials), value = T))))

write_fst(combined_financials, "temp/combined_financials_impFrNY_negLinesCorr.fst")
