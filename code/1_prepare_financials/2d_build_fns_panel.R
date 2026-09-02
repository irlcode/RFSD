library(fst)
library(data.table)
# Load parsing dicts
sapply(dir("code/1_prepare_financials/helpers/parsing_dicts", full.names = T), source)

# Build panels from CSVs with parsing results
parsed_data_dir <- file.path("temp", "parsed")

fns_financials_cur <-
    rbindlist(
              lapply(
                     dir(parsed_data_dir, pattern = "cur", recursive = T, full.names = T),
                     function(x) {
                         fread(x, colClasses = "character", na.strings = "", keepLeadingZeros = T)
                     }
                     ),
              fill = T, use.names = T)

# Use correction number to drop outdated statements that were not deleted from FNS base for some reason
fns_financials_cur[, corr := as.numeric(corr)]
fns_financials_cur[, file_date := as.IDate(file_date, "%d.%m.%Y")]
setorderv(fns_financials_cur, c("inn", "year", "file_date", "corr"))
fns_financials_cur <- unique(fns_financials_cur, by = c("inn", "year"), fromLast = T)
fns_financials_cur[, all_na := as.numeric(rowSums(!is.na(.SD)) == 0), .SDcols = patterns("line_")]
fns_financials_cur[all_na == 1, .N, keyby = year]


fns_financials_lag1 <-
    rbindlist(
              lapply(
                     dir(parsed_data_dir, pattern = "lag1", recursive = T, full.names = T),
                     function(x) {
                         fread(x, colClasses = "character", na.strings = "",  keepLeadingZeros = T)
                     }
                     ),
              fill = T, use.names = T)
fns_financials_lag1[, corr := as.numeric(corr)]
fns_financials_lag1[, file_date := as.IDate(file_date, "%d.%m.%Y")]
setorderv(fns_financials_lag1, c("inn", "year", "file_date", "corr"))

fns_financials_lag1 <- unique(fns_financials_lag1, by = c("inn", "year"), fromLast = T)
fns_financials_lag1[, all_na := as.numeric(rowSums(!is.na(.SD)) == 0), .SDcols = patterns("line_")]
fns_financials_lag1 <- fns_financials_lag1[all_na == F]
fns_financials_lag1[, all_na := NULL]

fns_financials_lag2 <-
    rbindlist(
              lapply(
                     dir(parsed_data_dir, pattern = "lag2", recursive = T, full.names = T),
                     function(x) {
                         fread(x, colClasses = "character", na.strings = "",  keepLeadingZeros = T)
                     }
                     ),
              fill = T, use.names = T)
fns_financials_lag2[, corr := as.numeric(corr)]
fns_financials_lag2[, file_date := as.IDate(file_date, "%d.%m.%Y")]
setorderv(fns_financials_lag2, c("inn", "year", "file_date", "corr"))
fns_financials_lag2 <- unique(fns_financials_lag2, by = c("inn", "year"), fromLast = T)
fns_financials_lag2[, all_na := as.numeric(rowSums(!is.na(.SD)) == 0), .SDcols = patterns("line_")]
fns_financials_lag2 <- fns_financials_lag2[all_na == F]
fns_financials_lag2[, all_na := NULL]

# Character to numeric
for(dt in list(fns_financials_cur, fns_financials_lag1, fns_financials_lag2)) {
    line_cols <- grep("line_", names(dt), value = T)
    num_cols <- c(line_cols, "year")
    dt[, (num_cols) := lapply(.SD, as.numeric), .SDcols = num_cols]
    # All values to thousand rubles
    dt[okei == "383", lapply(.SD, function(x) x/1000), .SDcols = line_cols]
    dt[okei == "385", lapply(.SD, function(x) x*1000), .SDcols = line_cols]
    dt[, okei := NULL]
}

write_fst(fns_financials_cur, file.path("temp", "fns_financials.fst"))
write_fst(fns_financials_lag1, file.path("temp", "fns_financials_lag1.fst"))
write_fst(fns_financials_lag2, file.path("temp", "fns_financials_lag2.fst"))

# ================================================================================

# Shortcut
# fns_financials_cur <- read_fst(file.path("temp", "fns_financials.fst"), as.data.table = T)
# fns_financials_lag1 <- read_fst(file.path("temp", "fns_financials_lag1.fst"), as.data.table = T)
# fns_financials_lag2 <- read_fst(file.path("temp", "fns_financials_lag2.fst"), as.data.table = T)

# Reconstruct observations missing in _cur for year T from _lag1 for year T+1
fns_financials_new_obs <- fns_financials_lag1[!fns_financials_cur, on = c("inn", "year")]
fns_financials_new_obs[, c("new_obs", "all_na", "imp_any_from_future") := 1]
fns_financials <- rbindlist(list(fns_financials_cur, fns_financials_new_obs), use.names = T, fill = T)

fns_financials_new_obs2 <- fns_financials_lag2[!fns_financials, on = c("inn", "year")]
fns_financials_new_obs2[, new_obs := 2]
fns_financials_new_obs2[, c("all_na", "imp_any_from_future") := 1]
fns_financials <- rbindlist(list(fns_financials, fns_financials_new_obs2), use.names = T, fill = T)

setnafill(fns_financials, cols = c("new_obs", "all_na", "imp_any_from_future"), fill = 0)

# # Add lines 32xx to cur
# fns_financials <- lag1_lines_3xxx[fns_financials, on = c("inn", "year")]
# fns_financials <- lag2_lines_3xxx[fns_financials, on = c("inn", "year")]

# Mark simplifieds
fns_financials[knd == "0710099", simplified := 0]
fns_financials[knd == "0710096", simplified := 1]

# For the year 2025 and later, use fill-in lines to fill missings
fill_in_lines <- grep("fill_in", names(fns_financials), value = T)
parent_lines <- substr(fill_in_lines, start = 1, stop = 9)
for(l in parent_lines) {
    fns_financials[form_version %in% c("5.04", "5.10"), 
                   l := fcoalesce(l, l.fill_in), 
                   env = list(l = l, 
          # check_fills[, parent_fixed := fns_financials[form_version %in% c("5.04", "5.10"), sapply(.SD, \(x) mean(is.na(x))), .SDcols = parent_lines] ]
# check_fills[, .(line, parent - parent_fixed)]
                    l.fill_in = paste0(l, ".fill_in"))]
}
fns_financials[, (fill_in_lines) := NULL]

# Check
print(dcast(fns_financials[, .N, keyby = .(year, all_na)], year ~ paste0("all_na_", all_na)))
print(dcast(fns_financials[, .N, keyby = .(year, new_obs)], year ~ paste0("new_obs_", new_obs)))
print(fns_financials[, lapply(.SD, mean), .SDcols = patterns("imp_"), keyby = year])

# Save
setorderv(fns_financials, c("inn", "year"))
setcolorder(fns_financials, c("inn", "year", "okved", "okpo", "okopf", "okfs", "simplified", 
                              "new_obs", "all_na", "imp_any_from_future", 
                              sort(grep("line_", names(fns_financials), value = T))))

write_fst(fns_financials, file.path("temp", "fns_financials_impFrNY.fst"))

