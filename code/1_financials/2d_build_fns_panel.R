library(fst)
library(data.table)
setDTthreads(0)
source("financials/helpers/lines_tags_dict.R")

# Build panels from CSVs with parsing results
parsed_data_dir <- file.path("temp", "parsed_xml")

fns_financials_cur <-
    rbindlist(
              lapply(
                     dir(parsed_data_dir, pattern = "cur", full.names = T),
                     function(x) {
                         fread(x, colClasses = "character", na.strings = "", keepLeadingZeros = T)
                     }
                     ),
              fill = T, use.names = T)

# Use correction number to drop outdated statements that were not deleted from FNS base for some reason
fns_financials_cur[, corr := as.numeric(corr)]
setorderv(fns_financials_cur, c("inn", "year", "corr"))
fns_financials_cur <- unique(fns_financials_cur, by = c("inn", "year"), fromLast = T)
fns_financials_cur[, all_na := as.numeric(rowSums(!is.na(.SD)) == 0), .SDcols = patterns("line_")]
fns_financials_cur[all_na == 1, .N, keyby = year]

fns_financials_lag1 <-
                     dir(parsed_data_dir, pattern = "lag1", full.names = T),
                         fread(x, colClasses = "character", na.strings = "",  keepLeadingZeros = T)
fns_financials_lag1[, corr := as.numeric(corr)]
setorderv(fns_financials_lag1, c("inn", "year", "corr"))
fns_financials_lag1 <- unique(fns_financials_lag1, by = c("inn", "year"), fromLast = T)
fns_financials_lag1[, all_na := as.numeric(rowSums(!is.na(.SD)) == 0), .SDcols = patterns("line_")]
fns_financials_lag1 <- fns_financials_lag1[all_na == F]
fns_financials_lag1[, all_na := NULL]

fns_financials_lag2 <-
                     dir(parsed_data_dir, pattern = "lag2", full.names = T),
fns_financials_lag2[, corr := as.numeric(corr)]
setorderv(fns_financials_lag2, c("inn", "year", "corr"))
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

write_fst(fns_financials_cur, "data/fns/processed_fns_data_cur.fst")
write_fst(fns_financials_lag1, "data/fns/processed_fns_data_lag1.fst")
write_fst(fns_financials_lag2, "data/fns/processed_fns_data_lag2.fst")

# ================================================================================

# Shortcut
fns_financials_cur <- read_fst("data/fns/processed_fns_data_cur.fst", as.data.table = T)
fns_financials_lag1 <- read_fst("data/fns/processed_fns_data_lag1.fst", as.data.table = T)
fns_financials_lag2 <- read_fst("data/fns/processed_fns_data_lag2.fst", as.data.table = T)

lag1_lines_32xx <- fns_financials_lag1[, .SD, .SDcols=patterns("inn|year|line_32")]
lag1_lines_32xx[, year := year + 1]

# All the columns are in cur and prev and named identically except for changes in equity lines, see lines_tags_dict.R
# Rename columns in prev
setnames(fns_financials_lag1, names(changes_in_equity_lag1_tags), names(changes_in_equity_cur_tags))

fns_financials_new_obs <- fns_financials_lag1[!fns_financials_cur, on = c("inn", "year")]
fns_financials_new_obs[, c("new_obs", "all_na", "imp_any_from_future") := 1]
fns_financials <- rbindlist(list(fns_financials_cur, fns_financials_new_obs), use.names = T, fill = T)

fns_financials_new_obs2 <- fns_financials_lag2[!fns_financials, on = c("inn", "year")]
fns_financials_new_obs2[, new_obs := 2]
fns_financials_new_obs2[, c("all_na", "imp_any_from_future") := 1]
fns_financials <- rbindlist(list(fns_financials, fns_financials_new_obs2), use.names = T, fill = T)

setnafill(fns_financials, cols = c("new_obs", "all_na", "imp_any_from_future"), fill = 0)

# Add lines 32xx to cur
fns_financials <- lag1_lines_32xx[fns_financials, on = c("inn", "year")]

# Mark simplifieds
fns_financials[knd == "0710099", simplified := 0]
fns_financials[knd == "0710096", simplified := 1]

# Check
print(dcast(fns_financials[, .N, keyby = .(year, all_na)], year ~ paste0("all_na_", all_na)))
print(dcast(fns_financials[, .N, keyby = .(year, new_obs)], year ~ paste0("new_obs_", new_obs)))
print(dcast(fns_financials[, .N, keyby = .(new_obs, all_na)], new_obs ~ paste0("all_na_", all_na)))
print(dcast(fns_financials[, .N, keyby = .(simplified, all_na)], simplified ~ paste0("all_na_", all_na)))
print(fns_financials[, lapply(.SD, mean), .SDcols = patterns("imp_"), keyby = year])

# Save
setorderv(fns_financials, c("inn", "year"))
setcolorder(fns_financials, c("inn", "year", "okved", "okpo", "okopf", "okfs", "simplified", 
                              "new_obs", "all_na", "imp_any_from_future", 
                              sort(grep("line_", names(fns_financials), value = T))))

write_fst(fns_financials, "temp/fns_financials_impFrNY.fst")
#
# pl_data <- combined_financials[, .N, .(month_year = floor_date(file_date, "month"), same_year = year == year(file_date))]
# ggplot(pl_data[month_year > ym(201912) & month_year < ym(202401)], aes(month_year, N, col = same_year)) + geom_line()
# ggplot(pl_data[month_year > ym(201912) & month_year < ym(202401) & same_year == T], aes(month_year, N)) +
    # geom_line() +
    # scale_x_date(date_breaks = "2 months", date_labels = "%y-%m") +
    # theme_minimal() +
    # geom_vline(xintercept = c(ym(202001), ym(202101), ym(202201), ym(202301)), linetype = "dotted")
# pl_data2 <- combined_financials[, .N, .(month_year = floor_date(file_date, "month"), corr = corr > 0)]
# ggplot(pl_data2[month_year > ym(201912) & month_year < ym(202401)], aes(month_year, N, col = corr)) +
