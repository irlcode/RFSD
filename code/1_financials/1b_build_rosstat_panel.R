library(fst)
library(data.table)
setDTthreads(6)

# Paths to source files
zipped_csv_paths <- dir("data/rosstat", pattern = ".zip", full.names = T)
structure_paths <- dir("data/rosstat", pattern = "structure", full.names = T)

# Unzip, fix encoding, set names and rbind together data for all years
rosstat_financials <- 
    rbindlist(
              mapply(zipped_csv_paths, structure_paths,
					 FUN = function(d, s) {
                         dt <- fread(cmd = glue::glue("unzip -p {d} | iconv -f CP1251 -t UTF-8"), # convert to UTF-8 before importing
                                     colClasses = "character", 
                                     header = F, 
                                     keepLeadingZeros = T,
                                     quote = "",
                                     sep = ";")
						 var_names <- fread(s)[["field name"]] # import column names from structure file
                         setnames(dt, c(var_names, "version_date")) # last column's name is erroneously missing in file structure CSVs
                         dt[, year := stringi::stri_match_first_regex(d, "\\d{4}")] # infer year from data file name
}), 
			  fill = T, use.names = T)

# Check results
rosstat_financials[, .N, keyby = year]

# Numeric values to numeric class
line_cols <- grep("line_", names(rosstat_financials), value = T)
num_cols <- c("year", "type", line_cols)
rosstat_financials[, (num_cols) := lapply(.SD, as.numeric), .SDcols = num_cols]

# All financials values to thousands of rubles (`measure` column holds OKEI code)
rosstat_financials[measure == "383", (line_cols) := lapply(.SD, function(x) x/1000), .SDcols = line_cols]
rosstat_financials[measure == "385", (line_cols) := lapply(.SD, function(x) x*1000), .SDcols = line_cols]
rosstat_financials[, measure := NULL]

# Report type (0 -- non-commercial organizations, 1 -- SME, 2 -- others) to binary marker
rosstat_financials[, simplified := fifelse(type < 2, 1, 0)]

# Replace 0s with NAs (since there is no way to distinguish true 0s from missings)
for(line_ in grep("line_", names(rosstat_financials), value = T)) {
    set(rosstat_financials, i = which(rosstat_financials[[line_]] == 0), j = line_, value = NA)
}

# Drop duplicates
rosstat_financials <- unique(rosstat_financials, by = c("inn", "year"))

# Save
write_fst(rosstat_financials, "data/rosstat/processed_rosstat_data_2012_2018.fst")

# ================================================================================

# Shortcut
rosstat_financials <- read_fst("data/rosstat/processed_rosstat_data_2012_2018.fst", as.data.table = T)

# Split tables into two parts: curent year (names end with "3") and previous year values (names end with "4")
fin_vars_cur <- grep("line_.*?3$", names(rosstat_financials), value = T)
fin_vars_lag1 <- grep("line_.*?4$", names(rosstat_financials), value = T)
firm_info <- c("okved", "okpo", "okopf", "okfs", "simplified")
rosstat_financials_cur <- rosstat_financials[, c("inn", "year", firm_info, fin_vars_cur), with = F]
rosstat_financials_lag1 <- rosstat_financials[, c("inn", "year", firm_info, fin_vars_lag1), with = F]

# Mark observations that have no information
rosstat_financials_cur[, all_na := as.numeric(rowSums(!is.na(.SD)) == 0), .SDcols = patterns("line_")]
rosstat_financials_lag1[, all_na := as.numeric(rowSums(!is.na(.SD)) == 0), .SDcols = patterns("line_")]

# Keep such observations in current year data, but drop in previous year data: they are no use for imputation
rosstat_financials_lag1 <- rosstat_financials_lag1[all_na == F]
rosstat_financials_lag1[, all_na := NULL]

# Clear some space
rm(rosstat_financials)
gc()

# Merge parts back together and impute missing values from next year's records

## Year minus one for previous financials
rosstat_financials_lag1[, year := year - 1]

setnames(rosstat_financials_cur, gsub("3$", "", names(rosstat_financials_cur)))
setnames(rosstat_financials_lag1, gsub("4$", "", names(rosstat_financials_lag1)))

# Statements that are absent in current year data, but can be recovered from the next year's previous values
rosstat_financials_new_obs <- rosstat_financials_lag1[!rosstat_financials_cur, on = c("inn", "year")]
rosstat_financials_new_obs[, c("new_obs", "imp_any_from_future") := 1]
rosstat_financials <- rbindlist(list(rosstat_financials_cur, rosstat_financials_new_obs), use.names = T, fill = T)

setnafill(rosstat_financials, cols = c("new_obs", "imp_any_from_future"), fill = 0)
setnafill(rosstat_financials, cols = "all_na", fill = 1)

# Check
print(dcast(rosstat_financials[, .N, keyby = .(year, all_na)], year ~ paste0("all_na_", all_na)))
print(dcast(rosstat_financials[, .N, keyby = .(year, new_obs)], year ~ paste0("new_obs_", new_obs)))
print(dcast(rosstat_financials[, .N, keyby = .(new_obs, all_na)], new_obs ~ paste0("all_na_", all_na)))
print(dcast(rosstat_financials[, .N, keyby = .(simplified, all_na)], simplified ~ paste0("all_na_", all_na)))
print(rosstat_financials[, lapply(.SD, mean), .SDcols = patterns("imp_"), keyby = year])

setorderv(rosstat_financials, c("inn", "year"))
setcolorder(rosstat_financials, c("inn", "year", firm_info, c("new_obs", "all_na", "imp_any_from_future"), sort(grep("line_", names(rosstat_financials), value = T))))
dir.create("temp")
write_fst(rosstat_financials, "temp/rosstat_financials_impFrNY.fst")
