library(fst)
library(data.table)
setDTthreads(0)

# Load data
russian_financials <- read_fst("temp/combined_financials_impFrNY_negLinesCorr.fst", as.data.table = T)
russian_financials[, adj_any := 0]
# russian_financials[inn == "0100000621", .SD, .SDcols = patterns("line_1")]

# Declare imputation function
impute <- function(dt, imp_target, lines_to_sum, flag_imputation = T) {
    regex <- paste(lines_to_sum, collapse = "|") 
    env <- list(imp_target = imp_target, imp_value = paste0(imp_target, "_imp"))

    dt[, useless := rowSums(is.na(.SD)) == length(lines_to_sum), .SDcols = patterns(regex) ]
    dt[useless == F, imp_value := rowSums(.SD, na.rm = T), .SDcols = patterns(regex), env = env]

    dt[, orig_value := imp_target, env = env]
    dt[is.na(imp_target), imp_target := imp_value, env = env]
    dt[!is.na(imp_target) & !is.na(imp_value), imp_target := fifelse(abs(imp_target - imp_value) > 4, imp_value, imp_target), env = env]
    
    if(flag_imputation == T) {
        dt[, adj_any := pmax(adj_any, imp_target != orig_value | (!is.na(imp_target) & is.na(orig_value)), na.rm = T), env = env]
    }

    dt[, imp_value := NULL, env = env]
    dt[, useless := NULL]
    dt[, orig_value := NULL]
    # return(dt)
}

# Function demo:
# ch <- data.table(a = c(1, 10, NA, 1), b = c(2, 2, 2, NA), adj_any = 0)
# impute(ch, "a", "b")
# ch
       # a     b adj_any
   # <num> <num>   <num>
# 1:     1     2       0
# 2:     2     2       1
# 3:     2     2       1
# 4:     1    NA       0


# Lower levels imputation: sum of XXX[1-9] lines ---------------------------
XXX0_lines_for_simple_imp <- c("line_3210", "line_3220", 
                               "line_3310", "line_3320", 
                               "line_3410", "line_3420",  
                               "line_4110", "line_4120",
                               "line_4210", "line_4220", 
                               "line_4310", "line_4320",
                               "line_6310", "line_6320")

for(imp_target in XXX0_lines_for_simple_imp) {
    regex <- paste0(stringi::stri_sub(imp_target, 1, -2), "[1-9x]") # e.g. "line_1230" > "line_123[1-9]"
    lines_to_sum <- grep(regex, names(russian_financials), value = T)
    # message(imp_target, " = ", paste(lines_to_sum, collapse = " + "))
    impute(russian_financials, imp_target, lines_to_sum)

# Higher level imputation: sum of XX[1-9]0 lines -----------------------------
XX00_lines_for_simple_imp <- c("line_1100", "line_1200", 
                               "line_1300", "line_1400",
                               "line_1500", "line_6200",
                               "line_6300") 

for(imp_target in XX00_lines_for_simple_imp) {
    regex <- paste0(stringi::stri_sub(imp_target, 1, -3), "[1-9]0") # e.g. "line_1200" > "line_12[1-9]0"

# Imputation using fomulas common to simplified and non-simplifieds ---------
russian_financials[, line_6300_neg := -line_6300]
impute(russian_financials, "line_6400", c("line_6100", "line_6200", "line_6300_neg"))

# Imputation using specific formulas for simplifieds and non-simplifieds ----
russian_financials_full <- russian_financials[simplified == 0]
russian_financials_simple <- russian_financials[simplified == 1]
rm(russian_financials)
gc()


## Full statements 
### Prepare some lines
for(l in c("line_2120", "line_2210", "line_2220", "line_2330", "line_2350", 
           "line_3220", "line_3320", "line_4120", "line_4220", "line_4320")) {
    russian_financials_full[, l_neg := -l, env = list(l = l, l_neg = paste0(l, "_neg"))]
### Impute
impute(russian_financials_full, "line_1600", c("line_1100", "line_1200"))
impute(russian_financials_full, "line_1700", c("line_1300", "line_1400", "line_1500"))
impute(russian_financials_full, "line_2100", c("line_2110", "line_2120_neg"))
impute(russian_financials_full, "line_2200", c("line_2100", "line_2210_neg", "line_2220_neg"))
impute(russian_financials_full, "line_2300", c("line_2200", "line_2310", "line_2320", "line_2330_neg", "line_2340", "line_2350_neg"))



# Impute 24XX
russian_financials_full[, line_2410_neg := -line_2410]
impute(russian_financials_full[year >= 2020], "line_2410", c("line_2411", "line_2412"))
impute(russian_financials_full, "line_2400", c("line_2300", "line_2410_neg", "line_2460"))

## Construct 24XX lines with the same meaning across different periods
russian_financials_full[, line_2410_uniform_tax := NA_real_]
russian_financials_full[, line_2411_uniform_tax := NA_real_]
russian_financials_full[, line_2412_uniform_tax := NA_real_]
russian_financials_full[, line_2400_uniform_tax := NA_real_]
russian_financials_full[, line_2500_uniform_tax := NA_real_]

### Before 2018
russian_financials_full[year < 2018, line_2411_uniform_tax := line_2410]
impute(russian_financials_full[year < 2018], "line_2410_uniform_tax", c("line_2410_neg", "line_2430", "line_2450"), flag_imputation = F)
impute(russian_financials_full[year < 2020], "line_2412_uniform_tax", c("line_2430", "line_2450"), flag_imputation = F)
russian_financials_full[year < 2018, line_2460_neg := -line_2460] 
impute(russian_financials_full[year < 2018], "line_2400_uniform_tax", c("line_2300", "line_2410_uniform_tax", "line_2460_neg"), flag_imputation = F)
# impute(russian_financials_full[year == 2017], "line_2400_uniform_tax", c("line_2300", "line_2410_uniform_tax", "line_2460"), flag_imputation = F)
russian_financials_full[year == 2017, line_2400_uniform_tax_neg_2460 := line_2400]
impute(russian_financials_full[year == 2017], "line_2400_uniform_tax_neg_2460", c("line_2300", "line_2410_uniform_tax", "line_2460_neg"), flag_imputation = F)



### In 2018
russian_financials_full[year == 2018 & (!is.na(line_2411) | !is.na(line_2412)), line_2411_uniform_tax := line_2411]
russian_financials_full[year == 2018 & (!is.na(line_2411) | !is.na(line_2412)), line_2412_uniform_tax := line_2412]
russian_financials_full[year == 2018 & (!is.na(line_2411) | !is.na(line_2412)), line_2411_neg := -line_2411]
impute(russian_financials_full[year == 2018 & (!is.na(line_2411) | !is.na(line_2412))], "line_2410_uniform_tax", c("line_2411_neg", "line_2412"), flag_imputation = F)

russian_financials_full[year == 2018 & (is.na(line_2411) & is.na(line_2412)), line_2410_neg := -line_2410]
impute(russian_financials_full[year == 2018 & (is.na(line_2411) & is.na(line_2412))], "line_2410_uniform_tax", c("line_2410_neg", "line_2430", "line_2450"))
russian_financials_full[year == 2018 & (is.na(line_2411) & is.na(line_2412)), line_2411_uniform_tax := line_2410]
impute(russian_financials_full[year == 2018 & (is.na(line_2411) & is.na(line_2412))], "line_2412_uniform_tax", c("line_2430", "line_2450"), flag_imputation = F)
impute(russian_financials_full[year == 2018 & (is.na(line_2411) & is.na(line_2412))], "line_2400_uniform_tax", c("line_2300", "line_2410_uniform_tax", "line_2460"), flag_imputation = F)

### After 2018
impute(russian_financials_full[year > 2018], "line_2400_uniform_tax", c("line_2300", "line_2410", "line_2460"), flag_imputation = F)

# Impute 2500
impute(russian_financials_full[year < 2019], "line_2500", c("line_2400", "line_2510", "line_2520"))
impute(russian_financials_full[year < 2019], "line_2500_uniform_tax", c("line_2410_uniform_tax", "line_2510", "line_2520"), flag_imputation = F)
impute(russian_financials_full[year >= 2019], "line_2500", c("line_2410", "line_2510", "line_2520", "line_2530"))
impute(russian_financials_full[year >= 2019], "line_2500_uniform_tax", c("line_2410_uniform_tax", "line_2510", "line_2520", "line_2530"), flag_imputation = F)

# Impute 3XXX and 4XXX
impute(russian_financials_full, "line_3230", c("line_3210", "line_3220_neg"))
impute(russian_financials_full, "line_3200", c("line_3100", "line_3210", "line_3220_neg")) 
impute(russian_financials_full, "line_3300", c("line_3200", "line_3310", "line_3320_neg"))
# line_3400?
impute(russian_financials_full, "line_3500", c("line_3400", "line_3410", "line_3420"))
impute(russian_financials_full, "line_3600", c("line_1300"))
impute(russian_financials_full, "line_4100", c("line_4110", "line_4120_neg"))
impute(russian_financials_full, "line_4200", c("line_4210", "line_4220_neg"))
impute(russian_financials_full, "line_4300", c("line_4310", "line_4320_neg"))
impute(russian_financials_full, "line_4400", c("line_4100", "line_4200", "line_4300"))
impute(russian_financials_full, "line_4500", c("line_4400", "line_4450", "line_4490"))

## Simplified statements
for(l in c("line_2120", "line_2330", "line_2350", "line_2410")) {
    russian_financials_simple[, l_neg := -l, env = list(l = l, l_neg = paste0(l, "_neg"))]
impute(russian_financials_simple, "line_1600", c("line_1150", "line_1170", "line_1210", "line_1250", "line_1230"))
impute(russian_financials_simple, "line_1700", c("line_1300", "line_1350", "line_1360", "line_1410", "line_1450", "line_1510", "line_1520", "line_1550"))
impute(russian_financials_simple, "line_2200", c("line_2110", "line_2120_neg"))
impute(russian_financials_simple, "line_2300", c("line_2200", "line_2330_neg", "line_2340", "line_2350_neg"))
impute(russian_financials_simple, "line_2400", c("line_2110", "line_2120_neg", "line_2330_neg", "line_2340", "line_2350_neg", "line_2410_neg"))
russian_financials_simple[, line_2500 := line_2400]
russian_financials_simple[, line_2500_uniform_tax := line_2500]


# Combine data 
russian_financials <- rbindlist(list(russian_financials_full, russian_financials_simple), use.names=T, fill=T)
lines_to_delete <- grep("\\d_neg", names(russian_financials), value = T)
russian_financials[, (lines_to_delete) := NULL]

print(russian_financials[, .(adj_any = mean(adj_any)), keyby = year])
print(russian_financials[, .N, keyby = year])

# Tidy up and save
setorderv(russian_financials, c("inn", "year"))
write_fst(russian_financials, "output/russian_financials_2011_2023_imp_adj.fst")



