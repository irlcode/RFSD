library(data.table)

# Define articulation checks ===================================================
check_balance_full <- function(dt) {
    # conditions from https://www.consultant.ru/document/cons_doc_LAW_331182/c1d5b9d91f51ea899ca4683598039396417c1809/
    dt[, balance_fine_1100 := as.numeric(abs(line_1100 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = patterns("line_11[1-9]0")]
    dt[, balance_fine_1200 := as.numeric(abs(line_1200 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = patterns("line_12[1-6]0")]
    dt[, balance_fine_1300 := as.numeric(abs(line_1300 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = patterns("line_13[1-7]0")]
    dt[, balance_fine_1400 := as.numeric(abs(line_1400 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = patterns("line_14[1235]0")]
    dt[, balance_fine_1500 := as.numeric(abs(line_1500 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = patterns("line_15[1-5]0")]
    dt[, balance_fine_16001 := as.numeric(abs(line_1600 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = patterns("line_1[12]00")]
    dt[, balance_fine_16002 := as.numeric(abs(line_1600 - line_1700) <= 4)]
    dt[, balance_fine_1700 := as.numeric(abs(line_1700 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = patterns("line_1[345]00")]
    check_cols <- grep("balance_fine_", names(dt), value = T)
    setnafill(dt, cols = check_cols, fill = 1)
    dt[, balance_fine := as.numeric(rowSums(.SD) == length(check_cols)), .SDcols = check_cols]
}

check_balance_simple <- function(dt) {
    # conditions from https://www.consultant.ru/document/cons_doc_LAW_331182/cdec125e8b4ed592e4a36c18b4318ce3f304bbe3/
    dt[, balance_fine_16001 := as.numeric(abs(line_1600 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = c("line_1150", "line_1170", "line_1210", "line_1250", "line_1230")]
    dt[, balance_fine_16002 := as.numeric(abs(line_1600 - line_1700) <= 4)]
    dt[, balance_fine_1700 := as.numeric(abs(line_1700 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = c("line_1300", "line_1410", "line_1450", "line_1510", "line_1520", "line_1550")]
    check_cols <- grep("balance_fine_", names(dt), value = T)
    setnafill(dt, cols = check_cols, fill = 1)
    dt[, balance_fine := as.numeric(rowSums(.SD) == length(check_cols)), .SDcols = check_cols]

}

check_finres_full <- function(dt) {
    # conditions from https://www.consultant.ru/document/cons_doc_LAW_331182/dc2cbdd712256af48282919fd9edef3d1cec9f65/ 
    neg_lines <- c("line_2120", "line_2210", "line_2220", "line_2330", "line_2350")
    dt[, paste0(neg_lines, "_neg") := lapply(.SD, function(x) -x), .SDcols = neg_lines]
    dt[, finres_fine_2100 := as.numeric(abs(line_2100 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = c("line_2110", "line_2120_neg")]
    dt[, finres_fine_2200 := as.numeric(abs(line_2200 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = c("line_2100", "line_2210_neg", "line_2220_neg")]
    dt[, finres_fine_2300 := as.numeric(abs(line_2300 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = c("line_2200", "line_2310", "line_2320", "line_2330_neg", "line_2340", "line_2350_neg")]
    dt[, paste0(neg_lines, "_neg") := NULL]
    check_cols <- grep("finres_fine_", names(dt), value = T)
    setnafill(dt, cols = check_cols, fill = 1)
    dt[, finres_fine := as.numeric(rowSums(.SD, na.rm = T) == length(check_cols)), .SDcols = check_cols]
 return(dt)
}


check_finres_simple <- function(dt) {
    # conditions from https://www.consultant.ru/document/cons_doc_LAW_331182/ee42e3ade8001260d90e2aa589c9a4ec243c5a70/
    neg_lines <- c("line_2120", "line_2330", "line_2350", "line_2410")
    dt[, paste0(neg_lines, "_neg") := lapply(.SD, function(x) -x), .SDcols = neg_lines]
    dt[, finres_fine_2400 := as.numeric(abs(line_2400 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = c("line_2110", "line_2120_neg", "line_2330_neg", "line_2340", "line_2350_neg", "line_2410_neg")]
    dt[, paste0(neg_lines, "_neg") := NULL]
    setnafill(dt, cols = "finres_fine_2400", fill = 1)
    dt[, finres_fine := finres_fine_2400]
}


check_cashflow_full <- function(dt) {
    # conditions from https://www.consultant.ru/document/cons_doc_LAW_331182/6d9190c0349969d45dd1b3a781d9449ac74faf34/
    neg_lines <- c("line_4120", "line_4220", "line_4320")
    dt[, paste0(neg_lines, "_neg") := lapply(.SD, function(x) -x), .SDcols = neg_lines]

    dt[, cashflow_fine_4100 := as.numeric((line_4100 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = c("line_4110", "line_4120_neg")]
    dt[, cashflow_fine_4110 := as.numeric((line_4110 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = patterns("line_411[1234569x]")]
    dt[, cashflow_fine_4120 := as.numeric((line_4120 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = patterns("line_412[1234569x]")]
    dt[, cashflow_fine_4200 := as.numeric((line_4200 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = c("line_4210", "line_4220_neg")]
    dt[, cashflow_fine_4210 := as.numeric((line_4210 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = patterns("line_421[12345679x]")]
    dt[, cashflow_fine_4220 := as.numeric((line_4220 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = patterns("line_422[12345679x]")]
    dt[, cashflow_fine_4300 := as.numeric((line_4300 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = c("line_4310", "line_4320_neg")]
    dt[, cashflow_fine_4310 := as.numeric((line_4310 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = patterns("line_431[12345679x]")]
    dt[, cashflow_fine_4320 := as.numeric((line_4320 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = patterns("line_432[1234569x]")]
    dt[, cashflow_fine_4400 := as.numeric((line_4400 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = patterns("line_4[123]00")]
    dt[, cashflow_fine_4500 := as.numeric((line_4500 - rowSums(.SD, na.rm = T)) <= 4), .SDcols = c("line_4400", "line_4450", "line_4490")]
    dt[, paste0(neg_lines, "_neg") := NULL]
    check_cols <- grep("cashflow_fine_", names(dt), value = T)
    setnafill(dt, cols = check_cols, fill = 1)
    dt[, cashflow_fine := as.numeric(rowSums(.SD, na.rm = T) == length(check_cols)), .SDcols = check_cols]

}



