library(fst)
library(data.table)
library(ggplot2)
source("code/1_prepare_financials/helpers/check_articulation_functions.R")

articulation_save_path <- file.path("temp", "output", "articulation")
dir.create(articulation_save_path, recursive = T, showWarnings = F)

# Load panel ===================================================================
russian_financials <- read_fst(file.path("temp", "combined_financials_impFrNY_negLinesCorr.fst"), as.data.table = T)
russian_financials[, articulation_basic := as.numeric(line_1600 == line_1700)]

# Separate checks =================================================================

rosneft <- russian_financials[inn == "7706107510"]
gazprom <- russian_financials[inn == "7736050003"]

check_balance_full(rosneft)
check_finres_full(rosneft)
check_cashflow_full(rosneft)
rosneft[, .SD, .SDcols = patterns("year|check")]

check_balance_full(gazprom)
check_finres_full(gazprom)
check_cashflow_full(gazprom)
gazprom[, .SD, .SDcols = patterns("year|check")]

# ================================================================================
russian_financials_full <- russian_financials[simplified == 0]
russian_financials_simple <- russian_financials[simplified == 1]
rm(russian_financials)
gc()

calc_check_failed_share <- function(dt, check_name) {
    rbindlist(
              sapply(grep(check_name, names(dt), value = T),
                     function(check_var) {
                         env <- list(check_var = check_var)
                         dt[, .(check_failed_share = 1 - mean(check_var)), by = year, env = env]
                     },
                     USE.NAMES = T, simplify = F),
              idcol = "check"
              )
}

plot_check_failed_share <- function(dt, check_name) {
    pl_data <- calc_check_failed_share(dt, check_name)
    pl_data_wide <- dcast(pl_data, year ~ check, value.var = "check_failed_share")
    if(pl_data_wide[, !all(check_name <= rowSums(.SD)), .SDcols = patterns("check_\\d+"), env = list(check_name = paste0(check_name, "_check"))]) {
        stop("Share of fails in final check is larger than sum shares of failns in individual checks")
    }

    setorder(pl_data, year)
    pl <-
        ggplot(pl_data, aes(year, check_failed_share)) +
        geom_col(fill = "red") +
        scale_x_continuous(breaks = min(pl_data$year):max(pl_data$year)) +
        ylim(c(0, 1)) +
        lemon::facet_rep_wrap(~check, repeat.tick.labels = "x") +
        theme_minimal() +
        theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust =1),
              panel.grid.minor = element_blank(),
              panel.grid.major.x = element_blank(),
              legend.position = "bottom") +
        labs(y = "Failed checks share", y = "")

    print(pl)
}



## Full statements
check_balance_full(russian_financials_full)
plot_check_failed_share(russian_financials_full, "balance")
ggsave(file.path(articulation_save_path, "balance_check_full_impFrNY.pdf"), width = 12, height = 7)

check_finres_full(russian_financials_full)
plot_check_failed_share(russian_financials_full, "finres")
ggsave(file.path(articulation_save_path, "finres_check_full_impFrNY.pdf"), width = 12, height = 7)

check_cashflow_full(russian_financials_full)
plot_check_failed_share(russian_financials_full, "cashflow")
ggsave(file.path(articulation_save_path, "cashflow_check_full_impFrNY.pdf"), width = 12, height = 7)

## Simplified statements
check_balance_simple(russian_financials_simple)
plot_check_failed_share(russian_financials_simple, "balance")
ggsave(file.path(articulation_save_path, "balance_check_simple_impFrNY.pdf"), width = 12, height = 7)

check_finres_simple(russian_financials_simple)
plot_check_failed_share(russian_financials_simple, "finres")
ggsave(file.path(articulation_save_path, "finres_check_simple_impFrNY.pdf"), width = 12, height = 7)

russian_financials_simple[, lapply(.SD, function(x) mean(is.na(x))), .SDcols = patterns("2400|2110|2120|2330|2350|2410"), keyby = year]

# Check articulation ===============================================================

russian_financials_full[, articulation := as.numeric(balance_check == 1 & finres_check == 1 & cashflow_check == 1)]
russian_financials_simple[, articulation := as.numeric(balance_check == 1 & finres_check == 1)]

russian_financials <- rbindlist(list(russian_financials_full, russian_financials_simple), fill = T, use.names = T)

# Save =============================================================================
setorderv(russian_financials, c("inn", "year"))
write_fst(russian_financials[, .(inn, year, new_obs, simplified, articulation, articulation_basic, imp_any_from_future)], 
          file.path(articulation_save_path, "articulation_panel.fst"))
