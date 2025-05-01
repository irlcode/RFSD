library(data.table)
library(fst)

# Define temp dir
temp_dir <- file.path("temp", "geocoding")

# Load financials
financials <- read_fst("output/russian_financials_2011_2023.fst", as.data.table = T)

# Load geocoding results
load(file.path(temp_dir, "egrul_inn_ogrn_geocoded.rdata"))

# Merge
egrul_inn_ogrn_geocoded <- unique(egrul_inn_ogrn_geocoded[!is.na(inn), -c("ogrn", "datedump")], by = c("inn", "year"))
financials <- merge(financials, egrul_inn_ogrn_geocoded, by = c("inn", "year"), all.x = T, all.y = F)

# Save
write_fst(financials, "output/russian_financials_2011_2023.fst")
