library(data.table)

# Define temp dir
temp_dir <- file.path("temp", "geocoding")

# Load EGRUL addresses
egrul_address <- fread("/local/path/to/egrul_addresses_panel")

# Load their processed versions from 2_export_addresses.r
load(file.path(temp_dir, "egrul_addresses_for_nominatim.rdata"))

# Load geocoding results
egrul_addresses_geocoded <- fread(file.path(temp_dir, "geocoded_addresses.csv"), na.strings = "", colClasses = "character")

# Explore geocoding quality
## Share non-geocoded by region: we are lacking Baykonur, Donetsk, Lugansk, Kherson, Zaporozhie regions
#egrul_addresses_geocoded[, sum(is.na(place_rank))/.N, by = "region"]

# Assign geocoding quality
# https://nominatim.org/release-docs/latest/customize/Ranking/
# NB: anything below 18 is region-level
egrul_addresses_geocoded[, geocoding_quality := NA_character_ ]
egrul_addresses_geocoded[, place_rank := as.numeric(place_rank)]
egrul_addresses_geocoded[place_rank == 30, geocoding_quality := "house"]
egrul_addresses_geocoded[place_rank >= 26 & is.na(geocoding_quality), geocoding_quality := "street"]
egrul_addresses_geocoded[place_rank >= 12 & is.na(geocoding_quality), geocoding_quality := "city"]

# When city is unknown mark as missing
egrul_addresses_geocoded[ is.na(city) & !is.na(place_rank), geocoding_quality := NA_character_]

# Prepare for merge by applying the same data transformations
# as in 3_query_nominatim.r

character_variables <- c("postalcode", "region", "city", "street_house", "street")

## Remove new lines
egrul_addresses_for_nominatim[, c(character_variables) := lapply(.SD, function(x) { gsub("\\s+", " ", x, perl = T) }), .SDcols = character_variables]

## Double quotation marks to single
egrul_addresses_for_nominatim[, c(character_variables) := lapply(.SD, function(x) { gsub('"', "'", x, fixed = T) }), .SDcols = character_variables]

## Empty lines to NA
egrul_addresses_for_nominatim[, c(character_variables) := lapply(.SD, function(x) { ifelse(nchar(x) == 0, NA, x) }), .SDcols = character_variables]

# Perform the merge with unique addresses
egrul_addresses_for_nominatim <- merge(egrul_addresses_for_nominatim, unique(egrul_addresses_geocoded[!is.na(geocoding_quality), c(character_variables, "geocoding_quality", "lon", "lat"), with = F], by = character_variables), by = character_variables, all.x = T, all.y = F)

# Perform the INN-year coordinates mapping
merge_columns <- c("fias_index", "region_code", "region_name", "fias_region_code", "fias_region_name", 
"fias_district_name", "fias_cityvillage_name", "fias_settlement_name", 
"fias_street_name", "fias_street_type", "fias_house", "kladr_index", 
"kladr_region_code", "kladr_region_name", "kladr_city_name", 
"kladr_village_name", "kladr_street_name", "kladr_street_type", 
"kladr_house")

egrul_inn_ogrn_geocoded <- merge(egrul_address[, c("inn", "ogrn", "datedump", merge_columns), with = F], egrul_addresses_for_nominatim[, c(merge_columns, "geocoding_quality", "lon", "lat"), with = F], all.x = T, all.y = F)
egrul_inn_ogrn_geocoded[, c(merge_columns) := NULL, with = F]
gc()

# Assess quality
yearly_quality <- egrul_inn_ogrn_geocoded[, .N, by = c("geocoding_quality", "datedump")]
setorderv(yearly_quality, c("datedump", "geocoding_quality"))

# Sizeable drop in quality in 2023 (NAs are due to the new regions, but house-level cannot be explained)
#29:              <NA> 2022-01-01   45661
#30:              city 2022-01-01 1133575
#31:             house 2022-01-01 5403276
#32:            street 2022-01-01 4831898
#33:              <NA> 2023-01-01   79375
#34:              city 2023-01-01 1562584
#35:             house 2023-01-01 1232063
#36:            street 2023-01-01 8820043

# Examples of the drop
#test1 <- egrul_inn_ogrn_geocoded[ datedump == "2022-01-01" & geocoding_quality == "house"]
#test2 <- egrul_inn_ogrn_geocoded[ datedump == "2023-01-01" & geocoding_quality == "street"]
#test1[ inn %in% test2[!is.na(inn)]$inn ]
#
#                inn          ogrn   datedump geocoding_quality
#      1: 1657008382 1041628200778 2022-01-01             house
#      2: 1657044990 1031625407120 2022-01-01             house
#      3: 1660008495 1031630221260 2022-01-01             house
#      4: 1655040310 1031622500271 2022-01-01             house
#      5: 1655026121 1031621008770 2022-01-01             house
#     ---                                                      
#4540609: 2508140756 1212500016046 2022-01-01             house
#4540610: 2508140435 1212500012823 2022-01-01             house
#4540611: 6501315411 1216500003763 2022-01-01             house
#4540612: 6500001300 1226500000132 2022-01-01             house
#4540613: 6504024957 1216500001915 2022-01-01             house
#
#egrul_inn_ogrn_geocoded[ datedump == "2022-01-01" & inn == "2508140756"]
#egrul_inn_ogrn_geocoded[ datedump == "2023-01-01" & inn == "2508140756"]
#
# weird case
#egrul_inn_ogrn_geocoded[ datedump == "2022-01-01" & inn == "1657008382"]
#egrul_inn_ogrn_geocoded[ datedump == "2023-01-01" & inn == "1657008382"]
#
#egrul_address[ datedump == "2022-01-01" & inn == "2508140435"]
#egrul_address[ datedump == "2023-01-01" & inn == "2508140435"]

# Save point
setkeyv(egrul_inn_ogrn_geocoded, c("inn", "ogrn", "datedump"))
save(egrul_inn_ogrn_geocoded, file = file.path(temp_dir, "egrul_inn_ogrn_geocoded.rdata"), compress = "gzip")
