library(fst)
library(data.table)
library(stringi)
g <- glue::glue

setwd("YOUR_DIR")

# Load addr_id-inn-year mapping
addr_id_inn_year_mapping <- read_fst("addr_id_inn_year_mapping.fst", as.data.table = T)

# Load nominatim results
nominatim_results <- fread("geocoded_addresses_nominatim.csv", na.strings = "")
# Load photon results
photon_results <- fread("geocoded_addresses_photon.csv", na.strings = "")

# Mark coders
nominatim_results[, coder := "nominatim"]
photon_results[, coder := "photon"]

# Load Pullenti nomralization results
pullenti_addr_orig <- fread("YOUR_DIR/pullenti_addr_orig.csv", select = c("addr_id", "level", "address_cleaned"))
pullenti_addr_nominatim <- fread("YOUR_DIR/pullenti_addr_nominatim.csv", select = c("addr_id", "level", "address_cleaned"))
pullenti_addr_photon <- fread("YOUR_DIR/pullenti_addr_photon.csv", select = c("addr_id", "level", "address_cleaned"))

# Use only street and building parts of the normalized address:
# - neither of the geocoders was ever found to suggest results for wrong city, let alone region
# - local Pullenti Address SDK is not consistent in normalizing city/settlement/locality names
use_levels <- c("STREET", "BUILDING")

# Trans addresses to lower and remove duplicates
pullenti_addr_orig[, address_cleaned := stri_trans_tolower(address_cleaned)]
pullenti_addr_nominatim[, address_cleaned := stri_trans_tolower(address_cleaned)]
pullenti_addr_photon[, address_cleaned := stri_trans_tolower(address_cleaned)]

pullenti_addr_orig <- unique(pullenti_addr_orig[level %in% use_levels], by = c("addr_id", "level"))
pullenti_addr_nominatim <- unique(pullenti_addr_nominatim[level %in% use_levels], by = c("addr_id", "level"))
pullenti_addr_photon <- unique(pullenti_addr_photon[level %in% use_levels], by = c("addr_id", "level"))

# Dcast to wide format (addr_id - STREET - BUILDING)
pullenti_addr_orig.street_building <- dcast(pullenti_addr_orig, addr_id ~ level, value.var = "address_cleaned")
pullenti_addr_nominatim.street_building <- dcast(pullenti_addr_nominatim, addr_id ~ level, value.var = "address_cleaned")
pullenti_addr_photon.street_building <- dcast(pullenti_addr_photon, addr_id ~ level, value.var = "address_cleaned")

# Matching: build a reference table pointing to coder with best result for a given addr_id
pullenti_addr_orig.street_building[, match_level := NA_character_]
pullenti_addr_orig.street_building[, coder := NA_character_]

## house-level
pullenti_addr_orig.street_building[pullenti_addr_nominatim.street_building, 
                                   on = c("addr_id", "STREET", "BUILDING"), 
                                   `:=`(match_level = "house", coder = "nominatim")]
pullenti_addr_orig.street_building[, .N, match_level] # house: 2889525
pullenti_addr_orig.street_building[pullenti_addr_photon.street_building, 
                                   on = c("addr_id", "STREET", "BUILDING"), 
                                   `:=`(match_level = fcoalesce(match_level, "house"),
                                        coder = fcoalesce(coder, "photon"))]
pullenti_addr_orig.street_building[, .N, match_level] # house: 3353450

## street-level
pullenti_addr_orig.street_building[pullenti_addr_nominatim.street_building, 
                                   on = c("addr_id", "STREET"), 
                                   `:=`(match_level = fcoalesce(match_level, "street"),
                                        coder = fcoalesce(coder, "nominatim"))]
pullenti_addr_orig.street_building[, .N, match_level] # house: 3353450, street: 946676
pullenti_addr_orig.street_building[pullenti_addr_photon.street_building, 
                                   on = c("addr_id", "STREET"), 
                                   `:=`(match_level = fcoalesce(match_level, "street"),
                                        coder = fcoalesce(coder, "photon"))]
pullenti_addr_orig.street_building[, .N, .(match_level, coder)] # house: 3353450, street: 1219383

## city-level
pullenti_addr_orig.street_building[nominatim_results[!is.na(lon)], 
                                   on = "addr_id", 
                                   `:=`(match_level = fcoalesce(match_level, "city"), 
                                        coder = fcoalesce(coder, "nominatim"))]
pullenti_addr_orig.street_building[photon_results[!is.na(lon)], 
                                   on = "addr_id", 
                                   `:=`(match_level = fcoalesce(match_level, "city"), 
                                        coder = fcoalesce(coder, "photon"))]
pullenti_addr_orig.street_building[, .N, .(match_level)] # house: 3353152, street: 1219378, city: 712204, NA: 35956

# Add coordinates to the reference table
pullenti_addr_orig.street_building[, c("lon", "lat") := NULL]
pullenti_addr_orig.street_building[nominatim_results[!is.na(lon)], 
                                   on = c("addr_id", "coder"), 
                                   `:=`(lon = i.lon, lat = i.lat)]
pullenti_addr_orig.street_building[photon_results[!is.na(lon)], 
                                   on = c("addr_id", "coder"), 
                                   `:=`(lon = i.lon, lat = i.lat)]

# Map coordinates to inn-year by addr_id
addr_id_inn_year_mapping[pullenti_addr_orig.street_building,
                         on = "addr_id",
                         `:=`(lon = i.lon, lat = i.lat, geocoding_quality = i.match_level)]
addr_id_inn_year_mapping[, .N, .(geocoding_quality, year)][, .(geocoding_quality, N / sum(N) * 100), by = year] |> dcast(year ~ geocoding_quality)

# Save
write_fst(addr_id_inn_year_mapping, addr_id_inn_year_mapping_file)



