library(data.table)

# Create dir to store temp files
temp_dir <- file.path("temp", "geocoding")
dir.create(temp_dir, recursive = T, showWarnings = F)

# Load EGRUL addresses
egrul_address <- fread("/local/path/to/egrul_addresses_panel")

# Regions from https://github.com/hflabs/region
region_codes <- fread("https://raw.githubusercontent.com/hflabs/region/refs/heads/master/region.csv", colClasses = "character")
region_codes[name_with_type == "Кемеровская область - Кузбасс", name_with_type := "Кемеровская обл"]
region_codes[name_with_type == "Чувашская Республика - Чувашия", name_with_type := "Чувашская Респ"]
region_codes[name_with_type == "Респ Северная Осетия - Алания", name_with_type := "Респ Северная Осетия"]
region_codes[name_with_type == "Ханты-Мансийский Автономный округ - Югра", name_with_type := "Ханты-Мансийский АО"]
region_codes[name_with_type == "Республика Саха /Якутия/", name_with_type := "Республика Саха"]
region_codes[name_with_type == "Еврейская Аобл", name_with_type := "Еврейская автономная обл"]

region_codes[name_with_type == "г Москва", name_with_type := "Москва"]
region_codes[name_with_type == "г Байконур", name_with_type := "Байконур"]
region_codes[name_with_type == "г Севастополь", name_with_type := "Севастополь"]
region_codes[name_with_type == "г Санкт-Петербург", name_with_type := "Санкт-Петербург"]

region_codes[, name_with_type := gsub("^Респ ", "Республика ", name_with_type)]
region_codes[, name_with_type := gsub(" Респ$", " республика", name_with_type)]
region_codes[, name_with_type := gsub(" респ$", " республика", name_with_type)]
region_codes[, name_with_type := gsub(" обл$", " область", name_with_type)]
region_codes[, name_with_type := gsub(" АО$", " автономный округ", name_with_type)]

region_codes[, kladr_region := substr(kladr_id, 1, 2) ]
region_codes[, oktmo_region := substr(oktmo, 1, 2) ]
region_codes[, inn_region := substr(tax_office, 1, 2) ]

# region_code in egrul_addresses is kladr_region in region_codes
# fias_region_name in egrul_addresses is kladr_region in region_codes
# kladr_region_code in egrul_addresses is kladr_region in region_codes

# Only unique addresses
egrul_addresses_for_nominatim <- unique(egrul_address[, c("fias_index",
                                                          "region_code",
                                                          "region_name",
                                                          "fias_region_code",
                                                          "fias_region_name",
                                                          "fias_district_name",
                                                          "fias_cityvillage_name",
                                                          "fias_settlement_name",
                                                          "fias_street_name",
                                                          "fias_street_type",
                                                          "fias_house",
                                                          "kladr_index",
                                                          "kladr_region_code",
                                                          "kladr_region_name",
                                                          "kladr_city_name",
                                                          "kladr_village_name",
                                                          "kladr_street_name",
                                                          "kladr_street_type",
                                                          "kladr_house")])

# Make unified address fields
## Postal code
egrul_addresses_for_nominatim[, postalcode := NA_character_]
egrul_addresses_for_nominatim[!is.na(fias_index), postalcode := fias_index]
egrul_addresses_for_nominatim[is.na(postalcode) & !is.na(kladr_index), postalcode := kladr_index]

## Region/state
### We unify region names according to the codes
egrul_addresses_for_nominatim[, region := NA_character_]
egrul_addresses_for_nominatim[ !is.na(region_name), region := region_codes[ match(egrul_addresses_for_nominatim[!is.na(region_name)]$region_name, kladr_region)]$name_with_type ]
egrul_addresses_for_nominatim[ !is.na(fias_region_name) & is.na(region), region := region_codes[ match(egrul_addresses_for_nominatim[!is.na(fias_region_name) & is.na(region)]$fias_region_name, kladr_region)]$name_with_type ]
egrul_addresses_for_nominatim[ !is.na(kladr_region_code) & is.na(region), region := region_codes[ match(egrul_addresses_for_nominatim[!is.na(kladr_region_code) & is.na(region)]$kladr_region_code, kladr_region)]$name_with_type ]

egrul_addresses_for_nominatim[ toupper(region_code) == "ХЕРСОНСКАЯ ОБЛАСТЬ" & is.na(region), region := "Херсонская область"]
egrul_addresses_for_nominatim[ toupper(region_code) == "ЗАПОРОЖСКАЯ ОБЛАСТЬ" & is.na(region), region := "Запорожская область"]
egrul_addresses_for_nominatim[ toupper(region_code) == "ДОНЕЦКАЯ НАРОДНАЯ РЕСПУБЛИКА" & is.na(region), region := "Донецкая область"]
egrul_addresses_for_nominatim[ toupper(region_code) == "ЛУГАНСКАЯ НАРОДНАЯ РЕСПУБЛИКА" & is.na(region), region := "Луганская область"]

egrul_addresses_for_nominatim[ toupper(fias_region_code) == "ХЕРСОНСКАЯ ОБЛАСТЬ" & is.na(region), region := "Херсонская область"]
egrul_addresses_for_nominatim[ toupper(fias_region_code) == "ЗАПОРОЖСКАЯ ОБЛАСТЬ" & is.na(region), region := "Запорожская область"]
egrul_addresses_for_nominatim[ toupper(fias_region_code) == "ДОНЕЦКАЯ НАРОДНАЯ РЕСПУБЛИКА" & is.na(region), region := "Донецкая область"]
egrul_addresses_for_nominatim[ toupper(fias_region_code) == "ЛУГАНСКАЯ НАРОДНАЯ РЕСПУБЛИКА" & is.na(region), region := "Луганская область"]

egrul_addresses_for_nominatim[ toupper(kladr_region_name) == "УСТЬ-ОРДЫНСКИЙ БУРЯТСКИЙ" & is.na(region), region := "Иркутская область"]
egrul_addresses_for_nominatim[ toupper(kladr_region_name) == "ИРКУТСКАЯ ОБЛ УСТЬ-ОРДЫНСКИЙ БУРЯТСКИЙ" & is.na(region), region := "Иркутская область"]
egrul_addresses_for_nominatim[ toupper(kladr_region_name) == "КОРЯКСКИЙ" & is.na(region), region := "Камчатский край"]
egrul_addresses_for_nominatim[ toupper(kladr_region_name) == "ТАЙМЫРСКИЙ" & is.na(region), region := "Красноярский край"]
egrul_addresses_for_nominatim[ toupper(kladr_region_name) == "ЭВЕНКИЙСКИЙ" & is.na(region), region := "Красноярский край"]
egrul_addresses_for_nominatim[ toupper(kladr_region_name) == "КРАСНОЯРСКИЙ" & is.na(region), region := "Красноярский край"]
egrul_addresses_for_nominatim[ toupper(kladr_region_name) == "ТАТАРСТАН" & is.na(region), region := "Республика Татарстан"]
egrul_addresses_for_nominatim[ toupper(kladr_region_name) == "МУРМАНСКАЯ" & is.na(region), region := "Мурманская область"]
egrul_addresses_for_nominatim[ toupper(kladr_region_name) == "КОМИ-ПЕРМЯЦКИЙ" & is.na(region), region := "Пермский край"]
egrul_addresses_for_nominatim[ toupper(kladr_region_name) == "ТВЕРСКАЯ" & is.na(region), region := "Тверская область"]
egrul_addresses_for_nominatim[ toupper(kladr_region_name) == "ТВЕРСКАЯ" & is.na(region), region := "Тверская область"]
egrul_addresses_for_nominatim[ toupper(kladr_region_name) == "ЗАБАЙКАЛЬСКИЙ КРАЙ АГИНСКИЙ БУРЯТСКИЙ" & is.na(region), region := "Забайкальский край"]
egrul_addresses_for_nominatim[ toupper(kladr_region_name) == "МОСКВА" & is.na(region), region := "Москва"]
egrul_addresses_for_nominatim[ toupper(kladr_region_name) == "ТАЙМЫРСКИЙ (ДОЛГАНО-НЕНЕЦКИЙ)" & is.na(region), region := "Красноярский край"]
egrul_addresses_for_nominatim[ toupper(kladr_region_name) == "КАМЧАТСКИЙ" & is.na(region), region := "Камчатский край"]
egrul_addresses_for_nominatim[ toupper(kladr_region_name) == "ДОНЕЦКАЯ НАРОДНАЯ РЕСПУБЛИКА" & is.na(region), region := "Донецкая область"]

## City
egrul_addresses_for_nominatim[, city := NA_character_]
egrul_addresses_for_nominatim[!is.na(fias_settlement_name), city := fias_settlement_name]
egrul_addresses_for_nominatim[!is.na(fias_settlement_name) & is.na(city), city := fias_cityvillage_name]
egrul_addresses_for_nominatim[!is.na(kladr_city_name) & is.na(city), city := kladr_city_name]
egrul_addresses_for_nominatim[!is.na(kladr_village_name) & is.na(city), city := kladr_village_name]

## Fixes for federal cities
egrul_addresses_for_nominatim[region %in% c("Москва", "Байконур", "Севастополь", "Санкт-Петербург"), city := region]

## Create street-and-house address where possible, priotizing fias above kladr
egrul_addresses_for_nominatim[, street_house := NA_character_]
egrul_addresses_for_nominatim[!is.na(fias_street_type) & !is.na(fias_street_name) & !is.na(fias_house), street_house := paste0(fias_street_type, " ", fias_street_name, ", ", fias_house)]
egrul_addresses_for_nominatim[is.na(street_house) & !is.na(fias_street_name) & !is.na(fias_house), street_house := paste0(fias_street_name, ", ", fias_house)]

egrul_addresses_for_nominatim[is.na(street_house) & !is.na(kladr_street_type) & !is.na(kladr_street_name) & !is.na(kladr_house), street_house := paste0(kladr_street_type, " ", kladr_street_name, ", ", kladr_house)]
egrul_addresses_for_nominatim[is.na(street_house) & !is.na(kladr_street_name) & !is.na(kladr_house), street_house := paste0(kladr_street_name, ", ", kladr_house)]

egrul_addresses_for_nominatim[is.na(street_house) & !(region %in% c("Москва", "Байконур", "Севастополь", "Санкт-Петербург")) & !is.na(kladr_village_name) & !is.na(kladr_house), street_house := paste0(kladr_village_name, ", ", kladr_house)]

## Street only
egrul_addresses_for_nominatim[, street := NA_character_]
egrul_addresses_for_nominatim[!is.na(fias_street_type) & !is.na(fias_street_name), street := paste0(fias_street_type, " ", fias_street_name)]
egrul_addresses_for_nominatim[is.na(street) & !is.na(fias_street_name), street := paste0(fias_street_name)]

egrul_addresses_for_nominatim[is.na(street) & !is.na(kladr_street_type) & !is.na(kladr_street_name), street := paste0(kladr_street_type, " ", kladr_street_name)]
egrul_addresses_for_nominatim[is.na(street) & !is.na(kladr_street_name), street := paste0(kladr_street_name)]

egrul_addresses_for_nominatim[is.na(street) & !(region %in% c("Москва", "Байконур", "Севастополь", "Санкт-Петербург")) & !is.na(kladr_village_name), street_house := paste0(kladr_village_name)]

# Keep only entries with at least one parameter known
egrul_addresses_for_nominatim <- egrul_addresses_for_nominatim[!is.na(postalcode) | !is.na(region) | is.na(city) | is.na(street_house) | is.na(street)]

# Unique addresses to geocode
addresses_to_geocode <- unique(egrul_addresses_for_nominatim[, c("postalcode", "region", "city", "street_house", "street"), with = F])

# Save point
save(egrul_addresses_for_nominatim, file = file.path(temp_dir, "egrul_addresses_for_nominatim.rdata"), compress = "gzip")
save(addresses_to_geocode, file = file.path(temp_dir, "addresses_to_geocode.rdata"), compress = "gzip")
