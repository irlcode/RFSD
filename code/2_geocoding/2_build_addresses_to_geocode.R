library(data.table)
library(fst)
library(stringi)
g <- glue::glue

setwd("YOUR_DIR")

# Load the most recent EGRUL addresses panel (built outside of this project)
load("/your/path/to/egrul_address_panel")

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

egrul_address_panel[region_codes, on = "fias_region_code == kladr_region", `:=`(region = i.name_with_type, region_taxcode = i.tax_office)]
egrul_address_panel[region_codes, on = "kladr_region_code == kladr_region", `:=`(region = fcoalesce(region, i.name_with_type), region_taxcode = fcoalesce(region_taxcode, i.tax_office))]
egrul_address_panel[fias_region_code == "90", `:=`(region = "Запорожская область", region_taxcode = "9000")]
egrul_address_panel[fias_region_code == "93", `:=`(region = "Донецкая народная республика", region_taxcode = "9300")]
egrul_address_panel[fias_region_code == "94", `:=`(region = "Луганская народная республика", region_taxcode = "9400")]
egrul_address_panel[fias_region_code == "95", `:=`(region = "Херсонская область", region_taxcode = "9500")]
egrul_address_panel[kladr_region_code == "90", `:=`(region = fcoalesce(region, "Запорожская область"), region_taxcode = fcoalesce(region_taxcode, "9000"))]
egrul_address_panel[kladr_region_code == "93", `:=`(region = fcoalesce(region, "Донецкая народная республика"), region_taxcode = fcoalesce(region_taxcode, "9300"))]
egrul_address_panel[kladr_region_code == "94", `:=`(region = fcoalesce(region, "Луганская народная республика"), region_taxcode = fcoalesce(region_taxcode, "9400"))]
egrul_address_panel[kladr_region_code == "95", `:=`(region = fcoalesce(region, "Херсонская область"), region_taxcode = fcoalesce(region_taxcode, "9500"))]

#### Broken cases
egrul_address_panel[stri_detect_regex(kladr_region_name, "усть-ордынский", case_insensitive = T), region := "Иркутская область"]
egrul_address_panel[stri_detect_regex(kladr_region_name, "камчатский|корякский", case_insensitive = T), region := "Камчатский край"]
egrul_address_panel[stri_detect_regex(kladr_region_name, "красноярский|таймырский|эвенкийский", case_insensitive = T), region := "Красноярский край"]
egrul_address_panel[stri_detect_regex(kladr_region_name, "татарстан", case_insensitive = T), region := "Республика Татарстан"]
egrul_address_panel[stri_detect_regex(kladr_region_name, "мурманская", case_insensitive = T), region := "Мурманская область"]
egrul_address_panel[stri_detect_regex(kladr_region_name, "коми-пермяцкий", case_insensitive = T), region := "Пермский край"]
egrul_address_panel[stri_detect_regex(kladr_region_name, "тверская", case_insensitive = T), region := "Тверская область"]
egrul_address_panel[stri_detect_regex(kladr_region_name, "москва", case_insensitive = T), region := "Москва"]
egrul_address_panel[stri_detect_regex(kladr_region_name, "петербург", case_insensitive = T), region := "Санкт-Петербург"]
egrul_address_panel[stri_detect_regex(kladr_region_name, "забайкальский|агинский", case_insensitive = T), region := "Забайкальский край"]
egrul_address_panel[stri_detect_regex(kladr_region_name, "донецкая", case_insensitive = T), region := "Донецкая народная республика"]
egrul_address_panel[is.na(region), .N] # 0
# write_fst(egrul_address_panel[, .(inn, ogrn, year = as.numeric(stri_sub(datedump, to = 4)) - 1, region, region_taxcode)], "egrul_inn_ogrn_correct_region_mapping.fst")

# Only unique addresses
address_columns <- c("fias_index", "region_code", "region_name", "district_name", "fias_region_code", "fias_region_name", "fias_district_name", "fias_cityvillage_name", "fias_settlement_name", "fias_street_name", "fias_street_type", "fias_house", "kladr_index", "kladr_region_code", "kladr_region_name", "kladr_city_name", "kladr_village_name", "kladr_district_name", "kladr_street_name", "kladr_street_type", "kladr_house")
egrul_address_panel[, (address_columns) := lapply(.SD, function(x) fifelse(x %chin% c("", " ", "-", "–", "—"), NA_character_, x)), .SDcols = address_columns]
egrul_address_panel[, addr_id := .GRP, by = address_columns]
addr_id_mapping <- egrul_address_panel[, .(inn, ogrn, year = as.numeric(stri_sub(datedump, to = 4)) - 1, addr_id)]

# egrul_address_paneles_for_photon <- unique(egrul_address_panel, by = "addr_id")

# Unified address fields
## Postal code
egrul_address_panel[, postalcode := fcoalesce(fias_index, kladr_index)]

## City
egrul_address_panel[, city := NA_character_]
egrul_address_panel[region %in% c("Москва", "Байконур", "Севастополь", "Санкт-Петербург"), city := region]
egrul_address_panel[, city := fcoalesce(city, 
                                  fias_settlement_name,
                                  fias_cityvillage_name,
                                  kladr_city_name,
                                  kladr_village_name,
                                  fias_district_name,
                                  kladr_district_name,
                                  district_name)]
# egrul_address_panel[is.na(city), .N] # 3691

# Build address
egrul_address_panel[, street_type := fcoalesce(fias_street_type, kladr_street_type)]
egrul_address_panel[, street_name := fcoalesce(fias_street_name, kladr_street_name)]
egrul_address_panel[!(region %in% c("Москва", "Байконур", "Севастополь", "Санкт-Петербург")), street_name := fcoalesce(street_name, fias_cityvillage_name, kladr_village_name)]
egrul_address_panel[, house := fcoalesce(fias_house, kladr_house)]

## Substitute abbreviations in street_type
egrul_address_panel[, street_type := stri_replace_all_fixed(street_type, ".", "")]
egrul_address_panel[, street_type := stri_trans_toupper(street_type)]
egrul_address_panel[street_type == "УЛ", street_type := "УЛИЦА"]
egrul_address_panel[street_type == "ПР-КТ", street_type := "ПРОСПЕКТ"]
egrul_address_panel[street_type == "ПЕР", street_type := "ПЕРЕУЛОК"]
egrul_address_panel[street_type == "Ш", street_type := "ШОССЕ"]
egrul_address_panel[street_type == "ПР-Д", street_type := "ПРОЕЗД"]
egrul_address_panel[street_type == "МКР", street_type := "МИКРОРАЙОН"]
egrul_address_panel[street_type == "Б-Р", street_type := "БУЛЬВАР"]
egrul_address_panel[street_type == "ТЕР", street_type := "ТЕРРИТОРИЯ"]
egrul_address_panel[street_type == "ПЛ", street_type := "ПЛОЩАДЬ"]
egrul_address_panel[street_type == "НАБ", street_type := "НАБЕРЕЖНАЯ"]
egrul_address_panel[street_type == "КВ-Л", street_type := "КВАРТАЛ"]
egrul_address_panel[street_type == "ТЕР СНТ", street_type := "СНТ"]
egrul_address_panel[street_type == "САДОВОЕ НЕКОМ-Е ТОВАРИЩЕСТВО", street_type := "СНТ"]
egrul_address_panel[street_type == "ЛН", street_type := "ЛИНИЯ"]
egrul_address_panel[street_type == "П", street_type := "ПОСЕЛОК"]
egrul_address_panel[street_type == "КМ", street_type := "КИЛОМЕТР"]
egrul_address_panel[street_type == "ТУП", street_type := "ТУПИК"]
egrul_address_panel[street_type == "АЛ", street_type := "АЛЛЕЯ"]
egrul_address_panel[street_type == "ДОР", street_type := "ДОРОГА"]
egrul_address_panel[street_type == "Р-Н", street_type := "РАЙОН"]
egrul_address_panel[street_type == "ПЛ-КА", street_type := "ПЛОЩАДКА"]
egrul_address_panel[street_type == "СТ", street_type := "СТАНЦИЯ"]
egrul_address_panel[street_type == "Д", street_type := "ДЕРЕВНЯ"]
egrul_address_panel[street_type == "ДОР", street_type := "ДОРОГА"]

egrul_address_panel[, street_name := stri_replace_all_fixed(street_name, ".", " ")]
egrul_address_panel[, street_name := stri_replace_all_regex(street_name, "\\s+", " ")]
egrul_address_panel[, street_name := stri_trans_toupper(street_name)]
egrul_address_panel[, street_name := stri_replace_all_fixed(street_name, "Ё", "Е")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bУЛ\\b", "УЛИЦА")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bПР-КТ\\b","ПРОСПЕКТ")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bПЕР\\b","ПЕРЕУЛОК")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bШ\\b","ШОССЕ")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bПР-Д\\b", "ПРОЕЗД")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bМКР\\b", "МИКРОРАЙОН")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bБ-Р\\b", "БУЛЬВАР")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bТЕР\\b", "ТЕРРИТОРИЯ")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bПЛ\\b", "ПЛОЩАДЬ")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bНАБ\\b", "НАБЕРЕЖНАЯ")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bКВ-Л\\b", "КВАРТАЛ")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bТЕР СНТ\\b", "СНТ")]
egrul_address_panel[, street_name := stri_replace_first_fixed(street_name, "САДОВОЕ НЕКОМ-Е ТОВАРИЩЕСТВО", "СНТ")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bЛН\\b", "ЛИНИЯ")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bП\\b", "ПОСЕЛОК")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bКМ\\b", "КИЛОМЕТР")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bТУП\\b", "ТУПИК")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bАЛ\\b", "АЛЛЕЯ")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bДОР\\b", "ДОРОГА")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bР-Н\\b", "РАЙОН")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bПЛ-КА\\b", "ПЛОЩАДКА")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bСТ\\b", "СТАНЦИЯ")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bД\\b", "ДЕРЕВНЯ")]
egrul_address_panel[, street_name := stri_replace_first_regex(street_name, "\\bДОР\\b", "ДОРОГА")]

# Prepare address cols
addr_cols <- c("region", "city", "street_type", "street_name", "house")
egrul_address_panel[, (addr_cols) := lapply(.SD, function(x) fifelse(is.na(x), "", x)), .SDcols = addr_cols]
egrul_address_panel[, (addr_cols) := lapply(.SD, stri_trans_tolower), .SDcols = addr_cols]

# Only entries with at least one parameter known
egrul_address_panel <- egrul_address_panel[region != "" | city != ""]# & (!is.na(street_house) | !is.na(street))]
egrul_address_panel[, .N] # 10412749

# Create addr_id - inn - year mapping
egrul_address_panel[, addr_id := .GRP, by = addr_cols]
egrul_address_panel[, year := as.numeric(stri_sub(datedump, to = 4)) - 1]
write_fst(egrul_address_panel[, .(inn, ogrn, year, addr_id)], g("addr_id_inn_year_mapping.fst"))

# Unique addresses to geocode
addresses_to_geocode <- unique(egrul_address_panel[, .(addr_id, region, city, street_type, street_name, house)], by = "addr_id")
addresses_to_geocode[, .N] # 6899439

# Country
addresses_to_geocode[, country := fifelse(region %in% c("донецкая народная республика", 
                                                        "запорожская область",
                                                        "херсонская область", 
                                                        "луганская народная республика"), 
                                          "украина", 
                                          fifelse(region == "байконур", 
                                                  "казахстан", 
                                                  "россия"))]
addresses_to_geocode[, .N, country]

# Cleaning

## Street
addresses_to_geocode[, street := stri_trim_both(stri_join(street_type, street_name, sep = " "))]

## House
addresses_to_geocode[, house := stri_replace_first_regex(house, "^[а-я\\.\\s;,№-]*", "")]
addresses_to_geocode[, house := stri_replace_first_regex(house, "(\\d)\\s*(,|;|\\.)", "$1")]
addresses_to_geocode[, house := stri_replace_all_fixed(house, '"', '')]
addresses_to_geocode[, house := stri_replace_first_regex(house, "\\\\{1,}", "/")]
addresses_to_geocode[, house := stri_replace_first_regex(house, "лит(ера|\\.)\\s*", "")]
addresses_to_geocode[, house := stri_replace_first_regex(house, "(\\d)(-|\\s+)([а-я]\\b)", "$1$3")]
addresses_to_geocode[, house := stri_replace_first_regex(house, "(\\dк)[\\.\\/\\s-](\\d)", "$1$2")]
addresses_to_geocode[, house := stri_replace_first_regex(house, "\\s+", " ")]

addresses_to_geocode[, addr_string := stri_join(country, region, fifelse(city == region, "", city), street, house, sep = ", ")]
addresses_to_geocode[, addr_string := stri_replace_all_regex(addr_string, "(, ){2,}", ", ")]

## Street-house
addresses_to_geocode[, street_house := stri_join(street, house, sep = ", ")]
addresses_to_geocode[, street_house := stri_replace_all_regex(street_house, "(^, |, $)", "")]

# Save 
write_fst(addresses_to_geocode, "addresses_to_geocode.fst")
