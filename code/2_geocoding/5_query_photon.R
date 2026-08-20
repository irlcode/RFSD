library(fst)
library(filelock)
library(data.table)
library(stringi)
library(httr2)
library(furrr)
g <- glue::glue

n_workers <- floor(detectCores() * 2/3) # Set N cores for parallel computing

options(future.globals.maxSize = 2700*1024^2)
plan(multisession, workers = n_workers)

setwd("YOUR_DIR")

# Define the location of the lock files
lock_file <- "YOUR_DIR/write_document_lists.lck"

data_file <- "YOUR_DIR/geocoded_addresses_photon.csv"

# Define the location of the data file
# Load Nominatim results
nominatim_results <- fread("YOUR_DIR/geocoded_addresses_nominatim.csv", na.strings = "")

# Load Pullenti normalized addresses
pullenti_addr_orig <- fread("YOUR_DIR/pullenti_addr_orig.csv", select = c("addr_id", "level", "address_cleaned"))
pullenti_addr_nominatim <- fread("YOUR_DIR/pullenti_addr_nominatim.csv", select = c("addr_id", "level", "address_cleaned"))

# Use only street and building parts of the normalized address:
# - neither of the geocoders was ever found to suggest results for wrong city, let alone region
# - local Pullenti Address SDK is not consistent in normalizing city/settlement/locality names
use_levels <- c(
                # "REGIONAREA", "REGIONCITY",
                # "SETTLEMENT", "CITY", "LOCALITY",
                "STREET",
                "BUILDING"
)

# Trans addresses to lower and remove duplicates
pullenti_addr_orig[, address_cleaned := stri_trans_tolower(address_cleaned)]
pullenti_addr_nominatim[, address_cleaned := stri_trans_tolower(address_cleaned)]

pullenti_addr_orig <- unique(pullenti_addr_orig[level %in% use_levels], by = c("addr_id", "level"))
pullenti_addr_nominatim <- unique(pullenti_addr_nominatim[level %in% use_levels], by = c("addr_id", "level"))

# Dcast to wide format (addr_id - STREET - BUILDING)
pullenti_addr_orig.street_building <- dcast(pullenti_addr_orig, addr_id ~ level, value.var = "address_cleaned")
pullenti_addr_nominatim.street_building <- dcast(pullenti_addr_nominatim, addr_id ~ level, value.var = "address_cleaned")

# Determine match quality
pullenti_addr_orig.street_building[pullenti_addr_nominatim.street_building, 
                                   on = c("addr_id", "STREET", "BUILDING"),
                                   match_level := "house"]
pullenti_addr_orig.street_building[pullenti_addr_nominatim.street_building, 
                                   on = c("addr_id", "STREET"),
                                   match_level := fcoalesce(match_level, "street")]
pullenti_addr_orig.street_building[, .N, match_level]

# Send addresses with quality lower than "house" to recoding by Photon
to_photon <- nominatim_results[!pullenti_addr_match_level.nominatim[match_level == "house"], on = "addr_id", .(addr_id, addr_string)]
to_photon[, chunk := cut(1:.N, breaks = n_workers, include.lowest = T, labels = F)]
to_photon_list <- split(to_photon, by = "chunk")

# Define columns of interest to return in the order of interes
extr_values_from_list <- function(resp_list) {
    resp_list <-
        list(region.photon = resp_list$features[[1]]$properties$state,
             city.photon = resp_list$features[[1]]$properties$city,
             street.photon = ifelse(resp_list$features[[1]]$properties$type == "street", 
                                    resp_list$features[[1]]$properties$name,  
                                    resp_list$features[[1]]$properties$street),
             house.photon = resp_list$features[[1]]$properties$housenumber,
             postalcode.photon = resp_list$features[[1]]$properties$postcode,
             type.photon = resp_list$features[[1]]$properties$type,
             lon = resp_list$features[[1]]$geometry$coordinates[[1]],
             lat = resp_list$features[[1]]$geometry$coordinates[[2]]
        )
    resp_list <- lapply(resp_list, function(x) ifelse(is.null(x), NA, x))
    resp_list[2:4] <- lapply(resp_list[2:4], function(x) stri_replace_all_regex(x, "[^А-ЯЁа-яё0-9/\\s\\.-]", ""))
    resp_list <- lapply(resp_list, function(x) stri_replace_all_regex(x, "\\s+", " "))

    resp_list
}

geocode <- function(query_list) {
    tryCatch({

        query <- req_retry(req_url_query(req_url_path(request(api_url), "/api"), !!!query_list), max_seconds = 5)

        resp <- req_perform(query)
        resp_body_json(resp)

        extr_values_from_list(resp_body_json(resp))

    }, error = function(e) {

         list(region.photon = NA_character_,
                             city.photon = NA_character_,
                             street.photon = NA_character_,
                             house.photon = NA_character_,
                             postalcode.photon = NA_character_,
                             type.photon = NA_character_,
                             lon = NA_character_,
                             lat = NA_character_
                        )


    })
}


# Declare API url with Photon
api_url <- "http://127.0.0.1:2322/"

# Geocode
future_walk(to_photon_list, ~ {
                for(i in 1:nrow(.x)) {

                    address <- .x[i]

                    url_params_street_house <- 
                        list(limit = 1,
                             q = address$addr_string                  
                        )

                    resp_list <- geocode(url_params_street_house)

                    out <- c(address, resp_list)

                    file.lock <- lock(lock_file)
                    fwrite(out, file = data_file, append = T, quote = T, eol = "\n", logical01 = T, qmethod = "escape", nThread = 1)
                    unlock(file.lock)

                }
    }, .progress = T)


# Load results
photon_results <- fread(data_file)

# Build address string for normalization
addr_string_cols <- c("region.photon", "city.photon", "street.photon", "house.photon")
photon_results[, (addr_string_cols) := lapply(.SD, stri_trans_tolower), .SDcols = addr_string_cols]
photon_results[, country.photon := fifelse(stri_detect_regex(region.photon, "крым$|крим$|^(запо|херс|донец|луган)"), 
                                           "украина", 
                                           fifelse(region.photon == "байконур", 
                                                   "казахстан", 
                                                   "россия"))]
addr_string_cols <- c("country.photon", addr_string_cols)
photon_results[, (addr_string_cols) := lapply(.SD, function(x) fifelse(is.na(x), "", x)), .SDcols = addr_string_cols]
photon_results[, addr_string.photon := stri_join(country.photon, 
                                                 region.photon, 
                                                 fifelse(city.photon == region.photon, "", city.photon), 
                                                 street.photon,
                                                 house.photon, 
                                                 sep = ", ")]
photon_results[, addr_string.photon := stri_replace_all_regex(addr_string.photon, "(, ){2,}", ", ")]
photon_results[, addr_string.photon := stri_replace_all_regex(addr_string.photon, ", $", "")]

# Save
fwrite(photon_results, data_file)

