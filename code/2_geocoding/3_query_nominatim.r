library(data.table)
library(fst)
library(stringi)
library(httr2)
library(jsonlite)
library(furrr)
library(filelock)
g <- glue::glue

n_workers <- floor(parallel::detectCores() * 2/3) # Set N cores for parallel computing

options(future.globals.maxSize = 2700*1024^2)
plan(multisession, workers = n_workers)

setwd("YOUR_DIR")

# Define the location of the lock files
lock_file <- "YOUR_DIR/write_document_lists.lck"

# Define the location of the data file
data_file <- "YOUR_DIR/geocoded_addresses_nominatim.csv"

# Load the addresses
addresses_to_geocode <- read_fst("addresses_to_geocode.fst", as.data.table = T)

if(file.exists(data_file)) {
    prev_run <- fread(data_file, select = "addr_id")
    address_to_geocode <- addresses_to_geocode[!prev_run, on = "addr_id"]
}

addresses_to_geocode[, chunk := cut(1:.N, breaks = n_workers, labels = F, include.lowest = T)]
addresses_to_geocode_list <- split(addresses_to_geocode[, .(addr_id, country, region, city, street, street_house, addr_string, chunk)], by = "chunk", keep.by = F)

# Define columns of interest to return in the order of interes
extr_values_from_list <- function(resp_list) {
    resp_list <- list(
                      addresstype.nominatim = resp_list[[1]]$addresstype,
                      region.nominatim = resp_list[[1]]$address$state,
                      city.nominatim = fcoalesce(lapply(c("city", "town", "municipality"),
                                                        function(x) {
                                                            v <- resp_list[[1]]$address[[x]]
                                                            if(is.null(v)) {
                                                                NA_character_
                                                            } else { v }
                                                        }
                                                  )
                      ),
                      street.nominatim = resp_list[[1]]$address$road,
                      house.nominatim = resp_list[[1]]$address$house_number,
                      postalcode.nominatim = resp_list[[1]]$address$postcode,
                      type.nominatim = resp_list[[1]]$type,
                      category.nominatim = resp_list[[1]]$category,
                      place_rank = resp_list[[1]]$place_rank,
                      lon = resp_list[[1]]$lon,
                      lat = resp_list[[1]]$lat
    )
    resp_list <- lapply(resp_list, function(x) ifelse(is.null(x), NA, x))
    resp_list[2:4] <- lapply(resp_list[2:4], function(x) stri_replace_all_regex(x, "[^А-ЯЁа-яё0-9/\\s\\.-]", ""))
    resp_list <- lapply(resp_list, function(x) stri_replace_all_regex(x, "\\s+", " "))

    resp_list
}

geocode <- function(query_list) {
    tryCatch({

        query <- req_retry(req_url_query(req_url_path(request(api_url), "/search"), !!!query_list), max_seconds = 5)

        resp <- req_perform(query)

        extr_values_from_list(resp_body_json(resp))

    }, error = function(e) {

        list(region.nominatim = NA_character_,
             addresstype.nominatim = NA_character_,
             city.nominatim = NA_character_,
             street.nominatim = NA_character_,
             house.nominatim = NA_character_,
             postalcode.nominatim = NA_character_,
             type.nominatim = NA_character_,
             category.nominatim = NA_character_,
             place_rank = NA_character_,
             lon = NA_character_,
             lat = NA_character_
        )

    })
}

# Declare API url with Nominatim
api_url <- "http://0.0.0.0:8080/"

future_walk(addresses_to_geocode_list, ~ {

                # Debug:
                # # addresses <- addresses_to_geocode_list[[3]][1:1YOUR_DIR0]
                addresses <- .x

                # Loop through items
                for(i in 1:nrow(addresses)) {

                    # Debug: address <- addresses[1]
                    address <- addresses[i]

                    # # Specify parameters with street and house
                    url_params <- list(format = "jsonv2",
                                       limit = "1",
                                       addressdetails = "1",
                                       extratags = "0",
                                       country = address$country,
                                       state = address$region,
                                       city = address$city,
                                       street = address$street_house,
                                       countrycodes = "RU,UA",
                                       dedupe = "1",
                                       accept_language = "ru")
                    url_params <- url_params[url_params != ""]
                    resp_list <- geocode(url_params)

                    # If street and house geocoding failed,
                    # perform on street only
                    if( all(is.na(resp_list)) ) {

                        url_params <- list(format = "jsonv2",
                                           limit = "1",
                                           addressdetails = "1",
                                           extratags = "0",
                                           country = address$country,
                                           state = address$region,
                                           city = address$city,
                                           street = address$street,
                                           countrycodes = "RU,UA",
                                           dedupe = "1",
                                           accept_language = "ru")

                        url_params <- url_params[url_params != ""]
                        resp_list <- geocode(url_params)
                    }

                    # If street geocoding failed,
                    # perform on city
                    if( all(is.na(resp_list)) ) {

                        url_params <- list(format = "jsonv2",
                                           limit = "1",
                                           addressdetails = "1",
                                           extratags = "0",
                                           country = address$country,
                                           state = address$region,
                                           city = address$city,
                                           countrycodes = "RU,UA",
                                           dedupe = "1",
                                           accept_language = "ru")

                        url_params <- url_params[url_params != ""]
                        resp_list <- geocode(url_params)

                    }

                    # Export to CSV
                    out <- c(address, resp_list)

                    # Remove new lines
                    variable_types <- sapply(out, class)
                    character_variables <- names(variable_types[variable_types == "character"])

                    # Write to CSV
                    file.lock <- lock(lock_file)
                    fwrite(out, file = data_file, append = T, quote = T, eol = "\n", logical01 = T, qmethod = "escape", nThread = 1)
                    unlock(file.lock)

                }
    }, .progress = T, .options = furrr_options( globals = c("lock_file", "data_file", "api_url", "extr_values_from_list", "geocode"), packages = c("data.table", "httr2", "jsonlite", "filelock", "stringi"), scheduling = T))
# # 10K entries is 5.3 MB
# # 7M entries is 7e6/1e4= 700 times more

# Load results and build address string to normalize
nominatim_results <- fread(data_file)
sapply(nominatim_results, function(x) mean(is.na(x)))
nominatim_results[, addr_string.nominatim := stri_join(country, region.nominatim, city.nominatim, street.nominatim, house.nominatim, sep = ", ")]
nominatim_results[, addr_string.nominatim := stri_replace_all_regex(addr_string.nominatim, "(, ){2,}", ", ")]
nominatim_results[, addr_string.nominatim := stri_replace_all_regex(addr_string.nominatim, ", $", "")]
fwrite(nominatim_results, data_file)
