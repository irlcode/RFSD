library(data.table)
library(stringi)
library(stringr)
library(httr2)
library(jsonlite)
library(furrr)
options(future.globals.maxSize = 2700*1024^2)
plan(multisession, workers = 10)
library(filelock)

# Define temp dir
temp_dir <- file.path("temp", "geocoding")

# Define the location of the lock files
lock_file <- file.path(temp_dir, "write_document_lists.lck")

# Define the location of the data file
data_file <- file.path(temp_dir, "geocoded_addresses.csv")

# Define the location of the file to store the caseids that were processed
status_file <- file.path(temp_dir, "geocoded_addresses.status")

# Load the addresses
load(file.path(temp_dir, "addresses_to_geocode.rdata"))

# Expand abbreviations
addresses_to_geocode[, street_processed := gsub("пр-кт", "проспект", street, fixed = T) ]
addresses_to_geocode[, street_processed := gsub("ПР-КТ", "ПРОСПЕКТ", street_processed, fixed = T) ]

addresses_to_geocode[, street_house_processed := gsub("пр-кт", "проспект", street_house, fixed = T) ]
addresses_to_geocode[, street_house_processed := gsub("ПР-КТ", "ПРОСПЕКТ", street_house_processed, fixed = T) ]

# Define columns of interest to return in the order of interest
columns_of_interest <- c("postalcode", "region", "city", "street_house",
                         "street", "place_id", "osm_type", "osm_id", "lat",
                         "lon", "display_name", "place_rank", "category",
                         "type", "importance", "address.house_number",
                         "address.road", "address.suburb",
                         "address.city_district", "address.town",
                         "address.village", "address.city",
                         "address.municipality","address.state",
                         "address.ISO3166.2.lvl4", "address.region",
                         "address.postcode")

# Destructive data header write
#foolproof_fwrite(data.table(t(columns_of_interest)), file = data_file, append = F, quote = T, sep = ",",  eol = "\n", na = "", dec = ".", row.names = F, col.names = F, qmethod = c("escape"))

# Split data.table with addresses into lists by region
addresses_to_geocode_list <- split(addresses_to_geocode, by = "region")

# Declare API url with Nominatim
api_url <- "http://0.0.0.0:8080/"

void <- future_map(addresses_to_geocode_list, ~ {

	# Debug: addresses <- addresses_to_geocode_list[[3]][1:10000]
	addresses <- .x

	# Loop through items
	for(i in 1:nrow(addresses)) {

		# Debug: address <- addresses[1]
		address <- addresses[i]

		# Specify parameters with street and house
		url_params_street_house <- list(format = "jsonv2",
			limit = "1",
			addressdetails = "1",
			extratags = "0",
			country = ifelse(address$region %in% c("Донецкая область", "Запорожская область", "Херсонская область", "Луганская область"), "Украина", ifelse(address$region == "Байконур", "Казахстан", "Россия")), 
			# postalcode = address$postalcode, # We will not use postal codes as they degrade results
			state = address$region,
			city = address$city,
			street = address$street_house_processed,
			countrycodes = "RU,UA",
			dedupe = "1",
			accept_language = "ru")

		# Remove empty parameters
		url_params_street_house <- url_params_street_house[!is.na(url_params_street_house)]

		# First query with street and house and postal code specified
		query_street_house <- req_retry(req_url_query(req_url_path(request(api_url), "/search"), !!!url_params_street_house), max_seconds = 5)

		resp_street_house <- req_perform(query_street_house)

		res <- as.data.table(resp_body_json(resp_street_house, simplifyVector = T))

		# If street and house geocoding failed,
		# perform on street only
		if( nrow(res) == 0 ) {

			url_params_street <- url_params_street_house
			url_params_street$street <- address$street_processed

			url_params_street <- url_params_street[!is.na(url_params_street)]

			query_street <- req_retry(req_url_query(req_url_path(request(api_url), "/search"), !!!url_params_street), max_seconds = 5)

			resp_street <- req_perform(query_street)

			res <- as.data.table(resp_body_json(resp_street, simplifyVector = T))

		}

		# If street geocoding failed,
		# perform on city
		if( nrow(res) == 0 ) {

			url_params_city <- url_params_street_house
			url_params_city$street <- NA

			url_params_city <- url_params_city[!is.na(url_params_city)]

			query_city <- req_retry(req_url_query(req_url_path(request(api_url), "/search"), !!!url_params_city), max_seconds = 5)

			resp_city <- req_perform(query_city)

			res <- as.data.table(resp_body_json(resp_city, simplifyVector = T))

		}

		# Export to CSV
		out <- address

		# Add geocoding results if successful
		if(nrow(res) > 0) {

			out <- cbind(out, res)

		}

		# Keep only the columns of interest in the order of interest
		names_outside_columns_of_interest <- columns_of_interest[!(columns_of_interest %in% names(out))]

		if( length(names_outside_columns_of_interest) > 0) {
			out[, c(names_outside_columns_of_interest) := NA_character_]
		}
		out <- out[, columns_of_interest, with = F]
				
		# Remove new lines
		variable_types <- sapply(out, class)
		character_variables <- names(variable_types[variable_types == "character"])
	
		out[, c(character_variables) := lapply(.SD, function(x) { gsub("\\s+", " ", x, perl = T) }), .SDcols = character_variables]
	
		# Double quotation marks to single
		out[, c(character_variables) := lapply(.SD, function(x) { gsub('"', "'", x, fixed = T) }), .SDcols = character_variables]
	
		# Empty lines to NA
		out[, names(out) := lapply(.SD, function(x) { ifelse(nchar(x) == 0, NA, x) }), .SDcols = names(out)]

		# Write to CSV
		file.lock <- lock(lock_file)
		fwrite(out, file = data_file, append = T, quote = T, eol = "\n", logical01 = T, qmethod = "escape", nThread = 1)
		unlock(file.lock)

	}


}, .progress = T, .options = furrr_options( globals = c("lock_file", "data_file", "status_file", "columns_of_interest", "api_url"), packages = c("data.table", "httr2", "jsonlite", "filelock"), scheduling = T))

# 10K entries is 5.3 MB
# 7M entries is 7e6/1e4= 700 times more
