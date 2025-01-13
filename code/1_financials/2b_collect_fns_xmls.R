library(httr2)
library(xml2)
library(data.table)
library(fst)
library(jsonlite)
library(stringi)

# You must have BFO_LOGIN and BFO_PASS in you .Renviron.
# Set them with usethis::edit_r_environ().
# Restart R session for the changes to take effect.

username = Sys.getenv("BFO_LOGIN")
password = Sys.getenv("BFO_PASS")

# Set user agent
useragent <- "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

output_dir <- file.path("temp", "xml")
broken_xmls_dir <- file.path("temp", "broken_xmls")
dir.create(output_dir, recursive = T, showWarnings = F)
dir.create(broken_xmls_dir, showWarnings = F)

# Function to get temporary access token
get_access_token <- function() {

    request("https://api-bo.nalog.ru") %>%
        req_url_path_append("oauth/token") %>%
        req_user_agent(useragent) %>%
        req_headers(Authorization = "Basic YXBpOjEyMzQ1Njc4OTA=") %>%
        req_body_form(username = username,
                      password = password,
                      grant_type = "password") %>%
        # req_timeout(100) %>%
        req_retry(max_tries = 33) %>%
        req_throttle(20 / 60) %>%
        req_perform() %>%
        resp_body_json()

}

# This function:
# - queries XMLs via API,
# - returns status of the query (not XMLs themselves!),
# - in case of successful query writes XML to disk
get_doc_xml <- function(doc_token, access_token, output_dir) {

    # This is not to overhaul the API
    # Sys.sleep(runif(n = 1, min = .5, max = 1))

    # Create a file to record broken XMLs' tokens to
    process_id <- Sys.getpid()
    broken_xmls_path <- file.path("temp", "broken_xmls", glue::glue("broken_xmls_pid{process_id}.csv"))

    # Query the file
    success <- tryCatch(

                        {
                            resp <- request("https://api-bo.nalog.ru") %>%
                                req_url_path_append("api/v1/files/") %>%
                                req_url_path_append(doc_token) %>%
                                req_user_agent(useragent) %>%
                                req_auth_bearer_token(access_token) %>%
                                req_timeout(10) %>%
                                req_retry(max_tries = 3) %>%
                                req_perform()

                            TRUE

                        }, error = function(e) {

                            # Pause to let the server rest a bit
                            # Sys.sleep(runif(n = 1, min = 1, max = 10))

                            message("failed to fetch ", doc_token)

                            FALSE

                        })
    # Write XML to disk
    if (success) {

        tryCatch(
                 {
                     doc_xml <- resp_body_xml(resp, check_type = F)
                     doc_name <- xml_attr(xml_find_first(doc_xml, "//Файл"), "ИдФайл")
                     doc_year <- xml_text(xml_find_all(xml_doc, "//Документ/@ОтчетГод"))
                     save_path <- file.path(output_dir, doc_year, paste0(doc_name, ".xml"))
                     write_xml(doc_xml, file = save_path)
                     message("saved to ", save_path, " | ", file.exists(save_path))
                 }, error = function(e) {
                     message(e)
                     # If the received XML is severly broken (write fails) record its token
                     fwrite(list(doc_token = doc_token),
                            broken_xmls_path,
                            append = file.exists(broken_xmls_path))

                 })
        TRUE # If the query has been successful return TRUE

    } else {
        FALSE # If the query failed return FALSE. The algo will try to re-query XMLs with FALSE status.

}}

# Load table with all docs' tokens
docs_data <-
    rbindlist(
              lapply(dir("input", pattern = "doc", full.names = T),
                     fread,
                     integer64 = "character", keepLeadingZeros = T,
                     select = c("inn", "fileName", "period", "token")))


# Remove already downloaded files from task
already_parsed <- dir(output_dir)
docs_data[, fileName := stringi::stri_trans_tolower(fileName)]
already_parsed <- stringi::stri_trans_tolower(already_parsed)
docs_tokens <- docs_data[fileName %notin% c(already_parsed)]$token

if(file.exists("temp/failed_tokens.csv")) {
    failed_tokens_log <- fread("temp/failed_tokens.csv", keepLeadingZeros = T)
    docs_tokens <- docs_tokens[docs_tokens %notin% failed_tokens_log[n_fails >= 3]$token]

message("N docs: ", length(docs_tokens))

# Fetch
prev_length_tokens <- -1
while (length(docs_tokens) > 0 & length(docs_tokens) != prev_length_tokens) {

    prev_length_tokens <- length(docs_tokens)

    access_token_info <- get_access_token()
    while(access_token_info$expires_in < 600) {Sys.sleep(ceiling(runif(n=1, 5, 60))); access_token_info <- get_access_token()}
    access_token <- access_token_info$access_token
    expiration_time <- Sys.time() + access_token_info$expires_in
    message(format(Sys.time(), "%Y-%m-%d %H:%M"), " -- received access token, expiration time: ", format(expiration_time, "%H:%M"))

    docs_tokens_chunk <- docs_tokens[1:min(ceiling(access_token_info$expires_in * 0.8), length(docs_tokens))]

    # Returns named vector of queries status, where names are XML tokens
    docs_collect_status <- sapply(docs_tokens_chunk,
                                  FUN = get_doc_xml, access_token, output_dir,
                                  USE.NAMES = T)

    # Remove successfully queried docs_tokens from docs_tokens vector and return to 'while (length(docs_tokens > 0))' test
    fetched_tokens <- names(docs_collect_status)[docs_collect_status == T]
    failed_tokens <- names(docs_collect_status)[docs_collect_status == F]
    docs_tokens <- docs_tokens[docs_tokens %notin% fetched_tokens]

    if(file.exists("temp/failed_tokens.csv")) {
        failed_tokens_log <- fread("temp/failed_tokens.csv", keepLeadingZeros = T)
        new_failed_tokens <- setdiff(failed_tokens, failed_tokens_log$token)
        failed_tokens_log[token %chin% failed_tokens, n_fails := n_fails + 1]
        if(length(new_failed_tokens) != 0) {
            failed_tokens_log <- rbindlist(list(failed_tokens_log, data.table(token = new_failed_tokens, n_fails = 1)))
        }
        fwrite(failed_tokens_log, "temp/failed_tokens.csv", append = F)
        docs_tokens <- docs_tokens[docs_tokens %notin% failed_tokens_log[n_fails >= 3]$tokens]

        fwrite(data.table(token = failed_tokens, n_fails = 1), append = F)
    }


    message(format(Sys.time(), "%Y-%m-%d %H:%M"), " -- collected ", stringi::stri_pad_left(sum(docs_collect_status), 3, pad = " "), " more | remaining: ", length(docs_tokens))


message("fetching complete")




