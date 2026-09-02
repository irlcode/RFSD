library(httr2)
library(xml2)
library(data.table)
library(fst)
# library(jsonlite)
library(stringi)
library(pbapply)
pbo <- pboptions(type = "txt")

# You must have BFO_LOGIN and BFO_PASS in you .Renviron.
# Set them with usethis::edit_r_environ().
# Restart R session for the changes to take effect.

username = Sys.getenv("BFO_LOGIN")
password = Sys.getenv("BFO_PASS")

# Set user agent
useragent <- "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

fns_dir <- file.path("source", "girbo")
xml_docs_dir <- file.path(fns_dir, "xml_ids")
xml_output_dir <- file.path(fns_dir, "xml")
failed_tokens_path <- file.path(fns_dir, "failed_tokens.csv")
broken_xmls_dir <- file.path(fns_dir, "broken_xmls")
broken_xmls_path <- file.path(broken_xmls_dir, glue::glue("broken_xmls.csv"))

dir.create(xml_output_dir, recursive = T, showWarnings = F)
dir.create(broken_xmls_dir, recursive = T, showWarnings = F)

# ================================================================================

# Function to get temporary access token.
# It asks for token recursively until a token with lifespan >= 10 minutes is received.
# R has a built-in mechanism to prevent infinite recursion, but we set it explicitly for clarity:
options(expressions = 5000)
get_access_token <- function() {

    access_token_info <-        
        request("https://api-bo.nalog.gov.ru") %>%
        req_url_path_append("oauth/token") %>%
        req_user_agent(useragent) %>%
        req_headers(Authorization = "Basic YXBpOjEyMzQ1Njc4OTA=") %>%
        req_body_form(username = username,
                      password = password,
                      grant_type = "password") %>%
        req_retry(max_tries = 33) %>%
        req_throttle(20 / 60) %>%
        req_perform() %>%
        resp_body_json()

    if(access_token_info$expires_in < 600) {
        Sys.sleep(ceiling(runif(n=1, 5, 60)))
        access_token_info <- get_access_token()
    }

    return(access_token_info)

}



# This function:
# - queries XMLs via API,
# - returns status of the query (not XMLs themselves!),
# - in case of successful query writes XML to disk
get_doc_xml <- function(doc_token, access_token, xml_output_dir) {

        
    Sys.sleep(.1)

    # Query the file
    success <- tryCatch(

                        {
                            resp <- request("https://api-bo.nalog.gov.ru") %>%
                                req_url_path_append("api/v1/files/") %>%
                                req_url_path_append(doc_token) %>%
                                req_user_agent(useragent) %>%
                                req_auth_bearer_token(access_token) %>%
                                req_timeout(3) %>%
                                req_retry(max_tries = 3) %>%
                                req_perform()

                            TRUE

                        }, error = function(e) {

                            FALSE

                        })
    # Write XML to disk
    if (success) {

        tryCatch(
                 {
                     doc_xml <- resp_body_xml(resp, check_type = F)
                     doc_name <- xml_attr(xml_find_first(doc_xml, "//Файл"), "ИдФайл")
                     save_path <- file.path(xml_output_dir, paste0(doc_name, ".xml"))
                     write_xml(doc_xml, file = save_path)

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

start_year <- as.integer(format(Sys.Date(), "%Y")) - 5
end_year <- as.integer(format(Sys.Date(), "%Y")) - 1

# ================================================================================
# Load table with all docs' tokens
docs_data <-
    rbindlist(
              lapply(dir(xml_docs_dir, pattern = "docs_list.*.csv$", full.names = T),
                     fread,
                     integer64 = "character", keepLeadingZeros = T,
                     select = c("inn", "fileName", "period", "token")))
docs_data <- unique(docs_data, by = "token")
docs_data[, fileName := stringi::stri_trans_tolower(fileName)]
# Drop JPG files that we are unable to parse
docs_data <- docs_data[!stri_detect_regex(fileName, "\\.jpg$")]

# ================================================================================
# Fetch
for(p in start_year:end_year) {
    # debug
    # p <- 2024

    message("period: ", p)
    dir.create(file.path(xml_output_dir, p), recursive = T, showWarnings = F)
    message("N docs: ", length(docs_data[period == p]$token))

    # ================================================================================

    # parts <- cut(1:nrow(docs_data[period == p]), breaks = 3, labels = c("jetty", "stt", "local"))
    # docs_data_period <- docs_data[period == p][parts == "jetty"]
    # docs_data_period <- docs_data[period == p][parts == "stt"]
    # docs_data_period <- docs_data[period == p][parts == "local"]
    docs_data_period <- docs_data[period == p]

    # ================================================================================

    # Remove already downloaded files from task
    already_fetched <- dir(file.path(xml_output_dir, p))
    message("Fetched: ", length(already_fetched))
    already_fetched <- stringi::stri_trans_tolower(already_fetched)
    docs_period_tokens <- docs_data_period[fileName %notin% already_fetched]$token
    message("Remaining: ", length(docs_period_tokens))
    if(file.exists(failed_tokens_path)) {
        failed_tokens_log <- fread(failed_tokens_path, keepLeadingZeros = T)
        docs_period_tokens <- setdiff(docs_period_tokens, failed_tokens_log[n_fails >= 3]$token)
    }

   

    message("Fetching XMLs, will take hours. Access token is automatically refreshed every so often. Data is chunked to fit into token's lifespan. Progress bar shows progress only for the current chunk.")

    # Get access token
    access_token_info <- get_access_token()
    access_token <- access_token_info$access_token
    expiration_time <- Sys.time() + access_token_info$expires_in
    message(format(Sys.time(), "%Y-%m-%d %H:%M"), " -- received access token, expiration time: ", format(expiration_time, "%H:%M"))

    fails <- 0
    # Query XMLs until all are obtained or deemed unobtainable
    while(length(docs_period_tokens) > 0 & fails < 3) {

        remaining_time <- as.numeric(difftime(expiration_time, Sys.time(), units = "secs"))

        if(remaining_time < 60) {
            message(format(Sys.time(), "%Y-%m-%d %H:%M"), " -- refreshing access token...")
            access_token_info <- get_access_token()
            access_token <- access_token_info$access_token
            expiration_time <- Sys.time() + access_token_info$expires_in
            remaining_time <- as.numeric(difftime(expiration_time, Sys.time(), units = "secs"))
            message(format(Sys.time(), "%Y-%m-%d %H:%M"), " -- received access token, expiration time: ", format(expiration_time, "%H:%M"))
        }

        docs_period_tokens_chunk <- docs_period_tokens[1:min(ceiling(remaining_time * 3), length(docs_period_tokens))]

        # Returns named vector of queries status, where names are XML tokens
        docs_collect_status <- pbsapply(docs_period_tokens_chunk,
                                        FUN = get_doc_xml, 
                                        access_token = access_token, 
                                        xml_output_dir = file.path(xml_output_dir, p),
                                        USE.NAMES = T)

        if(sum(docs_collect_status) == 0) {
            message("Critical fail, pausing to recover for 30 minutes...")
            fails <- fails + 1
            Sys.sleep(30 * 60)
            next
        }

        # Remove successfully queried docs_period_tokens from docs_period_tokens vector and return to 'while (length(docs_period_tokens > 0))' test
        fetched_tokens <- names(docs_collect_status)[docs_collect_status == T]
        failed_tokens <- names(docs_collect_status)[docs_collect_status == F]
        docs_period_tokens <- setdiff(docs_period_tokens, fetched_tokens)
        
        # Check how many times the failed tokens have failed, add new ones to log, update on disk
        if(file.exists(failed_tokens_path)) {
            failed_tokens_log <- fread(failed_tokens_path, keepLeadingZeros = T)
            new_failed_tokens <- setdiff(failed_tokens, failed_tokens_log$token)
            failed_tokens_log[token %chin% failed_tokens, n_fails := n_fails + 1]
            if(length(new_failed_tokens) != 0) {
                failed_tokens_log <- rbindlist(list(failed_tokens_log, 
                                                    data.table(fileName = docs_data[token %in% new_failed_tokens]$fileName,
                                                               token = new_failed_tokens, 
                                                               period = p, 
                                                               n_fails = 1)))
            }
            fwrite(failed_tokens_log, failed_tokens_path, append = F)
            # Remove those that failed from obtain list
            docs_period_tokens <- setdiff(docs_period_tokens, failed_tokens_log[n_fails >= 3]$token)

        } else {
            if(length(failed_tokens) != 0) {
                fwrite(data.table(fileName = docs_data[token %in% failed_tokens]$fileName, 
                                  token = failed_tokens, 
                                  period = p, 
                                  n_fails = 1), 
                       failed_tokens_path,
                       append = F)
            }
        }


        message(format(Sys.time(), "%Y-%m-%d %H:%M"), 
                " -- period: ", p, 
                " | collected: ", stringi::stri_pad_left(sum(docs_collect_status), 3, pad = " "), 
                " | remaining: ", length(docs_period_tokens))

    }
}


