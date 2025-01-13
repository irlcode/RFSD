library(httr2)
library(data.table)

# You must have BFO_LOGIN and BFO_PASS in you .Renviron.
# Set them with usethis::edit_r_environ().
# Restart R session for the changes to take effect.
#
# Also add the following to your .Renviron:
# RUSSIAN_FIRMS_FINANCIAL_REPORTS = "D:\_datasets\russian_firms_financial_reports"
# (modify accordingly if you are to run the code from this repo in a different location)

username = Sys.getenv("BFO_LOGIN")
password = Sys.getenv("BFO_PASS")

# path = Sys.getenv("RUSSIAN_FIRMS_FINANCIAL_REPORTS")
# setwd(path)

start_year <- 2019
end_year <- 2023

# Set user agent
useragent <- "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

# Function to get temporary access token
get_access_token <- function() {

    request("https://api-bo.nalog.ru") %>%
        req_url_path_append("oauth/token") %>%
        req_user_agent(useragent) %>%
        req_headers(Authorization = "Basic YXBpOjEyMzQ1Njc4OTA=") %>%
        req_body_form(username = username,
                      password = password,
                      grant_type = "password") %>%
        req_timeout(100) %>%
        req_retry(max_tries = 10) %>%
        req_throttle(20 / 60) %>%
        req_perform() %>%
        resp_body_json()

}

request_docs_data <- function(year, page, access_token) {

    query <- list(period = year,
                  page = page,
                  size = 2000,
                  fileType = "BFO",
                  reportType = "BFO_TKS")

    resp <- request("https://api-bo.nalog.ru") %>%
        req_url_path_append("api/v1/files/") %>%
        req_auth_bearer_token(access_token) %>%
        req_url_query(!!!query) %>%


# Obtain the token
token_info <- get_access_token()
access_token <- token_info$access_token
expiration_time <- Sys.time() + token_info$expires_in - 2

for (yr in start_year:end_year) {

    # yr <- 2023 # Debug

    message("year: ", yr)

    dir.create(file.path("temp", "docs_lists"), recursive = T, showWarnings = F)
    file_path <- file.path("temp", "docs_lists", glue::glue("docs_list_{yr}.csv"))

    first_page <- request_docs_data(yr, 1, access_token)

    if (length(first_page$content) == 0) {
        message("there's no data for this year, skipping...\n")
        next
    }

    total_pages <- first_page$totalPages
    total_docs <- first_page$totalElements

    # Check if all doc IDs have been already fetched
    if(file.exists(file_path)) {
        if(fread(file_path)[, .N] == total_docs) {
            message("All the docs IDs have already been fetched.")
            next
        }

    message("total pages: ", total_pages )
    message("total docs: ", total_docs)

    first_page_content <- rbindlist(lapply(first_page$content, as.data.table))

    fwrite(first_page_content, file_path)

    for (p in 2:total_pages) {
        message(yr, ": page ", p, " / ", total_pages)
        
        if (difftime(expiration_time, Sys.time(), units = "secs") < 50 ) {
            message("refreshing token...")
            access_token_info <- get_access_token()
            while(access_token_info$expires_in < 600) {
                Sys.sleep(ceiling(runif(n=1, 5, 60))); access_token_info <- get_access_token()
            }
            access_token <- access_token_info$access_token
            expiration_time <- Sys.time() + access_token_info$expires_in
            message(format(Sys.time(), "%Y-%m-%d %H:%M"), " -- received access token, expiration time: ", format(expiration_time, "%H:%M"))

        page <- request_docs_data(yr, p, access_token)
        page_content <- rbindlist(lapply(page$content, as.data.table))
        fwrite(page_content, file_path, append = T)




