# Function to extract value by XPath
extr_val <- function(doc, node_name, att) {

    if (!grepl("\\|", node_name)) { # Simple case

        val <- xml_text(xml_find_all(doc, paste0("//", node_name, "/@", att)))

    } else { # Complex case where xpath varies (variants are passed separated by `|`)

        node_names <- unlist(strsplit(node_name, "|", fixed = T), use.names = F)
        val <- xml_text(xml_find_all(doc, paste(paste0("//", node_names, "/@", att), collapse = "|")))
    }

    if (length(val) > 0) {

        val

    } else {

        NA

    }
}


# Gets values and writes to CSV
parse_xml <- function(xml_path, dir_year, temp_output_dir) {

    process_id <- Sys.getpid() # to use in file names so that every process writes in its own file

    # Current year's values

    cur_year_parsed <- tryCatch(
                                {
                                    xml_doc <- read_xml(xml_path)

                                    year <- as.numeric(xml_text(xml_find_all(xml_doc, "//Документ/@ОтчетГод")))

                                    firm_info <- list(
                                                      inn = extr_val(xml_doc, "НПЮЛ", "ИННЮЛ"),
                                                      okved = extr_val(xml_doc, "СвНП", "ОКВЭД2"),
                                                      okopf = extr_val(xml_doc, "СвНП", "ОКОПФ"),
                                                      okpo = extr_val(xml_doc, "СвНП", "ОКПО"),
                                                      okfs = extr_val(xml_doc, "СвНП", "ОКФС")
                                                      # simplified = as.numeric(grepl("BOUPR", file_name, ignore.case = T))
                                    )
                                    meta <- list(
                                                file_name =  xml_text(xml_find_all(xml_doc, "//Файл/@ИдФайл")),
                                                okei = xml_text(xml_find_all(xml_doc, "//Документ/@ОКЕИ")),
                                                knd = xml_text(xml_find_all(xml_doc, "//Документ/@КНД")),
                                                corr = xml_text(xml_find_all(xml_doc, "//Документ/@НомКорр")),
                                                file_date = xml_text(xml_find_all(xml_doc, "//Документ/@ДатаДок")),
                                                reported_period = xml_text(xml_find_all(xml_doc, "//Документ/@Период"))
                                                # must_audit = as.numeric(xml_text(xml_find_all(xml_doc, "//Документ/@ПрАудит")))
                                    )            

                                    balance <- lapply(balance_tags, FUN = extr_val, doc = xml_doc, att = "СумОтч")
                                    finres <- lapply(finres_tags, FUN = extr_val, doc = xml_doc, att = "СумОтч")
                                    equity <- lapply(lapply(changes_in_equity_cur_tags, FUN = extr_val, doc = xml_doc, att = "Итог"), function(x) as.character(sum(as.numeric(x))))
                                    equity_total_lag2 <- extr_val(doc = xml_doc, node_name = equity_lag2, att = "Итог")
                                    corrections <- lapply(correct_tags, FUN = extr_val, doc = xml_doc, att = "На31ДекПред")
                                    net_assets <- extr_val(doc = xml_doc, node_name = net_assets_tag, att = "На31ДекОтч")
                                    cashflow <- lapply(lapply(cashflow_tags, FUN = extr_val, doc = xml_doc, att = "СумОтч"), function(x) as.character(sum(as.numeric(x))))
                                    designated_use <- lapply(designated_use_tags, FUN = extr_val, doc = xml_doc, att = "СумОтч")

                                    row <- c(firm_info, 
                                             list(year = year),
                                             meta,
                                             balance, 
                                             finres, 
                                             equity, 
                                             corrections, 
                                             list(line_3600 = net_assets, line_3100 = equity_total_lag2), 
                                             cashflow, 
                                             designated_use)

                                    # Append new row to file on disk
                                    output_file <- glue::glue("{temp_output_dir}/{dir_year}_{process_id}_cur_result.csv")
                                    fwrite(as.data.table(row), output_file, append = file.exists(output_file))

                                    TRUE

                                },
                                error = function(e) {
                                    failed_xmls <- glue::glue("{temp_output_dir}/{dir_year}_corrupted_xmls_{process_id}.csv")
                                    fwrite(data.table(path = xml_path), failed_xmls, append = file.exists(failed_xmls))
                                    message("cur year")
                                    message(e$message)

                                    FALSE

                                }
    )

    # If current year's values have been extracted without error get previous year's values

    if (cur_year_parsed == T) {

        lag1_year_parsed <- tryCatch(
                                     {
                                         balance_lag1 <- as.list(
                                                                 pmax(
                                                                      sapply(balance_tags, FUN = extr_val, doc = xml_doc, att = "СумПред"),
                                                                      sapply(balance_tags, FUN = extr_val, doc = xml_doc, att = "СумПрдщ"),
                                                                      na.rm = T)
                                         )

                                         finres_lag1 <- as.list(
                                                                pmax(
                                                                     sapply(finres_tags, FUN = extr_val, doc = xml_doc, att = "СумПред"),
                                                                     sapply(finres_tags, FUN = extr_val, doc = xml_doc, att = "СумПрдщ"),
                                                                     na.rm = T)
                                         )

                                         equity_lag1 <- lapply(lapply(changes_in_equity_lag1_tags, FUN = extr_val, doc = xml_doc, att = "Итог"), function(x) as.character(sum(as.numeric(x))))

                                         corrections_lag1 <- lapply(correct_tags, FUN = extr_val, doc = xml_doc, att = "На31ДекПрПред")

                                         net_assets_lag1 <- extr_val(doc = xml_doc, node_name = net_assets_tag, att = "На31ДекПред")

                                         cashflow_lag1 <- as.list(
                                                                  pmax(
                                                                       sapply(sapply(cashflow_tags, FUN = extr_val, doc = xml_doc, att = "СумПред"), function(x) as.character(sum(as.numeric(x)))),
                                                                       sapply(sapply(cashflow_tags, FUN = extr_val, doc = xml_doc, att = "СумПрдщ"), function(x) as.character(sum(as.numeric(x)))),
                                                                       na.rm = T)
                                         )
                                         designated_use_lag1 <- as.list(
                                                                        pmax(
                                                                             sapply(designated_use_tags, FUN = extr_val, doc = xml_doc, att = "СумПред"),
                                                                             sapply(designated_use_tags, FUN = extr_val, doc = xml_doc, att = "СумПрдщ"),
                                                                             na.rm = T)
                                         )

                                         row_lag1 <- c(firm_info,
                                                       list(year = year - 1), # NB: year is set to the previous year
                                                       meta,
                                                       balance_lag1, 
                                                       finres_lag1, 
                                                       equity_lag1, 
                                                       corrections_lag1, 
                                                       list(line_3600 = net_assets_lag1), 
                                                       cashflow_lag1, 
                                                       designated_use_lag1)

                                         output_file <- glue::glue("{temp_output_dir}/{dir_year}_{process_id}_lag1_result.csv")
                                         fwrite(as.data.table(row_lag1), output_file, append = file.exists(output_file))

                                     },
                                     error = function(e) {
                                         failed_xmls <- glue::glue("{temp_output_dir}/corrupted_xmls_lag1.csv")
                                         fwrite(data.table(path = xml_path), failed_xmls, append = file.exists(failed_xmls))
                                         message("lag1 year")
                                         message(e$message)

                                         FALSE

                                     }
        )

        lag2_year_parsed <- tryCatch(
                                     {
                                         balance_lag2 <- lapply(balance_tags, FUN = extr_val, doc = xml_doc, att = "СумПрдшв")
                                         net_assets_lag2 <- extr_val(doc = xml_doc, node_name = net_assets_tag, att = "На31ДекПрПред")

                                         row_lag2 <- c(firm_info,
                                                       list(year = year - 2),
                                                       meta,
                                                       balance_lag2,  
                                                       list(line_3600 = net_assets_lag2))

                                         output_file <- glue::glue("{temp_output_dir}/{dir_year}_{process_id}_lag2_result.csv")
                                         fwrite(as.data.table(row_lag2), output_file, append = file.exists(output_file))

                                     },
                                     error = function(e) {
                                         failed_xmls <- glue::glue("{temp_output_dir}/corrupted_xmls_lag2.csv")
                                         fwrite(data.table(path = xml_path), failed_xmls, append = file.exists(failed_xmls))
                                         message("lag2 year")
                                         message(e$message)

                                         FALSE

                                     }
        )

        # Log that this XML has been processed
        processed_xmls_log <- glue::glue("{temp_output_dir}/{dir_year}_{process_id}_done.csv")
        fwrite(data.table(path = xml_path), processed_xmls_log, append = file.exists(processed_xmls_log))

    }

    # Return nothing

    NULL

}

