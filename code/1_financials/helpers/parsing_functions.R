# Function to extract value by XPath
extr_val <- function(doc, node_path, att) {

    val <- xml_attr(x = xml_find_all(doc, node_path), attr = att)

    if(grepl("ИзмДобавКап|ИзмРезервКап|УвеличНомАкц", node_path)) {

        val <- xml_attrs(xml_find_all(doc, paste0("//", node_path)))
    }

    if(length(val) > 0) {

        sum(as.numeric(unlist(val)), na.rm = T)

    } else {

        NA

    }
}

# Gets values and writes to CSV
parse_xml <- function(file_path, dir_year, temp_output_dir) {
    
    process_id <- Sys.getpid() # to use in file names so that every process writes in its own file

    xml_doc <- tryCatch(read_xml(file_path),
                        error = function(e) {
                            failed_xmls <- glue::glue("{temp_output_dir}/corrupted_xmls_pid{process_id}.csv")
                            fwrite(list(path = file_path), failed_xmls, append = T)

                            # Skip to the next file
                            return(NULL)

                        })

    meta <- list(
        file_name =  xml_text(xml_find_first(xml_doc, "//Файл/@ИдФайл")),
        form_version = xml_text(xml_find_first(xml_doc, "//Файл/@ВерсФорм")),
        okei = xml_text(xml_find_first(xml_doc, "//Документ/@ОКЕИ")),
        knd = xml_text(xml_find_first(xml_doc, "//Документ/@КНД")),
        corr = xml_text(xml_find_first(xml_doc, "//Документ/@НомКорр")),
        file_date = xml_text(xml_find_first(xml_doc, "//Документ/@ДатаДок")),
        # year <- as.numeric(xml_text(xml_find_first(xml_doc, "//Документ/@ОтчетГод"))),
        reported_period = xml_text(xml_find_first(xml_doc, "//Документ/@Период"))
    )
    meta <- lapply(meta, function(x) ifelse(length(x) == 0, NA, x))

    firm_info <- list(
        inn = xml_text(xml_find_first(xml_doc, "//НПЮЛ/@ИННЮЛ")),
        okved = xml_text(xml_find_first(xml_doc, "//СвНП/@ОКВЭД2")),
        okopf = xml_text(xml_find_first(xml_doc, "//СвНП/@ОКОПФ")),
        okpo = xml_text(xml_find_first(xml_doc, "//СвНП/@ОКПО")),
        okfs = xml_text(xml_find_first(xml_doc, "//СвНП/@ОКФС"))
    )
    firm_info <- lapply(firm_info, function(x) ifelse(length(x) == 0, NA, x))

    if(!is.na(meta$knd) && meta$knd == "0710099") {
        must_audit = xml_text(xml_find_first(xml_doc, "//Документ/@ПрАудит"))
        if(!is.na(must_audit) && must_audit == "1") {
            audit_filename = xml_text(xml_find_first(xml_doc, "//Документ/АудитЗакл/@НаимФайлАЗ"))
            audit_org_inn = xml_text(xml_find_first(xml_doc, "//Документ/СвАудит/@ИННЮЛ"))
            fwrite(c(firm_info, meta, list(year = dir_year, audit_filename = audit_filename, audit_org_inn = audit_org_inn)),
                   glue::glue("{temp_output_dir}/must_audit_pid{process_id}.csv"),
                   append = T)
        }
    }

    # ==========================================================================

    if_known_form <- meta$form_version %in% c("5.03", "5.08", "5.04", "5.10")
    if_old_form <- meta$form_version %notin% c("5.04", "5.10")
    if_noncomm <- substr(firm_info$okopf, 1, 1) %in% c("2", "7")
    form_type <- ifelse(!is.na(meta$knd) && meta$knd == "0710099", "FULL", "SIMPLE")

    if(if_known_form) {
        xml_scheme <- get(paste0("scheme_v", meta$form_version))

        if(!is.na(meta$form_version) && meta$form_version == "5.03") {
            balance_paths <- xml_scheme$balance_paths
        } else {
            if(if_noncomm) {
                balance_paths <- xml_scheme$balance_noncomm_paths
            } else {
                balance_paths <- xml_scheme$balance_comm_paths
            }
        }
    } else {
        if(form_type == "FULL") {
            xml_scheme <- scheme_v5.08
            if(if_noncomm) {
                balance_paths <- xml_scheme$balance_noncomm_paths
            } else {
                balance_paths <- xml_scheme$balance_comm_paths
            }
        } else {
            xml_scheme <- scheme_v5.03
            balance_paths <- xml_scheme$balance_paths
        }
    }

    # ==========================================================================

    # Current year's values
    cur_year <- tryCatch(
        {
            balance <- lapply(balance_paths, FUN = extr_val, doc = xml_doc, att = "СумОтч")
            finres <- lapply(xml_scheme$finres_paths, FUN = extr_val, doc = xml_doc, att = "СумОтч")
            equity <- lapply(xml_scheme$equity_paths, FUN = extr_val, doc = xml_doc, att = "Итог")
            equity_prev <- lapply(xml_scheme$equity_lag1_paths, FUN = extr_val, doc = xml_doc, att = "Итог")
            equity_total_prevprev <- extr_val(doc = xml_doc, node_path = xml_scheme$equity_lag2_path, att = "Итог")

            cashflow <- lapply(xml_scheme$cashflow_paths, FUN = extr_val, doc = xml_doc, att = "СумОтч")
            designated_use <- lapply(xml_scheme$designated_use_paths, FUN = extr_val, doc = xml_doc, att = "СумОтч")

            row <- c(firm_info,
                     list(year = dir_year),
                     meta,
                     balance,
                     finres,
                     equity,
                     equity_prev,
                     list(line_3100 = equity_total_prevprev),
                     cashflow,
                     designated_use)

            if(if_old_form) {
                corrections <- lapply(xml_scheme$correct_paths, FUN = extr_val, doc = xml_doc, att = "На31ДекПред")
                net_assets <- extr_val(doc = xml_doc, node_path = xml_scheme$net_assets_path, att = "На31ДекОтч")
                row <- c(row, corrections, list(line_3600 = xml_scheme$net_assets))
            }

            row <- lapply(row, function(x) ifelse(length(x) == 0, NA, x))

            # Append new row to file on disk
            output_file <- glue::glue("{temp_output_dir}/form_v{meta$form_version}_{form_type}_{ifelse(if_noncomm, 'noncomm', 'comm')}_{dir_year}_pid{process_id}_cur.csv")
            fwrite(row, output_file, append = T)

        },
        error = function(e) {

            failed_xmls <- glue::glue("{temp_output_dir}/corrupted_xmls_pid{process_id}.csv")
            fwrite(list(path = file_path), failed_xmls, append = T)

            # Skip to the next file
            return(NULL)

        }
    )

    # If current year's values have been extracted without error get previous years' values
    lag1_year <- tryCatch(
        {
            balance_lag1 <- as.list(
                pmax(
                    sapply(balance_paths,
                           FUN = extr_val, doc = xml_doc, att = "СумПред"),
                    sapply(balance_paths,
                           FUN = extr_val, doc = xml_doc, att = "СумПрдщ"),
                    na.rm = T)
            )

            finres_lag1 <- as.list(
                pmax(
                    sapply(xml_scheme$finres_paths, FUN = extr_val, doc = xml_doc, att = "СумПред"),
                    sapply(xml_scheme$finres_paths, FUN = extr_val, doc = xml_doc, att = "СумПрдщ"),
                    na.rm = T)
            )


            cashflow_lag1 <- as.list(
                pmax(
                    sapply(xml_scheme$cashflow_paths, FUN = extr_val, doc = xml_doc, att = "СумПред"),
                    sapply(xml_scheme$cashflow_paths, FUN = extr_val, doc = xml_doc, att = "СумПрдщ"),
                    na.rm = T)
            )
            designated_use_lag1 <- as.list(
                pmax(
                    sapply(xml_scheme$designated_use_paths, FUN = extr_val, doc = xml_doc, att = "СумПред"),
                    sapply(xml_scheme$designated_use_paths, FUN = extr_val, doc = xml_doc, att = "СумПрдщ"),
                    na.rm = T)
            )

            row_lag1 <- c(firm_info,
                          list(year = dir_year - 1), # NB: year is set to the previous year
                          meta,
                          balance_lag1,
                          finres_lag1,
                          # equity_lag1,
                          cashflow_lag1,
                          designated_use_lag1)

            if(if_old_form) {
                corrections_lag1 <- lapply(xml_scheme$correct_paths, FUN = extr_val, doc = xml_doc, att = "На31ДекПред")
                net_assets_lag1 <- extr_val(doc = xml_doc, node_path = xml_scheme$net_assets_path, att = "На31ДекОтч")
                row_lag1 <- c(row_lag1, corrections_lag1, list(line_3600 = net_assets_lag1))
            }

            row_lag1 <- lapply(row_lag1, function(x) ifelse(length(x) == 0, NA, x))

            output_file <- glue::glue("{temp_output_dir}/form_v{meta$form_version}_{form_type}_{ifelse(if_noncomm, 'noncomm', 'comm')}_{dir_year}_pid{process_id}_lag1.csv")
            fwrite(row_lag1, output_file, append = T)

        },
        error = function(e) {
            failed_xmls <- glue::glue("{temp_output_dir}/corrupted_xmls_pid{process_id}.csv")
            fwrite(list(path = file_path), failed_xmls, append = T)
            message("lag1 year")
            message(e$message)
        }
    )

    lag2_year <- tryCatch(
        {
            balance_lag2 <- lapply(balance_paths,
                                   FUN = extr_val, doc = xml_doc, att = "СумПрдшв")

            row_lag2 <- c(firm_info,
                          list(year = dir_year - 2),
                          meta,
                          balance_lag2)

            if(if_old_form) {
                row_lag2 <- c(row_lag2, list(line_3600 = extr_val(doc = xml_doc, node_path = xml_scheme$net_assets_path, att = "На31ДекПрПред")))
            }

            row_lag2 <- lapply(row_lag2, function(x) ifelse(length(x) == 0, NA, x))

            output_file <- glue::glue("{temp_output_dir}/form_v{meta$form_version}_{form_type}_{ifelse(if_noncomm, 'noncomm', 'comm')}_{dir_year}_pid{process_id}_lag2.csv")
            fwrite(row_lag2, output_file, append = T)

        },
        error = function(e) {
            failed_xmls <- glue::glue("{temp_output_dir}/corrupted_xmls_pid{process_id}.csv")
            fwrite(list(path = file_path), failed_xmls, append = T)
            message("lag2 year")
            message(e$message)
        }
    )



    # Log that this XML has been processed
    processed_xmls_log <- file.path(temp_output_dir, glue::glue("{dir_year}_pid{process_id}_done.csv"))
    fwrite(list(path = file_path), processed_xmls_log, append = T)

    # Return nothing

    NULL

}



