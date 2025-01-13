library(parallel)
library(data.table)
source("financials/helpers/parsing_functions.R")
source("financials/helpers/lines_tags_dict.R")
n_cores <- 32 # Set N cores for parallel computing

temp_output_dir <- "temp/parsed_xml"
cluster_logs_dir <- "temp/cluster_logs"
dir.create(temp_output_dir, recursive = T, showWarnings = F)
dir.create(cluster_logs_dir, recursive = T, showWarnings = F)

dir_paths <- dir("temp/xml", pattern = "\\d{4}$", full.names = T)

# Iterate over dirs, in each dir list all XMLs, and parse them in parallel
for (i in seq_along(dir_paths)) {
	
	dir_ <- dir_paths[i]
	
	# Print operation info:
	message(i, " / ", length(dir_paths))
	message("dir: ", dir_)
	time_start <- Sys.time()
	message("started at: ", format(time_start, "%H:%M"))
	dir_year <- basename(dir_) # Dir name to use in output files' names
	
	xml_paths <- dir(dir_, full.names = T)
	len_xml_paths <- length(xml_paths)
	message("files to parse: ", len_xml_paths)
	
	# Drop already processed XMLs
	processed_paths <- dir(temp_output_dir, pattern = glue::glue("{dir_year}(.*_done.csv|_corrupted)"), full.names = T)
	if (length(processed_paths) > 0) {
		processed_xmls <- rbindlist(lapply(processed_paths, fread))$path
		# message("already parsed: ", length(processed_xmls))
		xml_paths <- setdiff(xml_paths, processed_xmls)
		len_xml_paths <- length(xml_paths)
		message("remaining: ", len_xml_paths)
	} else {
        message("already parsed: 0")
    }
	
	# If all files in the dir have already been processed skip to the next dir
	if (len_xml_paths == 0) {
		next
	}
	
	# Create cluster of workers
	cl <- makeCluster(n_cores, 
                      outfile = glue::glue("{cluster_logs_dir}/cluster_log_{format(Sys.time(), '%Y%m%d_%H%M')}.txt")
    )
	
	# Export global variables to the cluster
	clusterExport(cl, c("xml_paths", "temp_output_dir", "n_cores"))
	
	# Import libraries and functions from inside the cluster
	clusterEvalQ(cl, {
		
		library(data.table)
        # setDTthreads(threads = n_cores)
		library(xml2)
		library(stringi)
		
		source("financials/helpers/lines_tags_dict.R")
		source("financials/helpers/parsing_functions.R")
		
	)
	
	# Launch parsing.
	# NB: `parse_xml` function produces NULL values (not to clutter space),
	# we use it for the "side effect" of writing data to CSV files.
	.void <- clusterApply(cl = cl,
                        x = xml_paths,
                        fun = parse_xml,
                        temp_output_dir = temp_output_dir)
	
	rm(.void)
	stopCluster(cl)
	gc()
	
	time_stop <- Sys.time()
	message("finished at: ", format(time_stop, "%H:%M"))
	message("\n")
}

