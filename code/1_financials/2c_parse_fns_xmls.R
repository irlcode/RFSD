library(parallel)
library(pbapply)
library(data.table)
sapply(dir("code/1_prepare_financials/helpers",
					 full.names = T, recursive = T, include.dirs = F),
			 source)
n_cores <- floor(detectCores() * 2/3) # Set N cores for parallel computing
pbo <- pboptions(type = "txt", txt.width = 50)

temp_output_dir <- file.path("temp", "parsed") # Temporary storage in user's dir
cluster_logs_dir <- file.path(temp_output_dir, "cluster_logs")
dir.create(temp_output_dir, recursive = T, showWarnings = F)
dir.create(cluster_logs_dir, recursive = T, showWarnings = F)

dir_paths <- dir("source/girbo/xml", pattern = "\\d{4}$", full.names = T)
dir_checkpoints <- paste0(checkpoint, "_", basename(dir_paths))

# Create cluster of workers
cl <- makeCluster(n_cores, outfile = glue::glue("{cluster_logs_dir}/cluster_log_{format(Sys.time(), '%Y%m%d_%H%M')}.txt"))

# Export global variables to the cluster
clusterExport(cl, c("temp_output_dir"))

# Import libraries and functions from inside the cluster
clusterEvalQ(cl, {

	library(data.table)
	library(xml2)
	library(stringi)

	sapply(dir("code/1_prepare_financials/helpers",
						 full.names = T, recursive = T, include.dirs = F),
				 source)

}
)

# Iterate over dirs, in each dir list all XMLs, and parse them in parallel
for (i in seq_along(dir_paths)) {

	dir_ <- dir_paths[i]
	dir_year <- as.numeric(basename(dir_)) # Dir name to use in output files' names
	dir_checkpoint <- paste0(checkpoint, "_", dir_year)
	temp_subdir <- file.path(temp_output_dir, dir_year)
	dir.create(temp_subdir, showWarnings = F)

	# Print operation info:
	message(i, " / ", length(dir_paths))
	message("dir: ", dir_)
	time_start <- Sys.time()
	message("started at: ", format(time_start, "%H:%M"))

	xml_paths <- dir(dir_, full.names = T)
	len_xml_paths <- length(xml_paths)
	message("files to parse: ", len_xml_paths)

	# Drop already processed XMLs
	processed_paths <- dir(temp_subdir, pattern = glue::glue("{dir_year}(.*_done.csv|_corrupted)"), full.names = T)
	if (length(processed_paths) > 0) {
		processed_xmls <- rbindlist(lapply(processed_paths, fread))$path
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

	# Launch parsing
	pbwalk(xml_paths,
				 parse_xml,
				 dir_year = dir_year,
				 temp_output_dir = temp_subdir,
				 cl = cl
				 )

	system(glue::glue("touch {dir_checkpoint}"))

	# Print operation info:
	time_stop <- Sys.time()
	message("finished at: ", format(time_stop, "%H:%M"))
	message("\n")

}

stopCluster(cl)

# Create checkpoint
if(all(file.exists(dir_checkpoints))) {
	system(glue::glue("touch {checkpoint}"))
}
