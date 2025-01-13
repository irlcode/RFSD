all : build_russian_financials_panel 
.PHONY : all

# ================================================================================
# Build financials panel

download_rosstat :
	@echo "Download Rosstat's annual financial statements tables."
	Rscript code/1_financials/1a_collect_rosstat_data.R

build_rosstat_panel : download_rosstat
	@echo "Combine Rosstat's annual financials into a panel, imputing missing statements from the next statements' prior-years values."
	Rscript code/1_financials/1b_build_rosstat_panel.R

collect_fns_xmls_ids : 
	@echo "Use the Federal Tax Service' API to obtain all available statements' tokens. Takes hours."
	Rscript code/1_financials/2a_collect_fns_xmls_ids.R

collect_fns_xmls : collect_fns_xmls_ids
	@echo "Fetch statements from API using their tokens. May take days."
	Rscript code/1_financials/2b_collect_fns_xmls.R

parse_fns_xmls : collect_fns_xmls
	@echo "Parse XMLs in parallel, creating multiple CSVs (by the number of available cores). Takes days."
	Rscript code/1_financials/2c_parse_fns_xmls.R

build_fns_panel : parse_fns_xmls
	@echo "Assemble a panel from the CSVs produced on the previous step, imputing missing statements from the next statements' prior-year values."
	Rscript code/1_financials/2d_build_fns_panel.R

build_filing_panel : build_rosstat_panel build_fns_panel
	@echo "Creating firm year panel indicating whether a statement was filed for a given year. NB: here we do not consider imputed statements."
	Rscript code/1_financials/3_build_filing_panel.R

combine_rosstat_fns : build_rosstat_panel build_fns_panel
	@echo "Combine Rosstat's and FNS' data."
	Rscript code/1_financials/4_combine_rosstat_fns_panels.R

build_articulation_panel : combine_rosstat_fns
	@echo "Create firm-year articulation panel."
	Rscript code/1_financials/5_build_articulation_panel.R

adjust_values : combine_rosstat_fns
	@echo "Adjusting summarizing lines' values where they do not equate sum of corresponding lines."
	Rscript code/1_financials/6_adjust_values.R

# ================================================================================
# Build elegibility panel

## This step requires EGRUL frim-year panel, Rosstat's classification codes firm-year, and several other objects constructed outside of this project and not included in this repository
prepare_egrul_panel : data/egrul_lite.fst data/rosstat_codes_panel.csv 
	@echo "Create firm-year panel with classification codes to be used to determine eligibility."
	Rscript code/2_eligibility/1_prepare_egrul_panel.R

## This step also requires additional data handcrated based on official decrees which is not provided in this repository
mark_eligible_and_exempt : prepare_egrul_panel
	@echo "Classify firms into eligible to file with Rosstat/FNS in a given year and non-eligible. In latter case add reason for exemption."
	Rscript code/2_eligibility/2_mark_eligible_and_exempt.R

# ================================================================================
# Build the published panel

build_russian_financials_panel : mark_eligible_and_exempt adjust_values
	@echo "Left-join eligibility panel and financials panel, drop non-eligble non-filers, keep all the others."
	Rscript code/3_final_panel/1_build_final_panel.R






