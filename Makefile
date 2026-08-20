all : build_filing_panel build_articulation_panel adjust_values
.PHONY : all

CHECKPOINTS_DIR := .make_checkpoints

$(CHECKPOINTS_DIR) :
	mkdir $(CHECKPOINTS_DIR)

# ================================================================================
# Build financials panel

$(CHECKPOINTS_DIR)/download_rosstat :  
	@echo "Downloading Rosstat's annual financial statements tables."
	Rscript code/1_financials/1a_collect_rosstat_data.R
	@touch $@

$(CHECKPOINTS_DIR)/build_rosstat_panel : $(CHECKPOINTS_DIR)/download_rosstat
	@echo "Combining Rosstat's annual financials into a panel, imputing missing statements from the next statements' prior-years values."
	Rscript code/1_financials/1b_build_rosstat_panel.R
	@touch $@

$(CHECKPOINTS_DIR)/collect_fns_xmls_ids : 	
	@echo "Using the Federal Tax Service' API to obtain all available statements' tokens. Takes hours."
	Rscript code/1_financials/2a_collect_fns_xmls_ids.R
	@touch $@

$(CHECKPOINTS_DIR)/collect_fns_xmls : $(CHECKPOINTS_DIR)/collect_fns_xmls_ids
	@echo "Fetching statements from API using their tokens. Takes hours."
	Rscript code/1_financials/2b_collect_fns_xmls.R
	@touch $@

$(CHECKPOINTS_DIR)/parse_fns_xmls : $(CHECKPOINTS_DIR)/collect_fns_xmls
	@echo "Parsing XMLs in parallel, creating multiple CSVs (2/3 of available cores * number of forms versions). Takes days."
	Rscript code/1_financials/2c_parse_fns_xmls.R
	@touch $@

$(CHECKPOINTS_DIR)/build_fns_panel : $(CHECKPOINTS_DIR)/parse_fns_xmls
	@echo "Assemble a panel from the CSVs produced on the previous step, imputing missing statements from the next statements' prior-year values."
	Rscript code/1_financials/2d_build_fns_panel.R
	@touch $@

$(CHECKPOINTS_DIR)/build_filing_panel : temp/rosstat_financials_impFrNY.fst temp/fns_financials_impFrNY.fst
	@echo "Create firm year panel indicating whether a statement was filed for a given year. NB: here we do not consider imputed statements."
	Rscript code/1_financials/3_build_filing_panel.R
	@touch $@

$(CHECKPOINTS_DIR)/combine_rosstat_fns : temp/rosstat_financials_impFrNY.fst temp/fns_financials_impFrNY.fst
	@echo "Combine Rosstat's and FNS' data."
	Rscript code/1_financials/4_combine_rosstat_fns_panels.R
	@touch $@

$(CHECKPOINTS_DIR)/build_articulation_panel : temp/combined_financials_impFrNY_negLinesCorr.fst
	@echo "Create firm-year articulation panel."
	Rscript code/1_financials/5_build_articulation_panel.R
	@touch $@

$(CHECKPOINTS_DIR)/adjust_values : temp/combined_financials_impFrNY_negLinesCorr.fst
	@echo "Adjusting summarizing lines' values where they do not equate sum of corresponding lines."
	Rscript code/1_financials/6_adjust_values.R
	@touch $@

