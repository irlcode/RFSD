# Make directory to store Rosstat source data
destination_dir <- file.path("data", "rosstat")
dir.create(destination_dir, recursive = T)

# Declare urls of datasets
data_urls <- c(
          "https://rosstat.gov.ru/opendata/7708234640-bdboo2012/data-20200331-structure-20121231.zip", # 2012 data
          "https://rosstat.gov.ru/opendata/7708234640-bdboo2013/data-20200331-structure-20131231.zip", # 2013
          "https://rosstat.gov.ru/opendata/7708234640-bdboo2014/data-20200327-structure-20141231.zip", # 2014
          "https://rosstat.gov.ru/opendata/7708234640-bdboo2015/data-20200327-structure-20151231.zip", # 2015
          "https://rosstat.gov.ru/opendata/7708234640-bdboo2016/data-20200327-structure-20161231.zip", # 2016
          "https://rosstat.gov.ru/opendata/7708234640-bdboo2017/data-20200327-structure-20171231.zip", # 2017
          "https://rosstat.gov.ru/opendata/7708234640-7708234640bdboo2018/data-20200327-structure-20181231.zip" # 2018 
)

# Declare urls of dataset descriptions (as for late 2024 structure is uniform, so this is just to be on the safe side)
structure_urls <- c(
					"https://rosstat.gov.ru/opendata/7708234640-bdboo2012/structure-20121231.csv", # 2012 file structure
					"https://rosstat.gov.ru/opendata/7708234640-bdboo2013/structure-20131231.csv", # 2012
					"https://rosstat.gov.ru/opendata/7708234640-bdboo2014/structure-20141231.csv", # 2012
					"https://rosstat.gov.ru/opendata/7708234640-bdboo2015/structure-20151231.csv", # 2015
					"https://rosstat.gov.ru/opendata/7708234640-bdboo2016/structure-20161231.csv", # 2016
					"https://rosstat.gov.ru/opendata/7708234640-bdboo2017/structure-20171231.csv", # 2017
					"https://rosstat.gov.ru/opendata/7708234640-7708234640bdboo2018/structure-20181231.csv" # 2018
)

# Make destination paths
data_destinations <- file.path(destination_dir, paste0("rosstat_data_", 2012:2018, ".zip"))
structure_destinations <- file.path(destination_dir, paste0("rosstat_structure_", 2012:2018, ".csv"))

# Collect
options(HTTPUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.3")
for(i in seq_along(2012:2018)) {
	download.file(data_urls[i], data_destinations[i], method = "curl")
    download.file(structure_urls[i], structure_destinations[i], method = "curl")
}


