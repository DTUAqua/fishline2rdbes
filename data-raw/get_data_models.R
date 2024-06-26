

library(readxl)

# Get CS data model

url_data_model <-
    "https://github.com/ices-tools-dev/RDBES/raw/master/Documents/RDBES%20Data%20Model%20CS.xlsx"


download.file(url = url_data_model,
              destfile = "./data-raw/RDBES_data_model_CS.xlsx", mode = "wb")

version <- gsub(".*? ", "", gsub("\\.", "_", readxl::excel_sheets("./data-raw/RDBES_data_model_CS.xlsx")[1]))

file.copy(from = "./data-raw/RDBES_data_model_CS.xlsx",
          to = paste0("./data/RDBES_data_model_CS_", version, ".xlsx"))

file.remove("./data-raw/RDBES_data_model_CS.xlsx")

# Get VD / SL data model

url_data_model <-
  "https://github.com/ices-tools-dev/RDBES/raw/master/Documents/RDBES%20Data%20Model%20VD%20SL.xlsx"


download.file(url = url_data_model,
              destfile = "./data-raw/RDBES_data_model_VD_SL.xlsx", mode = "wb")


version <- readxl::read_xlsx("./data-raw/RDBES_data_model_VD_SL.xlsx", sheet = 1)[15, 1]

version_1 <- gsub("\\.", "_", gsub(".*? ", "", version))

file.copy(from = "./data-raw/RDBES_data_model_VD_SL.xlsx",
          to = paste0("./data/RDBES_data_model_VD_SL_v", version_1, ".xlsx"))

file.remove("./data-raw/RDBES_data_model_VD_SL.xlsx")
