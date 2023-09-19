

library(readxl)


url_data_model <-
    "https://github.com/ices-tools-dev/RDBES/raw/master/Documents/RDBES%20Data%20Model%20CS.xlsx"


download.file(url = url_data_model,
              destfile = "./data-raw/RDBES_data_model_CS.xlsx", mode = "wb")

version <- gsub(".*? ", "", gsub("\\.", "_", readxl::excel_sheets("./data-raw/RDBES_data_model_CS.xlsx")[1]))

file.copy(from = "./data-raw/RDBES_data_model_CS.xlsx",
          to = paste0("./data/RDBES_data_model_CS_", version, ".xlsx"))

file.remove("./data-raw/RDBES_data_model_CS.xlsx")

# usethis::use_data(data_for_testing_fun_get_gear_info, overwrite = T)
