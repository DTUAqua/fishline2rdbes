#' Title
#'
#' @param table_name
#' @param data_model_version
#'
#' @importFrom openxlsx read.xlsx
#'
#' @return
#' @export
#'
#' @examples
get_data_model <- function(table_name,
                           data_model_path = "Q:/dfad/data/Data/RDBES/sample_data/create_RDBES_data/input",
                           data_model_version = "v1_19_20") {
  ## datamodel

  nam <- data.frame(name = c("Design", "Location", "Sampling Details", "Temporal Event",
                             "Vessel Selection", "Fishing Trip",
                             "Fishing Operation",
                             "Species Selection",  "Landing event", "Sample",
                             "Frequency Measure", "Biological Variable"),
                    code = c("DE", "LO", "SD", "TE", "VS", "FT", "FO", "SS", "LE", "SA", "FM", "BV"))

  if (! table_name %in% nam$name){
    print("Table Name Not Found In CS Design Model")
  } else{

    j <- match(table_name, nam$name)

    dat = openxlsx::read.xlsx(paste0(data_model_path, "/RDBES_Data_Model_CS_", data_model_version, ".xlsx"),
                    sheet = nam[j, "name"])

    dat <- dat[dat$Order %in% 1:900, c("Order", "Field.Name")]
    #dat$Field.Name <- substr(dat$Field.Name, 3, nchar(dat$Field.Name))

    dat2 <- data.frame(matrix(nrow = 0, ncol = nrow(dat)))
    names(dat2) <- dat$Field.Name

    # assign(
    #   x = nam[j, "code"],
    #   value = dat2,
    #   envir = .GlobalEnv)

    # rm(dat, dat2)

  }

  return(dat2)
}
