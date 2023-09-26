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
get_data_model_vd_sl <- function(table_name, data_model_path, data_model_version = "V1_19_3") {
  ## datamodel

  nam <- data.frame(name = c("Vessel Details", "Species List Details"),
                    code = c("VD", "SL"))

  if (! table_name %in% nam$name){
    print("Table Name Not Found In VD SL Data Model")
  } else{

    j <- match(table_name, nam$name)

    dat = openxlsx::read.xlsx(paste0(data_model_path, "/RDBES_Data_Model_VD_SL_", data_model_version, ".xlsx"),
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
