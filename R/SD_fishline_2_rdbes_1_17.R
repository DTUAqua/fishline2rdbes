#' FishLine 2 RDBES, sampling details (SD)
#'
#' @param id_input_file
#' @param data_model
#'
#' @return
#' @export
#'
#' @examples

SD_fishline_2_rdbes <- function(id_input_file = DE,
                                data_model = data_model) {
  library(dplyr)
  library(tidyr)

  sd_temp <- read.xlsx(data_model, sheet = 5)
  var_keep <- na.omit(sd_temp$Order)
  sd_temp <- t(sd_temp$R.Name)[1:max(var_keep)]

  #Start coding the SD variables

  sd <- id_input_file
  sd$SDrecType <- "SD"
  sd$SDctry <- "DK"
  sd$SDinst <- "2195"
  sd$SDid <- row.names(sd)

  SD <- select(sd, one_of(sd_temp))

  return(list(SD, sd_temp))

}
