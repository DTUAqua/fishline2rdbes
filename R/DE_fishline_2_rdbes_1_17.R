#' FishLine 2 RDBES, design (DE)
#'
#' @param year
#' @param data_model
#' @param type observer_at_sea |
#' @param hierarchy
#' @param stata quarter | none
#'
#' @return
#' @export
#'
#'
#' @examples

DE_fishline_2_rdbes <- function(year = 2016,
                                data_model = data_model,
                                type = "observer_at_sea",
                                hierarchy = 1,
                                strata = "quarter") {
  library(dplyr)
  library(tidyr)

  de_temp <- read.xlsx(data_model, sheet = 2)
  var_keep <- na.omit(de_temp$Order)
  de_temp <- t(de_temp$R.Name)[1:max(var_keep)]

  #Start coding the DE variables

  de <- NULL

  de$DErecType <- "DE"

  if (type == "observer_at_sea") {
    de$DEsampScheme <- "DNK observer at-sea"

  }

  de$DEyear <- year

  de <- as.data.frame(de)

  de$DEsampScheme <- "DNK observer at-sea"
  de$DEyear <- year

  if (strata == "quarter") {

    DEstratum <- data.frame(DEstratum = c("Q1", "Q2", "Q3", "Q4"))
    de <- sqldf('select * from de, DEstratum')

  } else if (strat == "none") {

    de <- mutate(de, DEstratum == "")

  }

  if (type == "observer_at_sea") {

    de$DEhierarchyCor <- "Y"

  }

  de$DEhierarchy <- hierarchy

  de_1 <- distinct(de)
  de_1$DEid <- row.names(de_1)

  DE <- select(de_1, one_of(de_temp))

  return(list(DE, de_temp))


}
