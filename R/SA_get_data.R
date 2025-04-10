#' SA_get_data
#'
#' @param xx
#'
#' @returns
#' @export
#'
#' @examples
#'
SA_get_data <- function(link_data = link,
                        sampling_scheme = "DNK_AtSea_Observer_Active",
                        years = 2021) {

  library(RODBC)

  link <- link
  link <- subset(link, DEsamplingScheme %in% sampling_scheme)

  trips <- unique(link$tripId[!is.na(link$tripId)])

  # Get data from FishLine
  channel <- odbcConnect("FishLineDW")
  samp <- sqlQuery(
    channel,
    paste(
      "SELECT specieslist.*, Animal.animalId, Animal.individNum,
                Animal.representative, Animal.number as ani_number
                FROM FishLineDW.dbo.SpeciesList INNER JOIN
                FishLineDW.dbo.Sample ON SpeciesList.sampleId = Sample.sampleId LEFT OUTER JOIN
                FishLineDW.dbo.Animal ON SpeciesList.speciesListId = Animal.speciesListId
       WHERE (Specieslist.year between ",
      min(years),
      " and ",
      max(years),
      ")
              and Sample.tripId in (",
      paste(trips, collapse = ","),
      ")",
      sep = ""
    )
  )
  close(channel)


  channel <- odbcConnect("FishLine")
  area <- sqlQuery(
    channel,
    paste("SELECT DFUArea, areaICES FROM FishLine.dbo.L_DFUArea",
      sep = ""
    )
  )
  art <-
    sqlQuery(
      channel,
      paste(
        "select speciesCode, latin, speciesFAO, aphiaID FROM FishLine.dbo.L_species"
      )
    )
  close(channel)

  output <- list(samp, area, art)
  names(output) <- c("samp", "area", "art")

  return(output)

}
