
#' FishLine 2 RDBES, species list (SL)
#'
#' @description Converts samples data from national database (fishLine) to RDBES. Data model v. 1.18. This function ...
#'
#' @param data_model Where to find the .xlsx with the data model
#' @param year Only takes a single year for now
#' @param cruises Name of cruises in national database
#' @param type only_mandatory | everything
#'
#' @return
#' @export
#'
#'
#' @examples
#'
#'
#'

SL_fishline_2_rdbes <-
  function(data_model = fields_sequence,
           year = 2016,
           years = c(2016),
           cruises = c("MON", "SEAS"),
           type = "only_mandatory")
  {

  library(RODBC)
  library(sqldf)
  library(dplyr)
  library(stringr)
  library(haven)

  sl_temp <- filter(data_model, substr(name, 1, 2) == "SL")
  sl_temp_t <- c("SLrecordType",t(sl_temp$name)[1:nrow(sl_temp)])

  # Get data from FishLine
  channel <- odbcConnect("FishLineDW")
  sl <- sqlQuery(channel, paste("select * FROM dbo.SpeciesList
                  WHERE (year between ", min(years) , " and ", max(years), ")
                                 and cruise in ('", paste(cruises, collapse = "','"), "')", sep = ""))
  close(channel)

  channel2 <- odbcConnect("FishLine")
  art <- sqlQuery(channel2, paste("select * FROM dbo.L_species"))
  close(channel2)


  #Selecting species per catchcategory and region

  sl <- left_join(sl, art)

  no_latin <- distinct(filter(sl, is.na(latin)), speciesCode, dkName)

  #Delete all none species
  sl <- filter(sl, !(is.na(latin)) & !(speciesCode %in% c("INV")))

  # Code SL table

  sl$SLrecordType <- "SL"
  sl$SLcountry = "DK"
  sl$SLinstitute <- "2195"
  sl$SLspeciesListName <- paste("All species")
  sl$SLyear <- year
  sl$SLcatchFraction <- "Catch"
  sl$SLcommercialTaxon <- ""
  sl$SLspeciesCode <- sl$aphiaID

  id <- distinct(ungroup(sl), SLspeciesListName)
  sl$SLid <- row.names(id)

  if (type == "only_mandatory") {

    sl_temp_optional <- filter(data_model, substr(name, 1, 2) == "SL" & min == 0)
    sl_temp_optional_t <- factor(t(sl_temp_optional$name)[1:nrow(sl_temp_optional)])

    for (i in levels(sl_temp_optional_t)) {

      eval(parse(text = paste0("sl$", i, " <- NA")))

    }
  }

  SL <- select(sl, one_of(sl_temp_t), SLid)

  return(list(SL, sl_temp, sl_temp_t))

  }
