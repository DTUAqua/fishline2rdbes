
#' FishLine 2 RDBES, biological variables (BV)
#'
#' @description Converts samples data from national database (fishLine) to RDBES. Data model v. 1.17. This function ...
#'
#' @param df
#' @param data_model Where to find the .xlsx with the data model
#' @param years
#' @param cruises Name of cruises in national database
#' @param treshold
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
  function(data_model = data_model,
           years = c(2006, 2018),
           year = 2016,
           cruises = c("MON", "SEAS"),
           treshold = 0.05)
  {

  library(openxlsx)
  library(RODBC)
  library(sqldf)
  library(dplyr)
  library(stringr)

  sl_temp <- read.xlsx(data_model, sheet = 13)
  var_keep <- na.omit(sl_temp$Order)
  sl_temp <- t(sl_temp$R.Name)[1:max(var_keep)]

  # Get data from FishLine
  channel <- odbcConnect("FishLineDW")
  sl <- sqlQuery(channel, paste("select * FROM dbo.SpeciesList
                  WHERE (year between ", min(years) , " and ", max(years), ")
                                 and cruise in ('", paste(cruises, collapse = "','"), "')", sep = ""))
  close(channel)

  channel2 <- odbcConnect("FishLine")
  dfuArea <- sqlQuery(channel2, paste("select * FROM dbo.L_dfuArea"))
  art <- sqlQuery(channel2, paste("select * FROM dbo.L_species"))
  close(channel2)

  #Selecting species per catchcategory and region

  sl <- left_join(sl, dfuArea[, c("DFUArea","areaICES")], by = c("dfuArea" = "DFUArea"))
  sl <- left_join(sl, art)


  no_latin <- distinct(filter(sl, is.na(latin)), speciesCode, dkName)

  #Delete all none species
  sl <- filter(sl, !(is.na(latin)) & !(speciesCode %in% c("INV")) & landingCategory %in% c("DIS","KON"))


  sl <- mutate(sl, region = ifelse(substr(areaICES, 1, 4) == "27.4", "27.4",
                                   ifelse(substr(areaICES, 1, 6) == "27.3.a", "27.3.a",
                                          ifelse(areaICES %in% c("27.3.c.22", "27.3.b.23", "27.3.d.24"), "27.3.2224",
                                                 ifelse(areaICES %in% c("27.3.d.25", "27.3.d.26", "27.3.d.27"), "27.3.2526", NA)))))

  no_observations <- distinct(sl, sampleId, speciesCode, aphiaID, region, landingCategory)

  no_observations_sum <- summarise(group_by(no_observations, speciesCode, aphiaID, region, landingCategory), no_obs = length(unique(sampleId)))

  no_observations_total <- summarise(group_by(no_observations, region, landingCategory), no_obs_total = length(unique(sampleId)))

  no_obs <- mutate(left_join(no_observations_sum, no_observations_total), pct = no_obs / no_obs_total)

  sl <- filter(no_obs, pct > treshold)

  # Code SL table

  sl$SLrecType <- "SL"
  sl$SLlistName <- paste("DNK_observer_at_sea", sl$region, sl$landingCategory, sep = "_")
  sl$Slyear <- year
  sl$SLsppCode <- sl$aphiaID
  sl$SLcommSpp <- ""
  sl$SLCatchFrac <- ifelse(sl$landingCategory == "KON", "Lan", "Dis")

  id <- distinct(ungroup(sl), SLlistName)
  id$SLid <- row.names(id)

  sl <- left_join(sl, id)

  SL <- select(ungroup(sl), one_of(sl_temp), region)

  return(list(SL, sl_temp))

  }
