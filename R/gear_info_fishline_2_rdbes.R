#' Gear information from FishLine required in the RDBES
#'
#' @description the functions populate nationalFishingActivity, metier5,
#' metier6, gear, meshSize, selectionDevice, selectionDeviceMeshSize,
#' targetSpecies, incidentalByCatchMitigationDeviceFirst,
#' incidentalByCatchMitigationDeviceTargetFirst,
#' incidentalByCatchMitigationDeviceSecond,
#' incidentalByCatchMitigationDeviceTargetSecond and gearDimensions
#' from FishLine

#'
#' @param variables
#' @param selection_device: TO DO
#'
#' @return
#' @export
#' @author Kirsten Birch Håkansson, DTU Aqua & people from the RCG metier group
#'
#' @import knitr data.table
#' @importFrom data.table data.table setnames
#' @importFrom stringr str_split_fixed str_detect
#' @importFrom openxlsx read.xlsx
#' @importFrom purrr map map2 map2_lgl map_lgl
#' @importFrom dplyr left_join select distinct
#'
#' @examples
#'


gear_info_fishline_2_rdbes <-
  function(df = samp) {

    # usethis::use_package("dplyr")
    # library(stringr)
    # library(data.table)
    # library(openxlsx)
    # library(purrr)
    # library(lubridate)
    # library(RODBC)
    # library(sqldf)
    # library(knitr)

    # Get references ----

    # Get refences from https://github.com/ices-eg/RCGs/tree/master/Metiers/Reference_lists ----
    ## Code taken from the same repo

    source(paste0("https://raw.githubusercontent.com/ices-eg/RCGs/master/Metiers/Scripts/Functions/loadAreaList.R"))

    url_area <- "https://github.com/ices-eg/RCGs/raw/master/Metiers/Reference_lists/AreaRegionLookup.csv"

    area_ref <- loadAreaList(url_area)

    source("https://raw.githubusercontent.com/ices-eg/RCGs/master/Metiers/Scripts/Functions/loadMetierList.R")

    url_metier <- "https://github.com/ices-eg/RCGs/raw/master/Metiers/Reference_lists/RDB_ISSG_Metier_list.csv"

    metier_ref <- loadMetierList(url_metier)

    source("https://raw.githubusercontent.com/ices-eg/RCGs/master/Metiers/Scripts/Functions/loadSpeciesList.R")

    url_target <- "https://github.com/ices-eg/RCGs/raw/master/Metiers/Reference_lists/Metier%20Subgroup%20Species%202020.xlsx"
    target_ref <- loadSpeciesList(url_target)



    # Add needed codes ----
    # The metier reference list requires a RCG region, gear and mesh size,
    #target assemblage, selection device and mesh size in the selection device

    ## RCG region ----

    print("Adding RCG region")

      channel <- odbcConnect("FishLine")
      area <- sqlQuery(
        channel,
        paste("SELECT DFUArea, areaICES FROM L_DFUArea",
              sep = ""
        )
      )
      close(channel)

      df_1 <- left_join(df, area, by = c("dfuArea" = "DFUArea"))

      print(paste("Before area join: ", nrow(df)))
      print(paste("After area join: ", nrow(df_1)))

      df_1 <- rename(df_1, "area" = "areaICES")

      df_2 <- left_join(df_1, area_ref)

      print(paste("Before RCG region join: ", nrow(df_1)))
      print(paste("After RCG region join: ", nrow(df_2)))

      no_rcg_region <- subset(df_2, is.na(df_2$RCG))

      print("Station with missing RCG region: ")

      print((distinct(no_rcg_region, year, cruise, trip, station, dfuArea, RCG)))




    ## Gear code ----

    print("Recode Gear codes")


      gear_unique_metier <- unique(metier_ref$gear)
      gear_unique_samp <- unique(df$gearType)

      df$gear <- df$gearType

      df$gear[df$gear == "TBN"] <- "OTB" # This is correct TBN is a OTB

      # The issues below should be validate

      df$gear[df$gear == "LL"] <- "LLS"  # This is a bit dodgy, since I don't if it is true - ask Hans / Frank
      df$gear[df$gear == "FIX"] <- "FPO" # This is a bit dodgy, since I don't know what this is

      gear_no_match <- subset(df, !(gear %in% gear_unique_metier))

      print("Station with gear codes not in the metier list: ")

      print((select(gear_no_match, year, cruise, trip, station, dfuArea, gearType, gear)))

    ## Target assemblage ----

    print("Adding target assemblage")

      channel <- odbcConnect("FishLine")
      art <- sqlQuery(
        channel,
        paste("SELECT speciesCode, speciesFAO FROM L_Species",
              sep = ""
        )
      )
      close(channel)

      df_4 <- left_join(df, art, by = c("targetSpecies1" = "speciesCode"))

      print(paste("Before species join: ", nrow(df)))
      print(paste("After species join: ", nrow(df_4)))

      df_4$FAO_species <- df_4$speciesFAO

      df_5 <- left_join(df_4, target_ref)


    df_5 <- left_join(df_4, metier_ref)

    # Code RDBES variables ----

    # nationalFishingActivity <- ""
    # metier5 <- ""
    # metier6
    # gear <- gearType
    # # meshSize - already in data with the correct name
    # selectionDevice <- "" # TO DO - we have the info in the observer programs
    # selectionDeviceMeshSize <- "" # Same as above
    # targetSpecies <-
    # # MitigationDevice is not recorded for now, but will come for gillnets in the future
    # incidentalByCatchMitigationDeviceFirst <- "NotRecorded" #This is T for now
    # incidentalByCatchMitigationDeviceTargetFirst <- "NotApplicable"
    # incidentalByCatchMitigationDeviceSecond <- "NotRecorded"
    # incidentalByCatchMitigationDeviceTargetSecond <- "NotApplicable"
    # gearDimensions <- numNets*lengthNets










  }
