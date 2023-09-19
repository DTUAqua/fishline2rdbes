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
    library(dplyr)
    library(stringr)
    library(data.table)
    library(openxlsx)
    library(purrr)
    library(lubridate)
    library(RODBC)
    library(sqldf)
    library(knitr)

    # Get references ----

    # Get refences from https://github.com/ices-eg/RCGs/tree/master/Metiers/Reference_lists ----
    ## Code taken from the same repo

    source(
      paste0(
        "https://raw.githubusercontent.com/ices-eg/RCGs/master/Metiers/Scripts/Functions/loadAreaList.R"
      )
    )

    url_area <-
      "https://github.com/ices-eg/RCGs/raw/master/Metiers/Reference_lists/AreaRegionLookup.csv"

    area_ref <- loadAreaList(url_area)

    source(
      "https://raw.githubusercontent.com/ices-eg/RCGs/master/Metiers/Scripts/Functions/loadMetierList.R"
    )

    url_metier <-
      "https://github.com/ices-eg/RCGs/raw/master/Metiers/Reference_lists/RDB_ISSG_Metier_list.csv"

    metier_ref <- loadMetierList(url_metier)

    # Remove >0 metiers

    metier_ref <- subset(metier_ref, mesh != ">0" & sd_mesh == 0)

    # Add years start and end date to relation

    metier_ref$Start_year[is.na(metier_ref$Start_year)] <- 1990
    metier_ref$End_year[is.na(metier_ref$End_year)] <- year(today())


    source(
      "https://raw.githubusercontent.com/ices-eg/RCGs/master/Metiers/Scripts/Functions/loadSpeciesList.R"
    )

    url_target <-
      "https://github.com/ices-eg/RCGs/raw/master/Metiers/Reference_lists/Metier%20Subgroup%20Species%202020.xlsx"
    target_ref <- loadSpeciesList(url_target)



    # Add needed codes ----
    # The metier reference list requires a RCG region, gear and mesh size,
    #target assemblage, selection device and mesh size in the selection device

    ## RCG region ----

    print("Adding RCG region")

    channel <- odbcConnect("FishLine")
    area <- sqlQuery(channel,
                     paste("SELECT DFUArea, areaICES FROM L_DFUArea",
                           sep = ""))
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

    print((
      distinct(no_rcg_region, year, cruise, trip, station, dfuArea, RCG)
    ))


    ## Gear code ----

    print("Recode Gear codes")


    gear_unique_metier <- unique(metier_ref$gear)
    gear_unique_samp <- unique(df$gearType)

    df_2$gear <- df_2$gearType

    df_2$gear[df$gear == "TBN"] <-
      "OTB" # This is correct TBN is a OTB

    # The issues below should be validate

    df_2$gear[df_2$gearType == "LL"] <-
      "LLS"  # This is a bit dodgy, since I don't know if this is true - ask Hans / Frank
    df_2$gear[df_2$gearType == "FIX"] <-
      "FPO" # This is a bit dodgy, since I don't know if this is true

    gear_no_match <- subset(df_2,!(gear %in% gear_unique_metier))

    print("Station with gear codes not in the metier list: ")

    print((
      select(
        gear_no_match,
        year,
        cruise,
        trip,
        station,
        dfuArea,
        gearType,
        gear
      )
    ))

    df_2$gear[is.na(df_2$gear)] <- "MIS"

    ## Target assemblage ----

    print("Adding target assemblage")

    channel <- odbcConnect("FishLine")
    art <- sqlQuery(channel,
                    paste("SELECT speciesCode, speciesFAO FROM L_Species",
                          sep = ""))
    close(channel)

    df_4 <-
      left_join(df_2, art, by = c("targetSpecies1" = "speciesCode"))

    print(paste("Before species join: ", nrow(df)))
    print(paste("After species join: ", nrow(df_4)))

    df_4$FAO_species <- df_4$speciesFAO

    df_5 <- left_join(df_4, target_ref)

    df_5 <- rename(df_5, targetSpecies = species_group)

    print("Adding metier6")

    metier_by <-
      join_by(
        RCG,
        year >= Start_year,
        year <= End_year,
        gear,
        targetSpecies == target,
        meshSize >= m_size_from,
        meshSize < m_size_to
      )

    df_6 <- left_join(df_5, metier_ref, metier_by)

    df_6$metier_level_6[is.na(df_6$metier_level_6)] <-
      "MIS_MIS_0_0_0"

    df_6 <- rename(df_6, metier6 = metier_level_6)

    # Code RDBES variables ----

    dat <- df_6

    dat$nationalFishingActivity <-
      "" # Kibi: I don't think we need to code this one. We can just use a relation when estimating
    dat$metier5 <- "" # Kibi: Optional. It can be deduced from metier6
    # metier6 - done above
    # # gear - done above
    # # meshSize - already in data with the correct name
    dat$selectionDevice <-
      "" # TO DO - we have the info in the observer programs for the active gears
    dat$selectionDeviceMeshSize <- "" # Same as above
    # # targetSpecies <-
    # # MitigationDevice is not recorded for now, but will come for gillnets in the future
    dat$incidentalByCatchMitigationDeviceFirst <-
      "NotRecorded" #This is T for now
    dat$incidentalByCatchMitigationDeviceTargetFirst <- "NotApplicable"
    dat$incidentalByCatchMitigationDeviceSecond <- "NotRecorded"
    dat$incidentalByCatchMitigationDeviceTargetSecond <- "NotApplicable"
    # dat$gearDimensions <- dat$numNets * dat$lengthNets

    return(dat)

  }
