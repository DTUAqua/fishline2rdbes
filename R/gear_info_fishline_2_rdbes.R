#' Gear information from FishLine required in the RDBES
#'
#' @description The function populate nationalFishingActivity, metier5,
#' metier6, gear, meshSize, selectionDevice, selectionDeviceMeshSize,
#' targetSpecies, incidentalByCatchMitigationDeviceFirst,
#' incidentalByCatchMitigationDeviceTargetFirst,
#' incidentalByCatchMitigationDeviceSecond,
#' incidentalByCatchMitigationDeviceTargetSecond and gearDimensions
#' from gear informations in FishLine

#'
#' @param variables
#' @param selection_device: TO DO
#'
#' @return
#' @export
#' @author Kirsten Birch Håkansson, DTU Aqua & people from the RCG metier group
#'
#' @import knitr purrr
#' @importFrom data.table data.table setnames
#' @importFrom stringr str_split_fixed str_detect
#' @importFrom openxlsx read.xlsx
#' @importFrom purrr map map2 map_lgl
#' @importFrom dplyr left_join join_by select distinct rename
#' @importFrom lubridate year today
#' @importFrom RODBC odbcConnect sqlQuery
#'
#' @examples
#'


gear_info_fishline_2_rdbes <-
  function(df = samp,
           record_type,
           ices_area_already_addded = F,
           checks = F) {

    library(purrr)
    library(data.table)
    library(stringr)
    library(purrr)
    library(openxlsx)
    library(dplyr)
    library(RODBC)

    # Get references ----

    # Get refences from https://github.com/ices-eg/RCGs/tree/master/Metiers/Reference_lists ----
    ## Code taken from the same repo

    ## Area ref ----

    source(
      paste0(
        "https://raw.githubusercontent.com/ices-eg/RCGs/master/Metiers/Scripts/Functions/loadAreaList.R"
      )
    )

    data.table::data.table()  #Needed for loading the fun - maybe ask RCG metier group to add this to their script
    data.table::setnames(df, names(df), names(df)) #Needed for loading the fun - maybe ask RCG metier group to add this to their script

    url_area <-
      "https://github.com/ices-eg/RCGs/raw/master/Metiers/Reference_lists/AreaRegionLookup.csv"

    area_ref <- loadAreaList(url_area)

    ## Metier ref ----

    source(
      "https://raw.githubusercontent.com/ices-eg/RCGs/master/Metiers/Scripts/Functions/loadMetierList.R"
    )

    df$cruise <- stringr::str_split_fixed(df$cruise, "", 1) #Needed for loading the fun - maybe ask RCG metier group to add this to their script
    x <- stringr::str_detect(df$year, "-") #Needed for loading the fun - maybe ask RCG metier group to add this to their script
    rm(x)

    url_metier <-
      "https://github.com/ices-eg/RCGs/raw/master/Metiers/Reference_lists/RDB_ISSG_Metier_list.csv"

    metier_ref <- loadMetierList(url_metier)

    ### Fix metier ref ----

    # Remove >0 metiers - these cause overlap with metiers with secified mesh sizes
    # Except for DRB, FPN, FPO and FYK - these never have mesh size ranges

    metier_ref <- subset(metier_ref, mesh != ">0" | gear %in% c("DRB", "FPN", "FPO", "FYK"))

    # Add years start and end date to relation

    metier_ref$Start_year[is.na(metier_ref$Start_year)] <- 1950 # An abrary year in the past
    metier_ref$End_year[is.na(metier_ref$End_year)] <- lubridate::year(lubridate::today())

    # Remove metiers with selection devices. Should be changed in the future
    # Selection device is required for the Baltic, but for now all active gears in the Baltic are coded as with BACOMA

    metier_ref$sd[metier_ref$RCG == "BALT" & metier_ref$sd != 0 & metier_ref$metier_level_6 != "OTB_DEF_115-120_3_115"] <- 0
    metier_ref$sd_mesh[metier_ref$RCG == "BALT" & metier_ref$sd_mesh != 0 & metier_ref$metier_level_6 != "OTB_DEF_115-120_3_115"] <- 0

    metier_ref <- subset(metier_ref, sd_mesh == 0)

    ## Species ref ----

    source(
      "https://raw.githubusercontent.com/ices-eg/RCGs/master/Metiers/Scripts/Functions/loadSpeciesList.R"
    )

    # Below crap needed for loading the fun - maybe ask RCG metier group to add this to their script

    y <- openxlsx::read.xlsx("https://github.com/ices-eg/RCGs/raw/master/Metiers/Reference_lists/Metier%20Subgroup%20Species.xlsx", sheet = 1)
    rm(y)

    url_target <-
      "https://github.com/ices-eg/RCGs/raw/master/Metiers/Reference_lists/Metier%20Subgroup%20Species.xlsx"
    target_ref <- loadSpeciesList(url_target)

    # Add needed codes ----
    # The metier reference list requires a RCG region, gear and mesh size,
    #target assemblage, selection device and mesh size in the selection device

    ## RCG region ----

    print("Adding RCG region")

    if (ices_area_already_addded == T) {

      df_1 <- df
    }

    if (ices_area_already_addded == F) {

      channel <- RODBC::odbcConnect("FishLine")
      area <- RODBC::sqlQuery(channel,
                              paste("SELECT DFUArea, areaICES FROM L_DFUArea",
                                    sep = ""))
      close(channel)

      df_1 <- left_join(df, area, by = c("dfuArea" = "DFUArea"))

      if (checks == T) {
        print(paste("Before join with L_DFUArea: ", nrow(df)))
        print(paste("After join with L_DFUArea: ", nrow(df_1)))
      }
    }


    df_1 <- dplyr::rename(df_1, "area" = "areaICES")

    df_2 <- left_join(df_1, area_ref)

    if (checks == T) {
      print(paste("Before join with area_ref: ", nrow(df_1)))
      print(paste("After join with area_ref: ", nrow(df_2)))
    }

    no_rcg_region <- subset(df_2, is.na(df_2$RCG))

    if (checks == T) {
      print("Station with missing RCG region: ")

      print((
        dplyr::distinct(no_rcg_region, year, cruise, trip, station, dfuArea, RCG)
      ))
    }

    ## Gear code ----

    print("Recode Gear codes")

    gear_unique_metier <- unique(metier_ref$gear)

    df_2$gear <- df_2$gearType

    df_2$gear[df$gearType == "TBN"] <-
      "OTB" # This is correct TBN is a OTB

    # The issues below should be validate

    df_2$gear[df_2$gearType == "LL"] <-
      "LLS"  # This is a bit dodgy, since I don't know if this is true - ask Hans / Frank
    df_2$gear[df_2$gearType == "FIX"] <-
      "FPO" # This is a bit dodgy, since I don't know if this is true

    gear_no_match <- subset(df_2,!(gear %in% gear_unique_metier))

    if (checks == T) {
      print("Station with gear type not in the metier list or missing gear type: ")

      print((
        dplyr::select(
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
    }

    df_2$gear[is.na(df_2$gear)] <- "MIS"

    ## Target assemblage ----

    print("Adding target assemblage")

    channel <- RODBC::odbcConnect("FishLine")
    art <- RODBC::sqlQuery(channel,
                    paste("SELECT speciesCode, speciesFAO FROM L_Species",
                          sep = ""))
    close(channel)

    df_4 <-
      dplyr::left_join(df_2, art, by = c("targetSpecies1" = "speciesCode"))

    # Fixed that Ammodytes marinus is missing from the list - so coded as SAN
    df_4$speciesFAO[df_4$speciesFAO == "QLH"] <- "SAN"

    if (checks == T) {
      print(paste("Before join with L_Species: ", nrow(df_2)))
      print(paste("After join with L_Species: ", nrow(df_4)))

    }

    df_4$FAO_species <- df_4$speciesFAO

    df_5 <- dplyr::left_join(df_4, target_ref)

    if (checks == T) {
      print(paste("Before join with target_ref: ", nrow(df_4)))
      print(paste("After join with target_ref: ", nrow(df_5)))

    }

    df_5 <- dplyr::rename(df_5, targetSpecies = species_group)

    if (checks == T) {
      no_target_spp <- subset(df_5, is.na(targetSpecies1) & !(tripType == "HVN" & fisheryType == 1))

      print("Station with missing target species in FishLine (this is always T for HVN, where fisheryType == 1, so these are not included): ")

      print(dplyr::distinct(no_target_spp, year,
                            cruise,
                            trip,
                            tripType,
                            fisheryType,
                            station,
                            targetSpecies1,
                            targetSpecies))

      no_target_spp <- subset(df_5, is.na(targetSpecies) & !(is.na(targetSpecies1)))

      if (nrow(no_target_spp > 0)) {
        print("Station where target species in FishLine is not translated (missing from target_ref | missing or wrong speciesFAO in L_Species): ")

        print(dplyr::distinct(no_target_spp, year,
                              cruise,
                              trip,
                              station,
                              targetSpecies1,
                              speciesFAO,
                              targetSpecies))
      }
    }

    print("Adding metier6")

    metier_by <-
      dplyr::join_by(
        RCG,
        year >= Start_year,
        year <= End_year,
        gear,
        targetSpecies == target,
        meshSize >= m_size_from,
        meshSize < m_size_to
      )

    df_6 <- dplyr::left_join(df_5, metier_ref, metier_by)

    df_6$metier_level_6[is.na(df_6$metier_level_6)] <-
      "MIS_MIS_0_0_0"

    df_6 <- dplyr::rename(df_6, metier6 = metier_level_6)

    if (checks == T) {

      no_metier6 <- subset(df_6, metier6 == "MIS_MIS_0_0_0")

    }

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
    # dat$MitigationDevice <- ""#is not recorded for now, but will come for gillnets in the future
    dat$incidentalByCatchMitigationDeviceFirst <-
      "NotRecorded" #This is T for now
    dat$incidentalByCatchMitigationDeviceTargetFirst <- "NotApplicable"
    dat$incidentalByCatchMitigationDeviceSecond <- "NotRecorded"
    dat$incidentalByCatchMitigationDeviceTargetSecond <- "NotApplicable"
    dat$gearDimensions <- dat$numNets * dat$lengthNets

    gear_var <- c("nationalFishingActivity","metier5", "metier6", "gear",
                  "meshSize", "selectionDevice", "selectionDeviceMeshSize",
                  "targetSpecies",
                  "incidentalByCatchMitigationDeviceFirst",
                  "incidentalByCatchMitigationDeviceTargetFirst",
                  "incidentalByCatchMitigationDeviceSecond",
                  "incidentalByCatchMitigationDeviceTargetSecond",
                  "gearDimensions")

    names(dat)[names(dat) %in% gear_var] <- paste0(record_type, names(dat)[names(dat) %in% gear_var])

    names(dat)


    # Warning ----
    if (nrow(df) != nrow(dat)) {
      print("THE NUMBER OF ROWS IN INPUT AND OUTPUT DF DO NOT MATCH - CHECK FUNCTION")
    }

    return(dat)

  }
