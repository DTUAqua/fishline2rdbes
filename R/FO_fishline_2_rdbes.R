
#' FishLine 2 RDBES, Fishing operation (FO)
#'
#' @description Converts samples data from national database (fishLine) to RDBES.
#' Data model v. 1.19
#'
#' @param path_to_data_model_baseTypes Where to find the baseTypes for the data model
#' @param year years needed
#' @param cruises Name of cruises in national database
#' @param type only_mandatory | everything
#'
#' @author Kirsten Birch Håkansson, DTU Aqua
#'
#' @return
#' @export
#'
#'
#' @examples




FO_fishline_2_rdbes <-
  function(ref_path = "Q:/mynd/RDB/create_RDBES_data/references",
           sampling_scheme = "DNK_Market_Sampling",
           years = 2016,
           data_model_path) {
    # Input for testing ----

    # ref_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data_old/references"
    # years <- 2021
    # sampling_scheme <- "DNK_AtSea_Observer_Active"
    # data_model_path <-
    #   "Q:/dfad/data/Data/RDBES/sample_data/create_RDBES_data/input"

    # Set-up ----

    library(sqldf)
    library(RODBC)
    library(dplyr)
    library(stringr)
    library(haven)

    FO <-
      get_data_model("Fishing Operation", data_model_path = data_model_path)

    # Get link ----
    link <- read.csv(ref_path)

    link <- subset(link, DEsamplingScheme == sampling_scheme)

    trips <- unique(link$tripId[!is.na(link$tripId)])

    # Get needed stuff ----

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    samp <- sqlQuery(
      channel,
      paste(
        "select * FROM dbo.Sample
         WHERE (Sample.year between ",
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
      paste("SELECT DFUArea, areaICES FROM L_DFUArea",
        sep = ""
      )
    )
    close(channel)



    # Add needed stuff ----

    fo_0 <- left_join(link[,grep("^[FO]|^[tripId]|^[sampleId]", names(link), value = T)], samp)

    fo <- left_join(fo_0, area, by = c("dfuArea" = "DFUArea"))


    # Recode for FO ----

    fo <- fo

    fo$FOid <- fo$sampleId
    fo$FOrecordType <- "FO"

    fo$FOsequenceNumber <-
      fo$sampleId # To be coded manual - depends on design
    fo$FOclustering <-
      "N" # To be coded manual - depends on design
    fo$FOclusterName <-
      "No" # To be coded manual - depends on design

    fo$FOsampler[fo$cruise %in% c("MON", "SEAS")] <- "Observer"
    fo$FOsampler[substr(fo$cruise, 1, 3) %in% c("BLH", "BRS", "MAK", "SIL", "SPE", "TBM")] <-
      "Self-Sampling"

    fo$FOvalidity <- fo$gearQuality
    fo$FOvalidity[is.na(fo$FOvalidity)] <- "I"

    fo$FOcatchReg[fo$cruise %in% c("MON", "SEAS") &
      fo$catchRegistration == "ALL" &
      fo$speciesRegistration == "ALL"] <- "All"
    fo$FOcatchReg[fo$cruise %in% c("MON", "SEAS") &
      fo$catchRegistration == "DIS" &
      fo$speciesRegistration == "ALL"] <- "Dis"
    fo$FOcatchReg[fo$cruise %in% c("MON", "SEAS") &
      fo$catchRegistration == "LAN" &
      fo$speciesRegistration == "ALL"] <- "Lan"
    fo$FOcatchReg[fo$cruise %in% c("MON", "SEAS") &
      fo$catchRegistration == "NON" |
      fo$speciesRegistration != "ALL"] <- "None"
    fo$FOcatchReg[fo$cruise %in% c("MON", "SEAS") &
      is.na(fo$catchRegistration) |
      is.na(fo$speciesRegistration)] <- "None"
    fo$FOcatchReg[substr(fo$cruise, 1, 3) %in%
                    c("BLH", "BRS", "MAK", "SIL", "SPE", "TBM")] <-
      "Lan"

    test <-
      summarise(
        group_by(
          fo,
          cruise,
          catchRegistration,
          speciesRegistration,
          FOcatchReg
        ),
        no_hauls = length(unique(sampleId))
      )

    # Start agg levels

    fo_t <- subset(fo, tripType == "HVN")
    fo_h <- subset(fo, tripType == "SØS")

    # Haul level
    if (nrow(fo_h > 0)) {
      fo_h$FOaggregationLevel <- "H"

      fo_h$FOstartDate <- as.character(as.Date(fo_h$dateGearStart))
      fo_h$FOstartTime <-
        as.character(strftime(fo_h$dateGearStart, format = "%H:%M"))
      fo_h$FOendDate <- as.character(as.Date(fo_h$dateGearEnd))
      fo_h$FOendTime <-
        as.character(strftime(fo_h$dateGearEnd, format = "%H:%M"))

      fo_h$FOduration[fo_h$cruise %in% c("MON", "SEAS")] <-
        as.character(round(as.numeric(fo_h$fishingtime), digits = 0))

      fo_h$FOdurationSource[fo_h$cruise %in% c("MON", "SEAS")] <-
        "Crew"
      fo_h$FOdurationSource[substr(fo_h$cruise, 1, 3) %in%
                              c("BLH", "BRS", "MAK", "SIL", "SPE", "TBM")] <-
        "Data"

      fo_h$FOstartLat <-
        as.character(round(fo_h$latPosStartDec, digits = 5))
      fo_h$FOstartLon <-
        as.character(round(fo_h$lonPosStartDec, digits = 5))
      fo_h$FOstopLat <-
        as.character(round(fo_h$latPosEndDec, digits = 5))
      fo_h$FOstopLon <-
        as.character(round(fo_h$lonPosEndDec, digits = 5))

      fo_h$FOfishingDepth <-
        as.character(round(fo_h$depthAveGear, digits = 0))
      fo_h$FOfishingDepth[is.na(fo_h$FOfishingDepth)] <- ""
    }

    # Trip level
    if (nrow(fo_t > 0)) {
      fo_t$FOaggregationLevel <- "T"

      fo_t$FOstartDate <- ""
      fo_t$FOstartTime <- ""
      fo_t$FOendDate <- as.Date(fo_t$dateGearEnd)
      fo_t$FOendTime <- ""

      fo_t$FOduration <- ""
      fo_t$FOdurationSource <- "Data"

      fo_t$FOstartLat <- ""
      fo_t$FOstartLon <- ""
      fo_t$FOstopLat <- ""
      fo_t$FOstopLon <- ""

      fo_t$FOfishingDepth <- ""
    }


    fo <- bind_rows(fo_h, fo_t)

    # End agg levels

    fo$FOhandlingTime <- ""

    fo$FOexclusiveEconomicZoneIndicator <- ""

    fo$FOarea <- fo$areaICES
    fo$FOarea[is.na(fo$FOarea) & fo$dfuArea == "4L"] <- "27.4.b"
    fo$FOrectangle <- fo$statisticalRectangle
    fo$FOrectangle[is.na(fo$FOrectangle)] <- ""
    fo$FOgsaSubarea <- "NotApplicable"
    fo$FOjurisdictionArea <- ""

    fo$FOwaterDepth <- ""

    fo$FOnationalFishingActivity <- ""
    fo$FOmetier5 <- ""
    fo$FOmetier6 <- "MIS_MIS_0_0_0"
    fo$FOgear <- fo$gearType
    fo$FOgear[is.na(fo$FOgear)] <- "MIS"
    fo$FOgear[fo$gearType == "LL"] <- "LLS"
    fo$FOgear[fo$gearType == "TBN"] <- "OTB"
    fo$FOgear[fo$gearType == "FIX"] <- "FPO"
    fo$FOgear[fo$gearType == "LHP"] <- "LHM"
    fo$FOmeshSize <- fo$meshSize
    fo$FOselectionDevice <- ""
    fo$FOselectionDeviceMeshSize <- ""
    fo$FOtargetSpecies <- ""

    fo$FOincidentalByCatchMitigationDeviceFirst <- "NotRecorded"
    fo$FOincidentalByCatchMitigationDeviceTargetFirst <-
      "NotApplicable"
    fo$FOincidentalByCatchMitigationDeviceSecond <- "NotRecorded"
    fo$FOincidentalByCatchMitigationDeviceTargetSecond <-
      "NotApplicable"

    fo$FOgearDimensions <- ""

    fo$FOobservationCode <- "So"

    fo$FOunitName <-
      paste(fo$cruise, fo$trip, fo$station, sep = "-")

    fo$FOselectionMethodCluster <-
      "" # To be coded manual - depends on design
    fo$FOnumberTotalClusters <-
      "" # To be coded manual - depends on design
    fo$FOnumberSampledClusters <-
      "" # To be coded manual - depends on design
    fo$FOselectionProbCluster <-
      "" # To be coded manual - depends on design
    fo$FOinclusionProbCluster <-
      "" # To be coded manual - depends on design

    fo$FOsampled <- "Y"
    fo$FOreasonNotSampled <- ""

    fo$FOsampled[fo$gearQuality == "I"] <- "N"
    fo$FOreasonNotSampled[fo$gearQuality == "I"] <- "Other"

    fo$FOsampled[fo$catchRegistration == "NON"] <- "N"
    fo$FOreasonNotSampled[fo$catchRegistration == "NON"] <- "Other"

    fo$FOsampled[fo$speciesRegistration %in% c("NON", "PAR")] <- "N"
    fo$FOreasonNotSampled[fo$speciesRegistration %in% c("NON", "PAR")] <-
      "Other"

    test_2 <-
      summarise(
        group_by(
          fo,
          cruise,
          gearQuality,
          catchRegistration,
          speciesRegistration,
          FOsampled
        ),
        no_hauls = length(unique(sampleId))
      )

    # For now only including Y - N requires manual coding
    # Should be 0, but that is not possible at the moment
    # fo$FOsampled[ft$cruise %in% c("MON", "SEAS") & is.na(ft$numberOfHaulsOrSets)] <- 0

    # Fill and select data ----
    fo <- plyr::rbind.fill(FO, fo)
    fo <- fo[ , c(names(FO), "tripId", "sampleId", "FOid", "year", "cruise", "trip")]

    fo[is.na(fo) ] <- ""

    return(list(fo, FO))

  }
