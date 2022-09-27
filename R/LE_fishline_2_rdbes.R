
#' FishLine 2 RDBES, Landing event (LE)
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
LE_fishline_2_rdbes <-
  function(ref_path = "Q:/mynd/RDB/create_RDBES_data/references",
           encryptedVesselCode_path = "Q:/mynd/kibi/RDBES/create_RDBES_data/RDBES_data_call_2022/output/for_production",
           sampling_scheme = "DNK_Market_Sampling",
           years = 2016,
           type = "everything") {
    # Input for testing ----

    # ref_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/references"
    # encryptedVesselCode_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/output/data_call_2022/for_production"
    # years <- c(2021)
    # sampling_scheme <- "DNK_Market_Sampling"
    # type <- "everything"

    # Set-up ----

    library(sqldf)
    library(dplyr)
    library(stringr)
    library(haven)

    data_model <- readRDS(paste0(ref_path, "/BaseTypes.rds"))
    link <-
      read.csv(paste0(ref_path, "/link_fishLine_sampling_designs.csv"))

    link <- subset(link, DEsamplingScheme == sampling_scheme)

    le_temp <- filter(data_model, substr(name, 1, 2) == "LE")
    le_temp_t <- c("LErecordType", t(le_temp$name)[1:nrow(le_temp)])

    trips <- unique(link$tripId[!is.na(link$tripId)])

    # Get needed stuff ----

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    samp <- sqlQuery(
      channel,
      paste(
        "select Sample.*, Trip.* FROM Sample INNER JOIN
                  Trip ON Sample.tripId = Trip.tripId
         WHERE (Trip.year between ",
        min(years),
        " and ",
        max(years),
        ")
                and Trip.tripId in (",
        paste(trips, collapse = ","),
        ")",
        sep = ""
      )
    )
    close(channel)

    samp <- subset(samp, !(is.na(dfuArea)))

    channel <- odbcConnect("FishLine")
    area <- sqlQuery(
      channel,
      paste("SELECT DFUArea, areaICES FROM L_DFUArea",
        sep = ""
      )
    )
    close(channel)

    # Get encryptedVesselCode

    encryptedVesselCode <-
      read.csv(paste0(
        encryptedVesselCode_path,
        "/DNK_",
        min(years),
        "_",
        max(years),
        "_HVD.csv"
      ),
      sep = ";"
      )

    # Get LOCODE's for arrival / departure location

    channel <- odbcConnect("FishLine")
    locode <- sqlQuery(
      channel,
      paste("SELECT harbour, harbourEU FROM L_Harbour",
        sep = ""
      )
    )
    close(channel)

    locode$arrivalLocation <- locode$harbourEU

    # Add needed stuff ----

    samp$dfuArea <- as.character(samp$dfuArea)

    le <- left_join(samp, area, by = c("dfuArea" = "DFUArea"))

    # encryptedVesselCode

    le_1 <-
      left_join(
        le,
        select(encryptedVesselCode, tripId, VDencryptedVesselCode)
      )

    # arrivalLocation

    le_1 <-
      left_join(le_1, locode, by = c("harbourLanding" = "harbour"))


    # Recode for FO ----

    le <- le_1

    le$LEid <- le$sampleId
    le$LErecordType <- "LE"

    le$LEencryptedVesselCode <- le$VDencryptedVesselCode

    le$LEmixedTrip[le$samplingType == "D" |
      is.na(le$samplingType)] <- "Y"
    le$LEmixedTrip[le$samplingType == "M"] <- "N"
    le$LEmixedTrip[le$LEencryptedVesselCode == "DNK - Unknown vessel"] <-
      "Y"

    le$LEencryptedVesselCode[le$LEmixedTrip == "Y"] <- ""

    distinct(le, samplingType, LEencryptedVesselCode, LEmixedTrip)

    le$LEsequenceNumber <-
      le$station # To be coded manual - depends on design
    le$LEhaulNumber <- ""

    le$LEstratification <-
      "N" # To be coded manual - depends on design
    le$LEstratumName <-
      "U" # To be coded manual - depends on design
    le$LEclustering <-
      "N" # To be coded manual - depends on design
    le$LEclusterName <-
      "U" # To be coded manual - depends on design

    le$LEsampler <- "Observer"  #le$samplingMethod # That is not completely TRUE

    le$LEfullTripAvailable <- "N"

    le$LEcatchReg <- "Lan"

    le$LElocode <- ""
    le$LElocationName <- ""
    le$LElocationType <- ""
    le$LEcountry <- "DK"

    le$LEdate <- as.Date(le$dateGearEnd)
    le$LEtime <- ""

    le$LEexclusiveEconomicZoneIndicator <- ""

    le$LEarea <- le$areaICES
    le$LErectangle <- le$statisticalRectangle
    le$LErectangle[is.na(le$LErectangle)] <- ""
    le$LEgsaSubarea <- "NotApplicable"
    le$LEjurisdictionArea <- ""

    le$LEnationalFishingActivity <- ""
    le$LEmetier5 <- ""
    le$LEmetier6 <- "MIS_MIS_0_0_0"
    le$LEgear <- le$gearType
    le$LEgear[is.na(le$LEgear)] <- "MIS"
    le$LEgear[le$gearType == "LL"] <- "LLS"
    le$LEgear[le$gearType == "TBN"] <- "OTB"
    le$LEgear[le$gearType == "FIX"] <- "FPO"
    le$LEgear[le$gearType == "LHP"] <- "LHM"
    le$LEmeshSize <- le$meshSize
    le$LEselectionDevice <- ""
    le$LEselectionDeviceMeshSize <- ""

    le$LEtargetSpecies <- ""

    le$LEmitigationDevice <- "NotRecorded"

    le$LEgearDimensions <- ""

    le$LEobservationCode <- "NotRecorded"

    le$LEnumberTotal <-
      "" # To be coded manual - depends on design
    le$LEnumberSampled <-
      "" # To be coded manual - depends on design
    le$LEselectionProb <-
      "" # To be coded manual - depends on design
    le$LEinclusionProb <-
      "" # To be coded manual - depends on design
    le$LEselectionMethod <-
      "NotApplicable" # To be coded manual - depends on design

    le$LEunitName <-
      paste(le$cruise, le$trip, le$station, sep = "-")

    le$LEselectionMethodCluster <-
      "" # To be coded manual - depends on design
    le$LEnumberTotalClusters <-
      "" # To be coded manual - depends on design
    le$LEnumberSampledClusters <-
      "" # To be coded manual - depends on design
    le$LEselectionProbCluster <-
      "" # To be coded manual - depends on design
    le$LEinclusionProbCluster <-
      "" # To be coded manual - depends on design

    le$LEsampled <- "Y"
    le$LEreasonNotSampled <- ""

    # For now only including Y - N requires manual coding
    # Should be 0, but that is not possible at the moment
    # le$LEsampled[ft$cruise %in% c("MON", "SEAS") & is.na(ft$numberOfHaulsOrSets)] <- 0


    if (type == "only_mandatory") {
      le_temp_optional <-
        filter(data_model, substr(name, 1, 2) == "LE" & min == 0)
      le_temp_optional_t <-
        factor(t(le_temp_optional$name)[1:nrow(le_temp_optional)])

      for (i in levels(ft_temp_optional_t)) {
        eval(parse(text = paste0("le$", i, " <- ''")))
      }
    }

    LE <-
      select(le, one_of(le_temp_t), tripId, sampleId, LEid, year)

    return(list(LE, le_temp, le_temp_t))
  }
