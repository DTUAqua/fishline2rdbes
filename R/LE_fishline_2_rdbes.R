
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
           encryptedVesselCode_path = "Q:/mynd/kibi/RDBES/create_RDBES_data_old/output/data_call_2022/for_production",
           sampling_scheme = "DNK_Market_Sampling",
           years = 2016) {
    # Input for testing ----

    # ref_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/references"
    # encryptedVesselCode_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/output/data_call_2022/for_production"
    # years <- c(2021)
    # sampling_scheme <- "DNK_Market_Sampling"

    # Set-up ----

    library(sqldf)
    library(plyr, include.only = c("rbind.fill"))
    library(dplyr)
    library(stringr)
    library(haven)
    library(lubridate)

    #data_model <- readRDS(paste0(ref_path, "/BaseTypes.rds"))
    LE <- get_data_model("Landing event")


    link <- read.csv(paste0(ref_path, "/link_fishLine_sampling_designs.csv"))
    link <- subset(link, DEsamplingScheme == sampling_scheme)

    trips <- unique(link$tripId[!is.na(link$tripId)])

    # Get needed stuff ----

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    samp <- sqlQuery(
      channel,
      paste(
        "select Sample.*, Trip.* FROM fishlineDW.dbo.Sample INNER JOIN
                  fishlineDW.dbo.Trip ON Sample.tripId = Trip.tripId
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

    samp$dateGearEnd <- force_tz(samp$dateGearEnd, tzone = "UTC")
    samp <- subset(samp, !(is.na(dfuArea)))

    channel <- odbcConnect("FishLine")
    area <- sqlQuery(
      channel,
      paste("SELECT DFUArea, areaICES FROM fishline.dbo.L_DFUArea",
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
      paste("SELECT harbour, harbourEU FROM fishline.dbo.L_Harbour",
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

    le$LEsequenceNumber <- le$station # To be coded manual - depends on design

    le$LEstratification <- "N" # To be coded manual - depends on design
    le$LEstratumName <- "U" # To be coded manual - depends on design
    le$LEclustering <- "N" # To be coded manual - depends on design
    le$LEclusterName <- "U" # To be coded manual - depends on design

    le$LEsampler <- "Observer"  #le$samplingMethod # That is not completely TRUE

    le$LEfullTripAvailable <- "N"

    le$LEcatchReg <- "Lan"
    le$LEcountry <- "DK"

    le$LEdate <- as.Date(le$dateGearEnd)

    le$LEarea <- le$areaICES
    le$LErectangle <- le$statisticalRectangle
    le$LErectangle[is.na(le$LErectangle)] <- ""
    le$LEgsaSubarea <- "NotApplicable"

    le$LEmetier6 <- "MIS_MIS_0_0_0"
    le$LEgear <- le$gearType
    le$LEgear[is.na(le$LEgear)] <- "MIS"
    le$LEgear[le$gearType == "LL"] <- "LLS"
    le$LEgear[le$gearType == "TBN"] <- "OTB"
    le$LEgear[le$gearType == "FIX"] <- "FPO"
    le$LEgear[le$gearType == "LHP"] <- "LHM"
    le$LEmeshSize <- le$meshSize

    le$LEmitigationDevice <- "NotRecorded"

    le$LEobservationCode <- "NotRecorded"

    le$LEselectionMethod <- "NotApplicable" # To be coded manual - depends on design

    le$LEunitName <- paste(le$cruise, le$trip, le$station, sep = "-")

    le$LEsampled <- "Y"


    le <- plyr::rbind.fill(LE, le)
    le <- le[ , c(names(LE), "tripId", "sampleId", "LEid", "year")]

    return(list(le, LE))
  }
