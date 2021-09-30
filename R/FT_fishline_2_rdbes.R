
#' FishLine 2 RDBES, Fishing trip (FT)
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
#'
#'
#'

FT_fishline_2_rdbes <-
  function(data_model_baseTypes_path = "Q:/mynd/RDB/create_RDBES_data/references",
           encryptedVesselCode_path = "Q:/mynd/RDB/create_RDBES_data/RDBES_data_call_2021/output/for_production",
           years = 2016,
           cruises = c("MON", "SEAS", "IN-HIRT"),
           type = "everything",
           sampler = "Observer",
           samplingType = "At-Sea")
  {


    # Input for testing ----

    data_model_baseTypes_path <- "Q:/mynd/RDB/create_RDBES_data/references"
    encryptedVesselCode_path <- "Q:/mynd/RDB/create_RDBES_data/RDBES_data_call_2021/output/for_production"
    years <- c(2018:2020)
    cruises <- c("MON", "SEAS")
    sampler <- "Observer"
    type <- "everything"
    samplingType <- "At-Sea"

    # Set-up ----

    library(RODBC)
    library(sqldf)
    library(dplyr)
    library(stringr)
    library(haven)

    data_model <- readRDS(paste0(data_model_baseTypes_path, "/BaseTypes.rds"))

    ft_temp <- filter(data_model, substr(name, 1, 2) == "FT")
    ft_temp_t <- c("VDrecordType", t(ft_temp$name)[1:nrow(ft_temp)])

    # Get needed stuff ----

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    tr <- sqlQuery(
      channel,
      paste(
        "select * FROM dbo.Trip
         WHERE (Trip.year between ", min(years), " and ", max(years) , ")
                and Trip.cruise in ('", paste(cruises, collapse = "','"),
        "')",
        sep = ""
      )
    )
    close(channel)

    # Get encryptedVesselCode

    encryptedVesselCode <- read.csv(paste0(encryptedVesselCode_path, "/DNK_", min(years), "_", max(years), "_HVD.csv"), sep = ";")

    # Add needed stuff ----
    # encryptedVesselCode

    tr_1 <- left_join(tr, select(encryptedVesselCode, tripId, VDencryptedVesselCode))

    # Recode for FT ----

    ft <-
      distinct(tr_1,
               tripId,
               tripType,
               cruise,
               trip,
               VDencryptedVesselCode,
               year,
               dateStart,
               dateEnd)

    ft$FTid <- ft$tripId
    ft$FTrecordType <- "VD"

    ft$FTencryptedVesselCode <- ft$VDencryptedVesselCode

    ft$FTsequenceNumber <- NA
    ft$FTstratification <- NA
    ft$FTstratumName <- NA
    FTclustering <- NA
    ft$FTclusterName <- NA

    ft$FTsampler <- sampler
    ft$FTsamplingType <- samplingType

    ft$FTnumberOfHaulsOrSets <- NA

    ft$FTdepartureLocation <- NA # need to be added
    ft$FTdepartureDate <- as.Date(ft$dateStart)
    ft$FTdepartureTime <- strftime(ft$dateStart, format = "%H:%M")

    ft$FTarrivalLocation <- NA
    ft$FTarrivalDate <- as.Date(ft$dateEnd)
    ft$FTarrivalTime <- strftime(ft$dateEnd, format = "%H:%M")

    ft$FTnumberTotal <- NA
    ft$FTnumberSampled <- NA
    ft$FTselectionProb <- NA
    ft$FTinclusionProb <- NA
    ft$FTselectionMethod <- NA
    ft$FTunitName <- paste(ft$cruise, ft$trip, sep = "-")

    ft$FTselectionMethodCluster <- NA
    ft$FTnumberTotalClusters <- NA
    ft$FTnumberSampledClusters <- NA
    ft$FTselectionProbCluster <- NA
    FTinclusionProbCluster <- NA
    FTsampled <- "Y"
    FTreasonNotSampled <- NA

    if (type == "only_mandatory") {
      ft_temp_optional <-
        filter(data_model, substr(name, 1, 2) == "FT" & min == 0)
      ft_temp_optional_t <-
        factor(t(ft_temp_optional$name)[1:nrow(ft_temp_optional)])

      for (i in levels(ft_temp_optional_t)) {
        eval(parse(text = paste0("ft$", i, " <- NA")))

      }
    }

    VD <- select(ft, one_of(vd_temp_t), tripId, FTid)

    return(list(FT, ft_temp, ft_temp_t))

  }
