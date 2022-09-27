
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


FT_fishline_2_rdbes <-
  function(data_model_baseTypes_path = "Q:/mynd/RDB/create_RDBES_data/references",
           encryptedVesselCode_path = "Q:/mynd/RDB/create_RDBES_data/RDBES_data_call_2021/output/for_production",
           years = 2016,
           cruises = c("MON", "SEAS", "IN-HIRT"),
           type = "everything")
  {


    # Input for testing ----

ref_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/references"
encryptedVesselCode_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/output/data_call_2022/for_production"
years <- 2021
sampling_scheme <- "DNK_AtSea_Observer_Active"
type <- "everything"

    # Set-up ----

    library(RODBC)
    library(sqldf)
    library(dplyr)
    library(stringr)
    library(haven)

    data_model <- readRDS(paste0(ref_path, "/BaseTypes.rds"))
    link <-
      read.csv(paste0(ref_path, "/link_fishLine_sampling_designs.csv"))

    link <- subset(link, DEsamplingScheme == sampling_scheme)

    ft_temp <- filter(data_model, substr(name, 1, 2) == "FT")
    ft_temp_t <- c("FTrecordType", t(ft_temp$name)[1:nrow(ft_temp)])

    trips <- unique(link$tripId[!is.na(link$tripId)])

    # Get needed stuff ----

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    tr <- sqlQuery(
      channel,
      paste(
        "select * FROM dbo.Trip
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
    sa <- sqlQuery(
      channel,
      paste(
        "SELECT  sampleId, tripId, year, cruise, gearQuality FROM Sample
         WHERE gearQuality = 'V' and (Sample.year between ",
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

    No_valid_hauls <- summarise(group_by(sa, tripId), numberOfHaulsOrSets = length(unique(sampleId)))

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
      sep = ";")

    # Get LOCODE's for arrival / departure location

    channel <- odbcConnect("FishLine")
    locode <- sqlQuery(
      channel,
      paste(
        "SELECT harbour, harbourEU FROM L_Harbour",
        sep = ""
      )
    )
    close(channel)

    locode$arrivalLocation <- locode$harbourEU

    # Add needed stuff ----

    tr_0 <- left_join(link[,grep("^[FT]|^[trip]", names(link), value = T)], tr)

    # encryptedVesselCode

    tr_1 <- left_join(tr_0, select(encryptedVesselCode, tripId, VDencryptedVesselCode))

    # arrivalLocation

    tr_1 <- left_join(tr_1, locode, by = c("harbourLanding" = "harbour"))

    # No_valid_hauls

    tr_1 <- left_join(tr_1, No_valid_hauls)

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
               dateEnd,
               arrivalLocation,
               numberOfHaulsOrSets, FTstratification, FTstratumName,
               FTnumberTotal, FTnumberSampled, FTselectionProb,
               FTinclusionProb, FTselectionMethod, FTsampled,
               FTnoSampReason, FOstratification, FOstratumName,
               FOnumberTotal, FOnumberSampled, FOselectionProb,
               FOinclusionProb, FOselectionMethod, FOsampled, FOnoSampReason)

    ft$FTid <- ft$tripId
    ft$FTrecordType <- "FT"

    ft$FTencryptedVesselCode <- ft$VDencryptedVesselCode

    ft$FTsequenceNumber <- NA  #To be coded manual - depends on design
    ft$FTclustering <- "N"         #To be coded manual - depends on design
    ft$FTclusterName <- "No"     #To be coded manual - depends on design

    ft$FTsampler[ft$cruise %in% c("MON", "SEAS")] <- "Observer"
    ft$FTsampler[substr(ft$cruise, 1, 3) %in% c("BLH", "BRS", "MAKK", "SIL", "SPE", "TBM")] <-
      "Self-Sampling"
    ft$FTsampler[is.na(ft$FTsampler)] <- "Observer"

    ft$FTsamplingType[ft$cruise %in% c("MON", "SEAS")] <- "AtSea"
    ft$FTsamplingType[substr(ft$cruise, 1, 3) %in% c("BLH", "BRS", "MAKK", "SIL", "SPE", "TBM")] <-
      "AtSea"
    ft$FTsamplingType[is.na(ft$FTsamplingType)] <- "OnShore"

    unique(ft[c("cruise", "FTsampler", "FTsamplingType")])

    #Number of valid hauls - only M for Observer at-sea, but would be nice to include for self-sampling
    ft$FTnumberOfHaulsOrSets <- ft$numberOfHaulsOrSets
    ft$FTnumberOfHaulsOrSets[!(ft$cruise %in% c("MON", "SEAS"))] <- ""
    ft$FTnumberOfHaulsOrSets[ft$cruise %in% c("MON", "SEAS") & is.na(ft$numberOfHaulsOrSets)] <- 0


    ft$FTdepartureLocation <- "DK999" # Don't have this in FishLine - need to get from DFAD - later!
    ft$FTdepartureDate <- as.Date(ft$dateStart)
    ft$FTdepartureTime <- strftime(ft$dateStart, format = "%H:%M")

    ft$FTarrivalLocation <- ft$arrivalLocation
    ft$FTarrivalLocation[is.na(ft$FTarrivalLocation)] <- "DK999"

    ft$FTarrivalDate <- as.Date(ft$dateEnd)
    ft$FTarrivalTime <- strftime(ft$dateEnd, format = "%H:%M")

    ft$FTunitName <- paste(ft$cruise, ft$trip, sep = "-")

    ft$FTselectionMethodCluster <- ""  #To be coded manual - depends on design
    ft$FTnumberTotalClusters <- ""     #To be coded manual - depends on design
    ft$FTnumberSampledClusters <- ""   #To be coded manual - depends on design
    ft$FTselectionProbCluster <- ""    #To be coded manual - depends on design
    ft$FTinclusionProbCluster <- ""       #To be coded manual - depends on design

    ft$FTsampled <- "Y"
    # ft$FTsampled[ft$numberOfHaulsOrSets == "0"] <- "N"
    ft$FTreasonNotSampled <- ""
    # ft$FTreasonNotSampled[ft$numberOfHaulsOrSets == "0"] <- "Other"

    if (type == "only_mandatory") {
      ft_temp_optional <-
        filter(data_model, substr(name, 1, 2) == "FT" & min == 0)
      ft_temp_optional_t <-
        factor(t(ft_temp_optional$name)[1:nrow(ft_temp_optional)])

      for (i in levels(ft_temp_optional_t)) {
        eval(parse(text = paste0("ft$", i, " <- ''")))

      }
    }

    FT <- select(ft, one_of(ft_temp_t), tripId, FTid, year, dateEnd)

    return(list(FT, ft_temp, ft_temp_t))

  }
