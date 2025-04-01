

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
  function(ref_path = "Q:/mynd/RDB/create_RDBES_data/references",
           encryptedVesselCode_path = "Q:/mynd/RDB/create_RDBES_data/RDBES_data_call_2021/output/for_production",
           years = 2016,
           sampling_scheme = "DNK_AtSea_Observer_Active")
  {


    # Input for testing ----


    # ref_path <- "C:/Users/kibi/OneDrive - Danmarks Tekniske Universitet/gits/create_RDBES_data/references/link_fishLine_sampling_designs_encryptedMatchAlle_2023.csv"
    # encryptedVesselCode_path <-
    #   "C:/Users/kibi/OneDrive - Danmarks Tekniske Universitet/gits/create_RDBES_data/output/for_production"
    # years <- 2023
    # sampling_scheme <- c("DNK_AtSea_Observer_Active", "DNK_AtSea_Observer_passive")
    # data_model_path <-
    #   "C:/Users/kibi/OneDrive - Danmarks Tekniske Universitet/gits/create_RDBES_data/input"

    # Set-up ----

    library(RODBC)
    library(sqldf)
    library(dplyr)
    library(stringr)
    library(haven)

    # Get data model ----
    FT <- fishline2rdbes::get_data_model("Fishing Trip")

    # Get link ----
    link <- read.csv(ref_path)

    link <- subset(link, DEsamplingScheme %in% sampling_scheme)

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

    tr_0 <- left_join(distinct(link[,grep("^FT|^trip", names(link), value = T)]), tr)

    # encryptedVesselCode

    tr_1 <- left_join(tr_0, select(encryptedVesselCode, tripId, VDencryptedVesselCode))

    # arrivalLocation

    tr_1 <- left_join(tr_1, locode, by = c("harbourLanding" = "harbour"))

    # No_valid_hauls

    tr_1 <- left_join(tr_1, No_valid_hauls)

    # Recode for FT ----

    ft <- tr_1

    ft$FTid <- ft$tripId
    ft$FTrecordType <- "FT"

    ft$FTencryptedVesselCode <- ft$VDencryptedVesselCode

    ft$FTsequenceNumber <- NA  #To be coded manual - depends on design
    ft$FTclustering <- "N"         #To be coded manual - depends on design
    ft$FTclusterName <- "No"     #To be coded manual - depends on design

    ft$FTsampler[ft$cruise %in% c("MON", "SEAS")] <- "Observer"
    ft$FTsampler[substr(ft$cruise, 1, 3) %in% c("BLH", "BRS", "MAK", "SIL", "SPE", "TBM") | ft$cruise == "IN-FISKER"] <-
      "SelfSampling"
    ft$FTsampler[is.na(ft$FTsampler)] <- "Observer"

    ft$FTsamplingType[ft$cruise %in% c("MON", "SEAS")] <- "AtSea"
    ft$FTsamplingType[substr(ft$cruise, 1, 3) %in% c("BLH", "BRS", "MAK", "SIL", "SPE", "TBM")] <-
      "AtSea"
    ft$FTsamplingType[is.na(ft$FTsamplingType)] <- "OnShore"

    unique(ft[c("cruise", "FTsampler", "FTsamplingType")])

    #Number of valid hauls - only M for Observer at-sea, but would be nice to include for self-sampling
    ft$FTnumberOfHaulsOrSets <- ft$numberOfHaulsOrSets
    ft$FTnumberOfHaulsOrSets[!(ft$cruise %in% c("MON", "SEAS"))] <- ""
    ft$FTnumberOfHaulsOrSets[ft$cruise %in% c("MON", "SEAS") & is.na(ft$numberOfHaulsOrSets)] <- 0


    ft$FTdepartureLocation <- "DK999" # Don't have this in FishLine - need to get from DFAD - later!
    ft$FTdepartureDate <- as.character(as.Date(ft$dateStart))
    ft$FTdepartureTime <- as.character(strftime(ft$dateStart, format = "%H:%M"))

    ft$FTarrivalLocation <- ft$arrivalLocation
    ft$FTarrivalLocation[is.na(ft$FTarrivalLocation)] <- "DK999"

    ft$FTarrivalDate <- as.character(as.Date(ft$dateEnd))
    ft$FTarrivalTime <- as.character(strftime(ft$dateEnd, format = "%H:%M"))


    ft$FTdominantLandingDate[ft$cruise %in% c("MON", "SEAS")] <- "" #This is not recorded, so it need to come from the sale slips
    ft$FTdominantLandingDate[substr(ft$cruise, 1, 3) %in% c("BLH", "BRS", "MAK", "SIL", "SPE", "TBM") |
                               ft$cruise == "IN-FISKER"]  <- "" #This is not recorded, so it need to come from the sale slips
    ft$FTdominantLandingDate[ft$cruise %in% c("IN-HIRT", "IN-LYNG", "IN-3 PART")] <- "" #This may be recorded, so it need to come from the sale slips

    # The encrypted logbook number is made during the production of the link files.
    # The encryption is made on the DFAD match_alle, so only samples with a logbook number present in DFAD'et get an encrypted one.
    # (logbook numbers in DFAD'et are cheked during the creation of the link file)
    # It may be better to have the encrypted match_alle stored somewhere along the DFAD

    if ("FTencryptedMatchAlle" %in% names(ft)) {
      ft$FTunitName <- paste(ft$cruise, ft$trip, ft$FTencryptedMatchAlle,  sep = "-")

    } else {
      ft$FTunitName <- paste(NA, ft$cruise, ft$trip, sep = "-")
    }

    ft$FTselectionMethodCluster <- ""  #To be coded manual - depends on design
    ft$FTnumberTotalClusters <- ""     #To be coded manual - depends on design
    ft$FTnumberSampledClusters <- ""   #To be coded manual - depends on design
    ft$FTselectionProbCluster <- ""    #To be coded manual - depends on design
    ft$FTinclusionProbCluster <- ""       #To be coded manual - depends on design

    ft$FTsampled <- "Y"
    # ft$FTsampled[ft$numberOfHaulsOrSets == "0"] <- "N"
    ft$FTreasonNotSampled <- ""
    # ft$FTreasonNotSampled[ft$numberOfHaulsOrSets == "0"] <- "Other"

    # Fill and select data ----
    ft <- plyr::rbind.fill(FT, ft)
    ft <- ft[ , c(names(FT), "tripId", "FTid", "year", "dateEnd")]

    ft$dateEnd <- as.character(ft$dateEnd)

    ft[is.na(ft)] <- ""

    return(list(ft, FT))

  }
