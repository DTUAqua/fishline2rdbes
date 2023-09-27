
#' FishLine 2 RDBES, Location selection (LO)
#'
#' @description Converts samples data from national database (fishLine) to RDBES.
#' Data model v. 1.19.13
#'
#' @param path_to_data_model_baseTypes Where to find the baseTypes for the data model
#' @param year years needed
#' @param cruises Name of cruises in national database
#' @param type only_mandatory | everything
#'
#' @author Kirsten Birch Håkansson, DTU Aqua
#'
#' @importFrom plyr rbind.fill
#'
#' @return
#' @export
#'
#'
#' @examples
#'
#'
#'



LO_fishline_2_rdbes <-
  function(ref_path = "Q:/mynd/kibi/RDBES/create_RDBES_data/references",
           sampling_scheme = "DNK_Market_Sampling",
           years = 2016){

    # Input for testing ----

    # ref_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/references"
    # sampling_scheme <- "DNK_Market_Sampling"
    # years <- c(2021)

    # Set-up ----

    library(RODBC)
    library(sqldf)
    library(plyr, include.only = c("rbind.fill"))
    library(dplyr)
    library(stringr)
    library(haven)
    library(lubridate)

    #data_model <- readRDS(paste0(ref_path, "/BaseTypes.rds"))
    LO <- get_data_model("Location")

    link <- read.csv(paste0(ref_path, "/link_fishLine_sampling_designs.csv"))
    link <- subset(link, DEsamplingScheme == sampling_scheme)

    trips <- unique(link$tripId[!is.na(link$tripId)])

    # Get needed stuff ----

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    tr <- sqlQuery(
      channel,
      paste(
        "select * FROM FishLineDW.dbo.Trip
         WHERE (Trip.year between ", min(years), " and ", max(years) , ")
                and Trip.tripId in (", paste(trips, collapse = ","),
        ")",
        sep = ""
      )
    )
    close(channel)

    tr$dateEnd <- force_tz(tr$dateEnd, tzone = "UTC")
    tr$dateSample <- force_tz(tr$dateSample, tzone = "UTC")

    test <- distinct(tr, harbourSample, harbourLanding)


    # Get LOCODE's for sampling location

    channel <- odbcConnect("FishLine")
    locode <- sqlQuery(
      channel,
      paste(
        "SELECT harbour, harbourEU FROM FishLine.dbo.L_Harbour",
        sep = ""
      )
    )
    close(channel)

    locode$LOlocode <- locode$harbourEU

    # Add needed stuff ----
    #LOCODE

    tr_1 <- left_join(tr, locode, by = c("harbourSample" = "harbour"))

    # Design variables

    tr_2 <- left_join(link, tr_1)

    tr_2 <- subset(tr_2, !is.na(LOlocode))

    tr_3 <- distinct(tr_2[,grep("^[LO]|^[trip]", names(tr_2), value = T)])

    # Recode for LO ----

    lo <- tr_3

    lo$LOid <- "" # To be coded after join with DE and SD
    lo$LOrecordType <- "LO"

    # lo$LOsequenceNumber <- NA   # To be coded after join with DE and SD
    lo$LOclustering <- "N"      # Not used in this scheme
    lo$LOclusterName <- "No"    # Not used in this scheme

    lo$LOsampler <-  "Observer" # DTU Aqua selects the locationa

    lo$LOunitName <- lo$LOlocode

    lo$LOsampled <- "Y"

    lo <- plyr::rbind.fill(LO, lo)
    lo <- lo[ , c(names(LO), "tripId", "LOid")]

    return(list(lo, LO))

  }
