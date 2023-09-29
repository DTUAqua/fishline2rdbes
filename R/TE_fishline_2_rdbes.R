
#' FishLine 2 RDBES, Species selection (SS)
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

TE_fishline_2_rdbes <-
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
    TE <- get_data_model("Temporal Event")

    link <- read.csv(ref_path)
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

    test <- distinct(tr, dateSample, dateStart, dateEnd)

    # Design variables

    tr_1 <- left_join(link[,grep("^[TE]|^[trip]", names(link), value = T)], tr)

    tr_2 <- subset(tr_1, !is.na(dateSample))

    # Recode for LO ----

    te <- tr_2

    te$TEid <- "" # To be coded after join with DE and SD
    te$TErecordType <- "TE"

    te$TEtimeUnit <- "Day"  # Not totally true, since some

    te$TEclustering <- "N"      # Not used in this scheme
    te$TEclusterName <- "No"    # Not used in this scheme

    te$TEsampler <-  "Observer" #te$samplingMethod # DTU Aqua selects the location

    te$TEunitName <- as.character(as.Date(te$dateSample))

    te$TEsampled <- "Y"

    te <- plyr::rbind.fill(TE, te)
    te <- te[ , c(names(TE), "tripId", "TEid")]

    return(list(te, TE))

  }
