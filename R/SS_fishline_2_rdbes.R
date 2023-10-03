
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

SS_fishline_2_rdbes <-
  function(ref_path = "Q:/mynd/RDB/create_RDBES_data/references",
           sampling_scheme = "DNK_Market_Sampling",
           years = 2016,
           xx = "observer at-sea",
           specieslist_name = "DNK_Market_Sampling_2021",
           data_model_path){


    # Input for testing ----

    # ref_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/references"
    # sampling_scheme <- "DNK_AtSea_Observer_Active"
    # years <- c(2021)
    # specieslist_name <- "DNK_AtSea_Observer_2021"
    # xx <- "observer at-sea"

    # Set-up ----

    library(RODBC)
    library(sqldf)
    library(plyr, include.only = c("rbind.fill"))
    library(dplyr)
    library(stringr)
    library(haven)
    library(lubridate)

    #data_model <- readRDS(paste0(ref_path, "/BaseTypes.rds"))
    SS <- get_data_model("Species Selection", data_model_path = data_model_path)


    link <- read.csv(ref_path)
    link <- subset(link, DEsamplingScheme == sampling_scheme)

    trips <- unique(link$tripId[!is.na(link$tripId)])

    # Get needed stuff ----

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    st <- sqlQuery(
      channel,
      paste(
        "select * FROM fishlineDW.dbo.Sample
         WHERE (Sample.year between ", min(years), " and ", max(years) , ")
                and Sample.tripId in (", paste(trips, collapse = ","),
        ")",
        sep = ""
      )
    )
    close(channel)

    st$dateGearEnd <- force_tz(st$dateGearEnd, tzone = "UTC")

    if (xx == "observer at-sea") {

      st <- filter(st, gearQuality == "V" & catchRegistration == "ALL" & speciesRegistration == "ALL")
    }

    # Recode for LO ----

    ss <- st

    ss$SSid <- "" # To be coded after join with DE and SD
    ss$SSrecordType <- "SS"

    ss$SSstratification <- "N"  # We never select species
    ss$SSstratumName <- "U"

    ss$SSclustering <- "N"      # Not used in this scheme
    ss$SSclusterName <- "No"    # Not used in this scheme

    if (xx == "other") {
      ss$SSobservationActivityType[ss$tripType == "HVN"] <- "Sort"
      ss$SScatchFraction[ss$tripType == "HVN"] <- "Lan"
      ss$SSobservationType[ss$tripType == "HVN"] <- "Volume"
      ss$SSsampler[ss$tripType == "HVN"] <-  "" # Not relevant, since we do not
      ss$SSuseForCalculateZero[ss$tripType == "HVN"] <- "N"

      ss$SSselectionMethod <- "NotApplicable"

    }

    if ( xx == "observer at-sea") {

      ss$SSobservationActivityType <- "Sort"

      Lan <- mutate(ss, SScatchFraction = "Lan")
      Dis <- mutate(ss, SScatchFraction = "Dis")
      ss <- bind_rows(Lan, Dis)

      ss$SSobservationType <- "Volume"
      ss$SSsampler <-  "Observer"
      ss$SSuseForCalculateZero <- "Y" # Not totally T eg. for partial selection


      ss$SSnumberTotal <- 1
      ss$SSnumberSampled <- 1

      ss$SSselectionMethod <- "CENSUS" # Not totally T eg. for partial selection


    }

    ss$SSspeciesListName <- specieslist_name

    ss$SSunitName <- paste(ss$cruise, ss$trip, ss$station, sep = "-")

    ss$SSsampled <- "Y"

    ss <- plyr::rbind.fill(SS, ss)
    ss <- ss[ , c(names(SS), "tripId", "sampleId", "SSid")]

    return(list(ss, SS))

  }
