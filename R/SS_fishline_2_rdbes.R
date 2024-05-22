
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
           specieslist_name = "DNK_Market_Sampling_2021"){


    # Input for testing ----

    # ref_path <- "Q:/dfad/data/Data/RDBES/sample_data/create_RDBES_data/references/link_fishLine_sampling_designs_2023.csv"
    # years <- 2023
    # sampling_scheme <- c("DNK_Industrial_Sampling", "Baltic SPF regional", "DNK_Pelagic_Sampling_HUC")
    # data_model_path <-
    #   "Q:/dfad/data/Data/RDBES/sample_data/fishline2rdbes/data"
    # specieslist_name <- "yyy"
    # xx <- " "

    # Set-up ----

    library(RODBC)
    library(sqldf)
    library(plyr, include.only = c("rbind.fill"))
    library(dplyr)
    library(stringr)
    library(haven)
    library(lubridate)

    #data_model <- readRDS(paste0(ref_path, "/BaseTypes.rds"))
    SS <- get_data_model("Species Selection")


    link <- read.csv(ref_path)
    link <- subset(link, DEsamplingScheme %in% sampling_scheme)

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
      ss$SSobservationActivityType <- "Sort"
      ss$SScatchFraction<- "Lan"
      ss$SSobservationType <- "Volume"
      ss$SSsampler <-  "" # Not relevant, since we do not
      ss$SSuseForCalculateZero <- "N"

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
