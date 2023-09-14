
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
           type = "everything"
           )
  {


    # Input for testing ----

    ref_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/references"
    sampling_scheme <- "DNK_AtSea_Observer_Active"
    years <- c(2021)
    type <- "everything"
    specieslist_name <- "DNK_AtSea_Observer_2021"
    xx <- "observer at-sea"

    # Set-up ----

    library(RODBC)
    library(sqldf)
    library(dplyr)
    library(stringr)
    library(haven)

    data_model <- readRDS(paste0(ref_path, "/BaseTypes.rds"))
    link <- read.csv(paste0(ref_path, "/link_fishLine_sampling_designs.csv"))

    link <- subset(link, DEsamplingScheme == sampling_scheme)

    ss_temp <- filter(data_model, substr(name, 1, 2) == "SS")
    ss_temp_t <- c("SSrecordType", t(ss_temp$name)[1:nrow(ss_temp)],
    "SStimeTotal", "SStimeSampled")

    # Wrong order, so correct
    ss_temp_t <- ss_temp_t[c(1:3, 7:9, 4:6, 10:12, 26:27, 13:25)]

    trips <- unique(link$tripId[!is.na(link$tripId)])

    # Get needed stuff ----

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    st <- sqlQuery(
      channel,
      paste(
        "select * FROM dbo.Sample
         WHERE (Sample.year between ", min(years), " and ", max(years) , ")
                and Sample.tripId in (", paste(trips, collapse = ","),
        ")",
        sep = ""
      )
    )
    close(channel)

    if (xx == "observer at-sea") {

      st <- filter(st, gearQuality == "V" & catchRegistration == "ALL" & speciesRegistration == "ALL")
    }

    # Recode for LO ----

    ss <- st

    ss$SSid <- "" # To be coded after join with DE and SD
    ss$SSrecordType <- "SS"

    ss$SSsequenceNumber <- NA   # To be coded after join with DE and SD

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

      ss$SSnumberTotal <- ""
      ss$SSnumberSampled <- ""
      ss$SSselectionProb <- ""    # Not included for this scheme
      ss$SSinclusionProb <- ""    # Not included for this scheme

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
      ss$SSselectionProb <- ""    # Not included for this scheme
      ss$SSinclusionProb <- ""    # Not included for this scheme

      ss$SSselectionMethod <- "CENSUS" # Not totally T eg. for partial selection


    }

        ss$SSspeciesListName <- specieslist_name

    ss$SStimeTotal <- ""
    ss$SStimeSampled <- ""


    ss$SSunitName <- paste(ss$cruise, ss$trip, ss$station, sep = "-")

    ss$SSselectionMethodCluster <- ""  # Not used in this scheme
    ss$SSnumberTotalClusters <- ""     # Not used in this scheme
    ss$SSnumberSampledClusters <- ""   # Not used in this scheme
    ss$SSselectionProbCluster <- ""    # Not used in this scheme
    ss$SSinclusionProbCluster <- ""    # Not used in this scheme

    ss$SSsampled <- "Y"
    ss$SSreasonNotSampled <- ""           # No non-responses, but we have NULL samples in our DB

    if (type == "only_mandatory") {
      ss_temp_optional <-
        filter(data_model, substr(name, 1, 2) == "SS" & min == 0)
      lo_temp_optional_t <-
        factor(t(ss_temp_optional$name)[1:nrow(ss_temp_optional)])

      for (i in levels(ss_temp_optional_t)) {
        eval(parse(text = paste0("ss$", i, " <- ''")))

      }
    }

    SS <- select(ss, one_of(ss_temp_t), tripId, sampleId, SSid)

    return(list(SS, ss_temp, ss_temp_t))

  }
