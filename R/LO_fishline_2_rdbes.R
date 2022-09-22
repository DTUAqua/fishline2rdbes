
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
#' @return
#' @export
#'
#'
#' @examples
#'
#'
#'

LO_fishline_2_rdbes <-
  function(ref_path = "Q:/mynd/RDB/create_RDBES_data/references",
           years = 2016,
           type = "everything"
           )
  {


    # Input for testing ----
#
#     ref_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/references"
#     years <- c(2021)
#     type <- "everything"

    # Set-up ----

    library(RODBC)
    library(sqldf)
    library(dplyr)
    library(stringr)
    library(haven)

    data_model <- readRDS(paste0(ref_path, "/BaseTypes.rds"))
    link <- read.csv(paste0(ref_path, "/link_fishLine_sampling_designs.csv"))

    lo_temp <- filter(data_model, substr(name, 1, 2) == "LO")
    lo_temp_t <- c("LOrecordType", t(lo_temp$name)[1:nrow(lo_temp)])

    trips <- unique(link$tripId[!is.na(link$tripId)])

    # Get needed stuff ----

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    tr <- sqlQuery(
      channel,
      paste(
        "select * FROM dbo.Trip
         WHERE (Trip.year between ", min(years), " and ", max(years) , ")
                and Trip.tripId in (", paste(trips, collapse = ","),
        ")",
        sep = ""
      )
    )
    close(channel)

    test <- distinct(tr, harbourSample, harbourLanding)

    # Get LOCODE's for sampling location

    channel <- odbcConnect("FishLine")
    locode <- sqlQuery(
      channel,
      paste(
        "SELECT harbour, harbourEU FROM L_Harbour",
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

    lo$LOlocationName <- ""
    lo$LOlocationType <- ""

    lo$LOsequenceNumber <- NA   # To be coded after join with DE and SD
    lo$LOclustering <- "N"      # Not used in this scheme
    lo$LOclusterName <- "No"    # Not used in this scheme

    lo$LOsampler <-  "Observer" # DTU Aqua selects the locationa

    lo$LOselectionProb <- ""    # Not included for this scheme
    lo$LOinclusionProb <- ""    # Not included for this scheme

    lo$LOunitName <- lo$LOlocode

    lo$LOselectionMethodCluster <- ""  # Not used in this scheme
    lo$LOnumberTotalClusters <- ""     # Not used in this scheme
    lo$LOnumberSampledClusters <- ""   # Not used in this scheme
    lo$LOselectionProbCluster <- ""    # Not used in this scheme
    lo$LOinclusionProbCluster <- ""    # Not used in this scheme

    lo$LOsampled <- "Y"
    lo$LOreasonNotSampled <- ""           # No non-responses, but we have NULL samples in our DB

    if (type == "only_mandatory") {
      lo_temp_optional <-
        filter(data_model, substr(name, 1, 2) == "LO" & min == 0)
      lo_temp_optional_t <-
        factor(t(lo_temp_optional$name)[1:nrow(lo_temp_optional)])

      for (i in levels(lo_temp_optional_t)) {
        eval(parse(text = paste0("lo$", i, " <- ''")))

      }
    }

    LO <- select(lo, one_of(lo_temp_t), tripId, LOid)

    return(list(LO, lo_temp, lo_temp_t))

  }
