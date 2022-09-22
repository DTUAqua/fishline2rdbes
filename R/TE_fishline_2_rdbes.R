
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
  function(ref_path = "Q:/mynd/RDB/create_RDBES_data/references",
           sampling_scheme = "DNK_Market_Sampling",
           years = 2016,
           type = "everything"
           )
  {


    # Input for testing ----

    # ref_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/references"
    # sampling_scheme <- "DNK_Market_Sampling"
    # years <- c(2021)
    # type <- "everything"

    # Set-up ----

    library(RODBC)
    library(sqldf)
    library(dplyr)
    library(stringr)
    library(haven)

    data_model <- readRDS(paste0(ref_path, "/BaseTypes.rds"))
    link <- read.csv(paste0(ref_path, "/link_fishLine_sampling_designs.csv"))

    link <- subset(link, DEsamplingScheme == sampling_scheme)

    te_temp <- filter(data_model, substr(name, 1, 2) == "TE")
    te_temp_t <- c("TErecordType", t(te_temp$name)[1:nrow(te_temp)])

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

    test <- distinct(tr, dateSample, dateStart, dateEnd)

    # Design variables

    tr_1 <- left_join(link[,grep("^[TE]|^[trip]", names(link), value = T)], tr)

    tr_2 <- subset(tr_1, !is.na(dateSample))

    # Recode for LO ----

    te <- tr_2

    te$TEid <- "" # To be coded after join with DE and SD
    te$TErecordType <- "TE"

    te$TEtimeUnit <- "Day"  # Not totally true, since some

    te$TEsequenceNumber <- NA   # To be coded after join with DE and SD
    te$TEclustering <- "N"      # Not used in this scheme
    te$TEclusterName <- "No"    # Not used in this scheme

    te$TEsampler <-  te$samplingMethod # DTU Aqua selects the location

    te$TEselectionProb <- ""    # Not included for this scheme
    te$TEinclusionProb <- ""    # Not included for this scheme

    te$TEunitName <- te$dateSample

    te$TEselectionMethodCluster <- ""  # Not used in this scheme
    te$TEnumberTotalClusters <- ""     # Not used in this scheme
    te$TEnumberSampledClusters <- ""   # Not used in this scheme
    te$TEselectionProbCluster <- ""    # Not used in this scheme
    te$TEinclusionProbCluster <- ""    # Not used in this scheme

    te$TEsampled <- "Y"
    te$TEreasonNotSampled <- ""           # No non-responses, but we have NULL samples in our DB

    if (type == "only_mandatory") {
      te_temp_optional <-
        filter(data_model, substr(name, 1, 2) == "TE" & min == 0)
      te_temp_optional_t <-
        factor(t(te_temp_optional$name)[1:nrow(te_temp_optional)])

      for (i in levels(te_temp_optional_t)) {
        eval(parse(text = paste0("te$", i, " <- ''")))

      }
    }

    TE <- select(te, one_of(te_temp_t), tripId, TEid)

    return(list(TE, te_temp, te_temp_t))

  }
