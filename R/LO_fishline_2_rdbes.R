
#' FishLine 2 RDBES, Location selection (LO)
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

VS_fishline_2_rdbes <-
  function(data_model_baseTypes_path = "Q:/mynd/RDB/create_RDBES_data/references",
           years = 2016,
           cruises = c("MON", "SEAS", "IN-HIRT"),
           type = "everything"
           )
  {


    # Input for testing ----

    data_model_baseTypes_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/references"
    years <- c(2018:2020)
    cruises <- c("IN-LYNG")
    type <- "everything"

    # Set-up ----

    library(RODBC)
    library(sqldf)
    library(dplyr)
    library(stringr)
    library(haven)

    data_model <- readRDS(paste0(data_model_baseTypes_path, "/BaseTypes.rds"))

    lo_temp <- filter(data_model, substr(name, 1, 2) == "LO")
    lo_temp_t <- c("LOrecordType", t(lo_temp$name)[1:nrow(lo_temp)])

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

    locode$samplingLocation <- locode$harbourEU

    # Add needed stuff ----
    #LOCODE

    tr_1 <- left_join(tr, locode, by = c("harbourSample" = "harbour"))

    # Recode for LO ----

    lo <- tr_1

    lo$LOid <- lo$tripId
    lo$LOrecordType <- "LO"

    lo$LOlocode <- lo$samplingLocation
    lo$LOlocationName <- ""
    lo$LOlocationType <- ""

    lo$LOsequenceNumber <- NA   #To be coded manual - depends on design
    lo$LOstratification <- "N"  #To be coded manual - depends on design
    lo$LOstratumName <- "U"     #To be coded manual - depends on design
    lo$LOclustering <- "N"      #To be coded manual - depends on design
    lo$LOclusterName <- "No"     #To be coded manual - depends on design

    lo$LOsampler <-  "Observer" # That is not completely TRUE

    lo$LOnumberTotal <- ""      #To be coded manual - depends on design
    lo$LOnumberSampled <- ""    #To be coded manual - depends on design
    lo$LOselectionProb <- ""    #To be coded manual - depends on design
    lo$LOinclusionProb <- ""    #To be coded manual - depends on design
    lo$LOselectionMethod <- "NotApplicable"  #To be coded manual - depends on design

    lo$LOunitName <- lo$samplingLocation

    lo$LOselectionMethodCluster <- ""  #To be coded manual - depends on design
    lo$LOnumberTotalClusters <- ""     #To be coded manual - depends on design
    lo$LOnumberSampledClusters <- ""   #To be coded manual - depends on design
    lo$LOselectionProbCluster <- ""    #To be coded manual - depends on design
    lo$LOinclusionProbCluster <- ""       #To be coded manual - depends on design

    lo$LOsampled <- "Y"
    lo$LOreasonNotSampled <- ""           #Reasoning requires manual coding

    if (type == "only_mandatory") {
      lo_temp_optional <-
        filter(data_model, substr(name, 1, 2) == "LO" & min == 0)
      lo_temp_optional_t <-
        factor(t(lo_temp_optional$name)[1:nrow(lo_temp_optional)])

      for (i in levels(lo_temp_optional_t)) {
        eval(parse(text = paste0("lo$", i, " <- ''")))

      }
    }

    LO <- select(lo, one_of(lo_temp_t), tripId, LOid, dateEnd, year)

    return(list(LO, lo_temp, lo_temp_t))

  }
