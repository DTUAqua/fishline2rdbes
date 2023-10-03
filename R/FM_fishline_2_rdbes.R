
#' FishLine 2 RDBES, Frequency Measure (FM)
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
FM_fishline_2_rdbes <-
  function(ref_path = "Q:/mynd/RDB/create_RDBES_data/references",
           sampling_scheme = "DNK_Market_Sampling",
           years = 2016,
           data_model_path){
    # Input for testing ----

    # ref_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/references"
    # years <- c(2021)
    # sampling_scheme <- "DNK_Market_Sampling"

    # Set-up ----


    library(sqldf)
    library(tidyr)
    library(plyr, include.only = c("rbind.fill"))
    library(dplyr)
    library(stringr)
    library(haven)

    FM <- get_data_model("Frequency Measure", data_model_path = data_model_path)

    link <- read.csv(ref_path)
    link <- subset(link, DEsamplingScheme == sampling_scheme)

    trips <- unique(link$tripId[!is.na(link$tripId)])

    # Get needed stuff ----

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    samp <- sqlQuery(
      channel,
      paste(
        "SELECT Specieslist.speciesListId, Sample.sampleId, Sample.tripId, Animal.*
                  FROM        fishlineDW.dbo.Animal INNER JOIN
                  fishlineDW.dbo.SpeciesList ON Animal.speciesListId = SpeciesList.speciesListId INNER JOIN
                  fishlineDW.dbo.Sample ON SpeciesList.sampleId = Sample.sampleId
         WHERE (Specieslist.year between ",
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


    sa_2 <- subset(samp, representative == "ja" & is.na(individNum)) # OBS TOBIS ...... måske ikke nødvendigt at tænke på tobis


    # Recode for SA ----

    fm <- sa_2

    fm$FMid <- fm$animalId
    fm$FMrecordType <- "FM"

    fm <- mutate(fm, FMpresentation = ifelse(treatment == "UR", "WHL",
                                             ifelse(treatment == "RH", "GUT",
                                                    ifelse(treatment == "RU", "GUH",
                                                           ifelse(treatment %in% c("VV", "VK"), "WNG",
                                                                  ifelse(treatment == "TAL", "Tail", NA))))))

    test <- filter(fm, is.na(FMpresentation))

    fm$FMspecimensState <- "NotDetermined"

    fm$FMstateOfProcessing <- "UNK"

    fm$FMclassMeasured <- fm$length
    fm$FMnumberAtUnit <- fm$number

    fm$FMtypeMeasured[!(fm$speciesCode %in% c("DVH", "DVR", "HRJ"))] <- "LengthTotal"
    fm$FMtypeMeasured[fm$speciesCode %in% c("DVH", "DVR", "HRJ")] <- "LengthCarapace"

    fm$FMtypeAssessment[!(fm$speciesCode %in% c("DVH", "DVR", "HRJ"))] <- "LengthTotal"
    fm$FMtypeAssessment[fm$speciesCode %in% c("DVH", "DVR", "HRJ")] <- "LengthCarapace"

    unique(fm$lengthMeasureUnit)

    fm$FMaccuracy[fm$lengthMeasureUnit == "CM"] <- "cm"

    fm$FMconversionFactorAssessment <- 1

    fm$FMsampler <- "Observer"

    fm$FMaddGrpMeasurement <- fm$weight

    fm$FMaddGrpMeasurementType   <- "WeightMeasured"

    fm <- plyr::rbind.fill(FM, fm)
    fm <- fm[ , c(names(FM), "sampleId", "speciesListId", "FMid", "year")]

    return(list(fm, FM))
  }
