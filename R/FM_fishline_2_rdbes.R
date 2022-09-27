
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
           type = "everything") {
    # Input for testing ----

    # ref_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/references"
    # years <- c(2021)
    # sampling_scheme <- "DNK_Market_Sampling"
    # type <- "everything"

    # Set-up ----


    library(sqldf)
    library(tidyr)
    library(dplyr)
    library(stringr)
    library(haven)

    data_model <- readRDS(paste0(ref_path, "/BaseTypes.rds"))
    link <-
      read.csv(paste0(ref_path, "/link_fishLine_sampling_designs.csv"))

    link <- subset(link, DEsamplingScheme == sampling_scheme)

    fm_temp <- filter(data_model, substr(name, 1, 2) == "FM")
    fm_temp_t <- c("FMrecordType", t(fm_temp$name)[1:nrow(fm_temp)])

    trips <- unique(link$tripId[!is.na(link$tripId)])

    # Get needed stuff ----

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    samp <- sqlQuery(
      channel,
      paste(
        "SELECT Specieslist.speciesListId, Sample.sampleId, Sample.tripId, Animal.*
                  FROM        Animal INNER JOIN
                  SpeciesList ON Animal.speciesListId = SpeciesList.speciesListId INNER JOIN
                  Sample ON SpeciesList.sampleId = Sample.sampleId
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


    sa_2 <- subset(samp, representative == "ja" & is.na(individNum))

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

    fm$FMmethod <- ""
    fm$FMmeasurementEquipment <- ""

    unique(fm$lengthMeasureUnit)

    fm$FMaccuracy[fm$lengthMeasureUnit == "CM"] <- "cm"

    fm$FMconversionFactorAssessment <- 1

    fm$FMsampler <- "Observer"

    fm$FMaddGrpMeasurement <- fm$weight

    fm$FMaddGrpMeasurementType   <- "WeightMeasured"



    if (type == "only_mandatory") {
      fm_temp_optional <-
        filter(data_model, substr(name, 1, 2) == "FM" & min == 0)
      fm_temp_optional_t <-
        factor(t(fm_temp_optional$name)[1:nrow(fm_temp_optional)])

      for (i in levels(fm_temp_optional_t)) {
        eval(parse(text = paste0("fm$", i, " <- ''")))
      }
    }

    FM <-
      select(fm, one_of(fm_temp_t), sampleId, speciesListId, FMid, year)

    return(list(FM, fm_temp, fm_temp_t))
  }
