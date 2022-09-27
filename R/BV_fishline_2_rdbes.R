
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
BV_fishline_2_rdbes <-
  function(ref_path = "Q:/mynd/RDB/create_RDBES_data/references",
           sampling_scheme = "DNK_Market_Sampling",
           years = 2016,
           bv_measurement = "length",
           type = "everything") {
    # Input for testing ----

    # ref_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/references"
    # years <- c(2021)
    # sampling_scheme <- "DNK_Market_Sampling"
    # type <- "everything"
    # bv_measurement <- "age"

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

    bv_temp <- filter(data_model, substr(name, 1, 2) == "BV")
    bv_temp_t <- c("BVrecordType", t(bv_temp$name)[1:nrow(bv_temp)])

    trips <- unique(link$tripId[!is.na(link$tripId)])

    # Get needed stuff ----

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    samp <- sqlQuery(
      channel,
      paste(
        "SELECT     SpeciesList.speciesListId, Sample.sampleId, Sample.tripId, Animal.animalId, Animal.animalInfoId, Animal.speciesListId AS Expr1, Animal.year, Animal.cruise, Animal.trip, Animal.tripType, Animal.station, Animal.dateGearStart, Animal.quarterGearStart, Animal.dfuArea,
                  Animal.statisticalRectangle, Animal.gearQuality, Animal.gearType, Animal.meshSize, Animal.speciesCode, Animal.landingCategory, Animal.dfuBase_Category, Animal.sizeSortingEU, Animal.sizeSortingDFU, Animal.ovigorous, Animal.cuticulaHardness, Animal.treatment,
                  Animal.speciesList_sexCode, Animal.sexCode, Animal.representative, Animal.individNum, Animal.number, Animal.speciesList_number, Animal.length, Animal.lengthMeasureUnit, Animal.weight, Animal.treatmentFactor, Animal.maturityIndex, Animal.maturityIndexMethod,
                  Animal.broodingPhase, Animal.weightGutted, Animal.weightLiver, Animal.weightGonads, Animal.parasiteCode, Animal.fatIndex, Animal.fatIndexMethod, Animal.numVertebra, Animal.maturityReaderId, Animal.maturityReader, Animal.remark, Animal.animalInfo_remark, Animal.catchNum,
                  Animal.otolithFinScale, Age.age, Age.agePlusGroup, Age.otolithReadingRemark
FROM        Animal INNER JOIN
                  SpeciesList ON Animal.speciesListId = SpeciesList.speciesListId INNER JOIN
                  Sample ON SpeciesList.sampleId = Sample.sampleId LEFT OUTER JOIN
                  Age ON Animal.animalId = Age.animalId
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


    bv <- subset(samp, !(is.na(individNum)))

    # Recode for SA ----

    bv$BVid <- ""
    bv$BVrecordType <- "BV"

    bv$BVnationalUniqueFishId <- bv$animalId

    bv <- mutate(bv, BVpresentation = ifelse(treatment == "UR", "WHL",
      ifelse(treatment == "RH", "GUT",
        ifelse(treatment == "RU", "GUH",
          ifelse(treatment %in% c("VV", "VK"), "WNG",
            ifelse(treatment == "TAL", "Tail", NA)
          )
        )
      )
    ))

    test <- filter(bv, is.na(BVpresentation))

    bv$BVspecimensState <- "NotDetermined"

    bv$BVstateOfProcessing <- "UNK"

    if (bv_measurement == "length") {
      len <- subset(bv, (!is.na(length)))

      len$FMclassMeasured <- round(len$length, digits = 0)

      len$BVstratification <- ""
      len$BVstratumName <- ""

      len$BVtypeMeasured[!(len$speciesCode %in% c("DVH", "DVR", "HRJ"))] <- "LengthTotal"
      len$BVtypeMeasured[len$speciesCode %in% c("DVH", "DVR", "HRJ")] <- "LengthCarapace"

      len$BVtypeAssessment[!(len$speciesCode %in% c("DVH", "DVR", "HRJ"))] <- "LengthTotal"
      len$BVtypeAssessment[len$speciesCode %in% c("DVH", "DVR", "HRJ")] <- "LengthCarapace"

      len$BVvalueMeasured <- len$length

      len$BVvalueUnitOrScale <- "Lengthmm"

      len$BVmethod <- ""
      len$BVmeasurementEquipment <- ""

      len$BVaccuracy[len$lengthMeasureUnit == "CM"] <- "cm"

      len$BVcertaintyQualitative <- "Unknown"
      len$BVcertaintyQuantitative <- ""

      len$BVconversionFactorAssessment <- 1

      len$BVnumberTotal <- ""
      len$BVnumberSampled <- ""
      len$BVselectionProb <- ""
      len$BVinclusionProb <- ""
      len$BVselectionMethod <- ""
      len <-
        mutate(
          len,
          BVunitName = paste(
            animalId,
            cruise,
            trip,
            station,
            speciesCode,
            landingCategory,
            sizeSortingEU,
            individNum,
            sep = "-"
          )
        )
      len <-
        mutate(len, BVunitName = str_replace_all(BVunitName, "-NA", ""))

      len$BVsampler <- "Observer"

      bv <- len
    }

    if (bv_measurement == "weight") {
      w <- subset(bv, (!is.na(weight)))

      w$FMclassMeasured <- round(w$length, digits = 0)

      w$BVstratification <- ""
      w$BVstratumName <- ""

      w$BVtypeMeasured <- "WeightMeasured"

      w$BVtypeAssessment <- "WeightLive"

      w$BVvalueMeasured <- w$weight

      w$BVvalueUnitOrScale <- "Weightg"

      w$BVmethod <- ""
      w$BVmeasurementEquipment <- ""

      w$BVaccuracy <- ""

      w$BVcertaintyQualitative <- "Unknown"
      w$BVcertaintyQuantitative <- ""

      w$BVconversionFactorAssessment <- w$treatmentFactor

      w$BVnumberTotal <- ""
      w$BVnumberSampled <- ""
      w$BVselectionProb <- ""
      w$BVinclusionProb <- ""
      w$BVselectionMethod <- ""
      w <-
        mutate(
          w,
          BVunitName = paste(
            animalId,
            cruise,
            trip,
            station,
            speciesCode,
            landingCategory,
            sizeSortingEU,
            individNum,
            sep = "-"
          )
        )
      w <-
        mutate(w, BVunitName = str_replace_all(BVunitName, "-NA", ""))

      w$BVsampler <- "Observer"

      bv <- w
    }

    if (bv_measurement == "age") {
      age <- subset(bv, (!is.na(age)))

      age$FMclassMeasured <- round(age$length, digits = 0)

      age$BVstratification <- ""
      age$BVstratumName <- ""

      age$BVtypeMeasured <- "Age"

      age$BVtypeAssessment <- "Age"

      age$BVvalueMeasured <- age$age

      age$BVvalueUnitOrScale[!(age$speciesCode %in% c("SIL"))] <- "Ageyear"
      age$BVvalueUnitOrScale[(age$speciesCode %in% c("SIL"))] <- "Agewr"

      age$BVmethod <- "otolith"
      age$BVmeasurementEquipment <- ""

      age$BVaccuracy <- ""

      unique(age$otolithReadingRemark)

      age$BVcertaintyQualitative[is.na(age$otolithReadingRemark)] <- "Unknown"
      age$BVcertaintyQualitative[!(is.na(age$otolithReadingRemark))] <- age$otolithReadingRemark[!(is.na(age$otolithReadingRemark))]

      age$BVcertaintyQuantitative <- ""

      age$BVconversionFactorAssessment <- 1

      age$BVnumberTotal <- ""
      age$BVnumberSampled <- ""
      age$BVselectionProb <- ""
      age$BVinclusionProb <- ""
      age$BVselectionMethod <- ""
      age <-
        mutate(
          age,
          BVunitName = paste(
            animalId,
            cruise,
            trip,
            station,
            speciesCode,
            landingCategory,
            sizeSortingEU,
            individNum,
            sep = "-"
          )
        )
      age <-
        mutate(age, BVunitName = str_replace_all(BVunitName, "-NA", ""))

      age$BVsampler <- "Observer"

      bv <- age
    }

    if (bv_measurement == "sex") {
      sex <- subset(bv, (!is.na(sexCode)))

      sex$FMclassMeasured <- round(sex$length, digits = 0)

      sex$BVstratification <- ""
      sex$BVstratumName <- ""

      sex$BVtypeMeasured <- "Sex"

      sex$BVtypeAssessment <- "Sex"

      sex$BVvalueMeasured[!(is.na(sex$sexCode))] <- sex$sexCode[!(is.na(sex$sexCode))]
      sex$BVvalueMeasured[(is.na(sex$sexCode))] <- "U"

      sex$BVvalueUnitOrScale <- "Sex"

      sex$BVmethod <- ""
      sex$BVmeasurementEquipment <- ""

      sex$BVaccuracy <- ""

      sex$BVcertaintyQualitative <- "Unknown"
      sex$BVcertaintyQuantitative <- ""

      sex$BVconversionFactorAssessment <- 1

      sex$BVnumberTotal <- ""
      sex$BVnumberSampled <- ""
      sex$BVselectionProb <- ""
      sex$BVinclusionProb <- ""
      sex$BVselectionMethod <- ""
      sex <-
        mutate(
          sex,
          BVunitName = paste(
            animalId,
            cruise,
            trip,
            station,
            speciesCode,
            landingCategory,
            sizeSortingEU,
            individNum,
            sep = "-"
          )
        )
      sex <-
        mutate(sex, BVunitName = str_replace_all(BVunitName, "-NA", ""))

      sex$BVsampler <- "Observer"

      bv <- sex
    }

    bv$BVvalueMeasured <- as.character(bv$BVvalueMeasured)


    if (type == "only_mandatory") {
      bv_temp_optional <-
        filter(data_model, substr(name, 1, 2) == "BV" & min == 0)
      bv_temp_optional_t <-
        factor(t(bv_temp_optional$name)[1:nrow(bv_temp_optional)])

      for (i in levels(bv_temp_optional_t)) {
        eval(parse(text = paste0("bv$", i, " <- ''")))
      }
    }

    BV <-
      select(bv, one_of(bv_temp_t), sampleId, speciesListId, BVid, year, FMclassMeasured)

    return(list(BV, bv_temp, bv_temp_t))
  }
