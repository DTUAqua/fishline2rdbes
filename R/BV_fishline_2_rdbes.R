
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
           years = 2016) {
    # Input for testing ----
    #
    # ref_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data_old/references"
    # years <- c(2021)
    # sampling_scheme <- "DNK_Market_Sampling"

    # Set-up ----


    library(sqldf)
    library(RODBC)
    library(tidyr)
    library(plyr, include.only = c("rbind.fill"))
    library(dplyr)
    library(stringr)
    library(haven)
    library(data.table)

    #data_model <- readRDS(paste0(ref_path, "/BaseTypes.rds"))
    BV <- get_data_model("Biological Variable")

    link <- read.csv(paste0(ref_path, "/link_fishLine_sampling_designs.csv"))
    link <- subset(link, DEsamplingScheme == sampling_scheme)

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
                  Animal.otolithFinScale, Age.age, Age.agePlusGroup, Age.otolithReadingRemark, Age.genetics
FROM        fishlineDW.dbo.Animal INNER JOIN
                  fishlineDW.dbo.SpeciesList ON Animal.speciesListId = SpeciesList.speciesListId INNER JOIN
                  fishlineDW.dbo.Sample ON SpeciesList.sampleId = Sample.sampleId LEFT OUTER JOIN
                  fishlineDW.dbo.Age ON Animal.animalId = Age.animalId
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


    samp <- mutate(samp, BVpresentation = ifelse(treatment == "UR", "WHL",
                                             ifelse(treatment == "RH", "GUT",
                                                    ifelse(treatment == "RU", "GUH",
                                                           ifelse(treatment %in% c("VV", "VK"), "WNG",
                                                                  ifelse(treatment == "TAL", "Tail", NA)
                                                           )
                                                    )
                                             )
    ))

    ###
    bv <- samp[! is.na(samp$individNum) |
                       (samp$speciesCode == "BRS" & !is.na(samp$age)), ]

    #genetics missing is 0 in the database
    bv$genetics[bv$genetics == 0] <- NA
    bv$otolithReadingRemark[is.na(bv$otolithReadingRemark)] <- "Unknown"

    var <- data.frame(variable = c("sexCode", "length", "weight",
                                   "maturityIndex", "age", "genetics"),
                      value = c("", "lengthMeasureUnit", "treatmentFactor",
                                "maturityIndexMethod", "otolithReadingRemark", ""),
                      BVvalueUnitOrScale = c("Sex", "Lengthmm", "Weightg", "SMSF", "Ageyear", ""),
                      BVmethod = c("", "", "", "gonad", "Otoltih", ""),
                      BVtypeMeasured = c("Sex", "LengthTotal", "WeightMeasured",
                                         "Maturity", "Age", "InfoGenetic"),
                      BVtypeAssessment = c("Sex", "LengthTotal", "WeightLive",
                                           "Maturity", "Age", "InfoGenetic"))

    setDT(bv)
    L1 <- melt.data.table(bv, id.vars = c("animalId", "sampleId", "speciesListId", "year", "cruise", "trip",
                                              "station", "speciesCode", "landingCategory",
                                              "sizeSortingEU", "individNum"),
                          measure.vars = c("sexCode", "length", "weight",
                                           "maturityIndex", "age", "genetics"),
                          value.name = "BVvalueMeasured")
    L1 <- L1[! is.na(L1$BVvalueMeasured), ]
    L1 <- merge(L1, var, by = "variable", all.x = T)


    L2 <- melt.data.table(bv, id.vars = c("animalId"),
                          measure.vars = c("lengthMeasureUnit", "otolithReadingRemark",
                                           "treatmentFactor"),
                          variable.name = "value", value.name = "aux")


    bv <- merge(L1, L2, by = c("animalId", "value"), all.x = T)
    bv <- merge(bv, unique(samp[, c("animalId", "BVpresentation")]),
                               by = "animalId", all.x = T)
    # Recode for SA ----

    bv$BVid <- ""
    bv$BVrecordType <- "BV"

    bv$BVnationalUniqueFishId <- bv$animalId

    bv$BVspecimensState <- "NotDetermined"

    bv$BVstateOfProcessing <- "UNK"

    bv[bv$speciesCode %in% c("DVH", "DVR", "HRJ") &
         bv$variable == "length", "BVtypeMeasured"] <- "LengthCarapace"

    bv[bv$speciesCode %in% c("DVH", "DVR", "HRJ") &
         bv$BVtypeAssessment == "lengthTotal", "BVtypeAssessment"] <- "LengthCarapace"

    bv$BVaccuracy <- tolower(ifelse(bv$value == "lengthMeasureUnit", bv$aux, ""))

    # bv[bv$speciesCode %in% c("SIL") & bv$BVvalueUnitOrScale == "ageyear", "BVvalueUnitOrScale"] <- "Agewr"
    bv$BVcertaintyQualitative <- ifelse(bv$value == "otolithReadingRemark", bv$aux, "Unknown")
    bv[bv$BVtypeMeasured %in% c("LengthTotal", "WeightMeasured"), "BVcertaintyQualitative"] <- "NotApplicable"
    bv[bv$BVtypeMeasured %in% c("Sex"), "BVcertaintyQualitative"] <- "QS1"

    bv$BVconversionFactorAssessment <- ifelse(bv$value == "treatmentFactor", bv$aux, "1")

    bv <-
        mutate(
          bv,
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

    bv$BVsampler <- "Observer"


    bv <- plyr::rbind.fill(BV, bv)
    bv <- bv[ , c(names(BV), "sampleId", "speciesListId", "BVid", "year")]

    return(list(bv, BV))
  }
