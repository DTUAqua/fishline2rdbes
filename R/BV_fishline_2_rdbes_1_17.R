
#' FishLine 2 RDBES, biological variables (BV)
#'
#' @description Converts samples data from national database (fishLine) to RDBES. Data model v. 1.17. This function ...
#'
#' @param df
#' @param data_model Where to find the .xlsx with the data model
#' @param years
#' @param cruises Name of cruises in national database
#' @param type
#'
#' @return
#' @export
#'
#'
#' @examples
#'
#'
#'

BV_fishline_2_rdbes <-
  function(data_model = data_model,
           years = c(2016),
           cruises = c("MON", "SEAS"),
           type = "individual")
  {

  library(openxlsx)
  library(RODBC)
  library(sqldf)
  library(dplyr)
  library(stringr)

  bv_temp <- read.xlsx(paste(wd, data_model, sep = ""), sheet = 16)
  var_keep <- na.omit(bv_temp$Order)
  bv_temp <- t(bv_temp$R.Name)[1:max(var_keep)]

  # Get data from FishLine
  channel <- odbcConnect("FishLineDW")
  dat <- sqlQuery(channel, paste("SELECT Animal.animalId, Animal.animalInfoId, Animal.speciesListId, Animal.year, Animal.cruise, Animal.trip, Animal.tripType, Animal.station, Animal.dateGearStart, Animal.quarterGearStart, Animal.dfuArea, Animal.statisticalRectangle,
                  Animal.gearQuality, Animal.gearType, Animal.meshSize, Animal.speciesCode, Animal.landingCategory, Animal.dfuBase_Category, Animal.sizeSortingEU, Animal.sizeSortingDFU, Animal.ovigorous, Animal.cuticulaHardness,
                  Animal.treatment, Animal.speciesList_sexCode, Animal.sexCode, Animal.representative, Animal.individNum, Animal.number, Animal.speciesList_number, Animal.length, Animal.lengthMeasureUnit, Animal.weight, Animal.treatmentFactor,
                  Animal.maturityIndex, Animal.maturityIndexMethod, Animal.broodingPhase, Animal.weightGutted, Animal.weightLiver, Animal.weightGonads, Animal.parasiteCode, Animal.fatIndex, Animal.fatIndexMethod, Animal.numVertebra,
                  Animal.maturityReaderId, Animal.maturityReader, Animal.remark, Animal.animalInfo_remark, Animal.catchNum, Animal.otolithFinScale, Age.ageId, Age.age, Age.agePlusGroup, Age.otolithWeight, Age.edgeStructure,
                  Age.otolithReadingRemark, Age.hatchMonth, Age.hatchMonthRemark, Age.ageReadId, Age.ageReadName, Age.hatchMonthReaderId, Age.hatchMonthReaderName, Age.genetics
                  FROM     Animal LEFT JOIN
                  Age ON Animal.animalId = Age.animalId WHERE (Animal.year between ", min(years) , " and ", max(years), ")
                                 and Animal.cruise in ('", paste(cruises, collapse = "','"), "')", sep = ""))
  close(channel)

  dat_1 <- filter(dat, individNum > 0)

  #Start coding the BV variables

  dat_2 <- mutate(dat_1, BVrecType = "BV", BVfishID = animalId, )

  }

#' FishLine 2 RDBES, biological variables (BV)
#'
#' @description Converts samples data from national database (fishLine) to RDBES. Data model v. 1.17. This function ...
#'
#' @param df
#' @param data_model Where to find the .xlsx with the data model
#' @param years
#' @param cruises Name of cruises in national database
#' @param type
#'
#' @return
#' @export
#'
#'
#' @examples
#'
#'
#'

BV_fishline_2_rdbes <-
  function(data_model = data_model,
           years = c(2016),
           cruises = c("MON", "SEAS"),
           type = "individual")
  {

  library(openxlsx)
  library(RODBC)
  library(sqldf)
  library(dplyr)
  library(stringr)

  bv_temp <- read.xlsx(paste(wd, data_model, sep = ""), sheet = 16)
  var_keep <- na.omit(bv_temp$Order)
  bv_temp <- t(bv_temp$R.Name)[1:max(var_keep)]

  # Get data from FishLine
  channel <- odbcConnect("FishLineDW")
  dat <- sqlQuery(channel, paste("SELECT Animal.animalId, Animal.animalInfoId, Animal.speciesListId, Animal.year, Animal.cruise, Animal.trip, Animal.tripType, Animal.station, Animal.dateGearStart, Animal.quarterGearStart, Animal.dfuArea, Animal.statisticalRectangle,
                  Animal.gearQuality, Animal.gearType, Animal.meshSize, Animal.speciesCode, Animal.landingCategory, Animal.dfuBase_Category, Animal.sizeSortingEU, Animal.sizeSortingDFU, Animal.ovigorous, Animal.cuticulaHardness,
                  Animal.treatment, Animal.speciesList_sexCode, Animal.sexCode, Animal.representative, Animal.individNum, Animal.number, Animal.speciesList_number, Animal.length, Animal.lengthMeasureUnit, Animal.weight, Animal.treatmentFactor,
                  Animal.maturityIndex, Animal.maturityIndexMethod, Animal.broodingPhase, Animal.weightGutted, Animal.weightLiver, Animal.weightGonads, Animal.parasiteCode, Animal.fatIndex, Animal.fatIndexMethod, Animal.numVertebra,
                  Animal.maturityReaderId, Animal.maturityReader, Animal.remark, Animal.animalInfo_remark, Animal.catchNum, Animal.otolithFinScale, Age.ageId, Age.age, Age.agePlusGroup, Age.otolithWeight, Age.edgeStructure,
                  Age.otolithReadingRemark, Age.hatchMonth, Age.hatchMonthRemark, Age.ageReadId, Age.ageReadName, Age.hatchMonthReaderId, Age.hatchMonthReaderName, Age.genetics
                  FROM     Animal LEFT JOIN
                  Age ON Animal.animalId = Age.animalId WHERE (Animal.year between ", min(years) , " and ", max(years), ")
                                 and Animal.cruise in ('", paste(cruises, collapse = "','"), "')", sep = ""))
  close(channel)

  dat_1 <- filter(dat, individNum > 0)

  #Start coding the BV variables

  dat_2 <- mutate(dat_1, BVrecType = "BV", BVfishID = animalId, )

  }
