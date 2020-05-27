
#' FishLine 2 RDBES, biological variables (BV)
#'
#' @description Converts samples data from national database (fishLine) to RDBES. Data model v. 1.17. This function ...
#'
#' @param data_model Where to find the .xlsx with the data model
#' @param year Only takes a single year for now
#' @param cruises Name of cruises in national database
#' @param type only_mandatory | everything
#'
#' @return
#' @export
#'
#'
#' @examples
#'
#'
#'

VD_fishline_2_rdbes_1_18 <-
  function(data_model = fields_sequence,
           year = 2016,
           cruises = c("MON", "SEAS"),
           type = "only_mandatory")
  {

  library(RODBC)
  library(sqldf)
  library(dplyr)
  library(stringr)
  library(haven)

  vd_temp <- filter(data_model, substr(name, 1, 2) == "VD")
  vd_temp_t <- c("VDrecordType",t(vd_temp$name)[1:nrow(vd_temp)])

  # Get data from FishLine
  channel <- odbcConnect("FishLineDW")
  tr <- sqlQuery(channel, paste("select * FROM dbo.Trip
                                 WHERE (Trip.year = ", year , ")
                                 and Trip.cruise in ('", paste(cruises, collapse = "','"), "')", sep = ""))
  close(channel)

  # Getting info form the Danish vessel registre
  ftj <- read_sas("Q:/dfad/data/Data/Ftjreg/ftjid.sas7bdat")

  # Getting LOCODE's for homeport
  locode <- read_sas("Q:/mynd/SAS Library/Lplads/lplads.sas7bdat")

  # Add info to the trips from fishLine

  tr$DepDate <- as.Date(tr$dateStart)
  platform <- distinct(tr, platform1, DepDate, tripId)
  platform$platform1 <- as.character(platform$platform1)
  ftj1 <- select(ftj, fid, Vessel_identifier_Fid, vstart, vslut, oal, bhavn, kw, bt, brt)

  vessel <- sqldf("select * from platform a left join ftj1 b on a.platform1=b.fid")
  vessel1 <- subset(vessel, DepDate >= vstart & DepDate <= vslut)
  vessel2 <- full_join(platform, vessel1)
  vessel3 <- left_join(vessel2, locode[, c("start", "harbourEU")], by = c("bhavn" = "start"))

  vd <- distinct(vessel3, tripId, platform1, Vessel_identifier_Fid, oal, harbourEU, kw, bt, brt)

  #Start coding the BV variables

  VDid <- distinct(vessel3, platform1, Vessel_identifier_Fid, oal, harbourEU, kw, bt, brt)
  VDid$VDid <- c(1:nrow(VDid))

  vd <- left_join(vd, VDid)
  vd$VDrecordType <- "VD"
  vd$VDencryptedVesselCode <- vd$Vessel_identifier_Fid
  vd$VDhomePort <- vd$harbourEU
  vd$VDflagCountry <- "DK"
  vd$VDlength <- round(vd$oal, digits = 0)
  vd$VDlengthCategory <- ifelse(vd$VDlen < 8, "<8",
                        ifelse(8 <= vd$VDlen & vd$VDlen < 10, "8-<10",
                               ifelse(10 <= vd$VDlen & vd$VDlen < 12, "10-<12",
                                      ifelse(12 <= vd$VDlen & vd$VDlen < 15, "12-<15",
                                             ifelse(15 <= vd$VDlen & vd$VDlen < 18, "15-<18",
                                                    ifelse(18 <= vd$VDlen & vd$VDlen < 24, "18-<24",
                                                           ifelse(24 <= vd$VDlen & vd$VDlen < 40, "18-<24",
                                                                  ifelse(40 <= vd$VDlen, "40<", NA))))))))

  test_VDlenCat <- distinct(vd, VDlen, VDlenCat)

  vd$VDpower <- round(vd$kw, digits = 0)
  vd$VDtonAge <- round(vd$bt, digits = 0) #OBS needs to look at both bt and brt
  vd$VDtonUnit <- "GT"

  if (type == "only_mandatory") {

    vd_temp_optional <- filter(data_model, substr(name, 1, 2) == "VD" & min == 0)
    vd_temp_optional_t <- factor(t(vd_temp_optional$name)[1:nrow(vd_temp_optional)])

    for (i in levels(vd_temp_optional_t)) {

      eval(parse(text = paste0("vd$", i, " <- NA")))

    }
  }

  VD <- select(vd, one_of(vd_temp_t), tripId)

  return(list(VD, vd_temp, vd_temp_t))

  }
