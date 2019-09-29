
#' FishLine 2 RDBES, fishing operation (FO)
#'
#' @description Converts samples data from national database (fishLine) to RDBES. Data model v. 1.17. This function ...
#'
#' @param data_model Where to find the .xlsx with the data model
#' @param year Only takes a single year for now
#' @param cruises Name of cruises in national database
#'
#' @return
#' @export
#'
#'
#' @examples
#'
#'
#'

FO_fishline_2_rdbes <-
  function(data_model = data_model,
           year = 2016,
           cruises = c("MON", "SEAS"))
  {

  library(openxlsx)
  library(RODBC)
  library(sqldf)
  library(dplyr)
  library(stringr)
  library(haven)

    fo_temp <- read.xlsx(data_model, sheet = 9)
    var_keep <- na.omit(fo_temp$Order)
    fo_temp <- t(fo_temp$R.Name)[1:max(var_keep)]

  # Get data from FishLine
  channel <- odbcConnect("FishLineDW")

  sa <- sqlQuery(channel, paste("select * FROM dbo.Sample
                                 WHERE catchRegistration ='ALL' and speciesRegistration = 'ALL' and gearQuality = 'V' and
                                 (Sample.year = ", year , ")
                                 and Sample.cruise in ('", paste(cruises, collapse = "','"), "')", sep = ""))
  close(channel)

  channel2 <- odbcConnect("FishLine")
  dfuArea <- sqlQuery(channel2, paste("select * FROM dbo.L_dfuArea"))
  close(channel2)

  source("Q:/mynd/kibi/EverythingInOnePlace/r_functions/function_read_in_rRDB.R")
  hh <- subset(read_in_rRDB(year,year,"Q:/dfad/users/kibi/home/data_formats/RDB/Output/","HH"), proj %in% c("DNK-MON","DNK-SEAS"))

  #Start coding the FT variables

  sa$FOid <- sa$sampleId
  sa$FTid <- sa$tripId
  sa$SDid <- ""
  sa$FOrecType <- "FO"
  sa$FOhaulNum <- sa$station

  sa$FOstratification <- "N"
  sa$FOstratum <- ""
  sa$FTclustering <- "No"
  sa$FTclusterName <- "U"

  sa$FOaggLev <- "H"
  sa$FOval <- sa$gearQuality
  sa$FOcatReg <- paste(toupper(substring(sa$catchRegistration, 1, 1)), tolower(substring(sa$catchRegistration, 2,3)), sep = "")
  sa$FOcatReg <- ifelse(sa$FOcatReg == "Non", "None", sa$FOcatReg)

  unique(sa$FOcatReg)

  test <- filter(sa, FOcatReg == "NANA")

  sa$FOstartDate <- as.Date(sa$dateGearStart)
  sa$FOstartTime <- strftime(sa$dateGearStart, format = "%H:%M:%S")
  sa$FOendDate <- as.Date(sa$dateGearEnd)
  sa$FOendTime <- strftime(sa$dateGearEnd, format = "%H:%M:%S")
  sa$FOdur <- sa$fishingtime

  sa$FOstartLat <- sa$latPosStartDec
  sa$FOstartLon <- sa$lonPosStartDec
  sa$FOstopLat <- sa$latPosEndDec
  sa$FOstopLon <- sa$lonPosEndDec
  sa$FOecoZone <- ""
  sa <-
    left_join(sa, dfuArea[, c("DFUArea", "areaICES")], by = c("dfuArea" = "DFUArea"))
  sa$FOarea <- sa$areaICES
  sa$FOstatRect <- sa$statisticalRectangle
  sa$FOsubRect <- ""
  sa$FOfu <- ""

  sa$FOdep <- sa$depthAveGear
  sa$FOwaterDep <- sa$depthAvg

  hh <- mutate(hh, proj1 = substr(proj, 5, 10))
  hh <-
    mutate(hh, station = ifelse(
      staNum < 10,
      paste(trpCode, 0, staNum, sep = ""),
      paste(trpCode, staNum, sep = "")
    ))

  sa$FOhaulNum <- as.character(sa$FOhaulNum)
  sa_1 <-
    left_join(sa, hh[, c("proj1", "station", "foCatNat", "foCatEu5", "foCatEu6")], by =
                c("FOhaulNum" = "station"))

  sa_1 <-
    mutate(sa_1, foCatNat = ifelse(
      trip == "1311",
      "OTB_MCD_>=120_0_0",
      ifelse(trip == "2380", "OTB_DEF_>=105_1_120", foCatNat)
    ))
  sa_1 <-
    mutate(sa_1, foCatEu6 = ifelse(
      trip == "1311",
      "OTB_MCD_>=120_0_0",
      ifelse(trip == "2380", "OTB_DEF_>=105_1_120", foCatEu6)
    ))

  sa_1$FOnatCat <- sa_1$foCatNat
  sa_1$FOmetier5 <- sa_1$foCatEu5
  sa_1$FOmetier6 <- sa_1$foCatEu6

  sa_1$FOgear <- sa_1$gearType
  sa_$FOmeshSize <- sa_1$meshSize
  sa_1$FOselDev <- ""
  sa_1$FOselDevMeshSize <- ""
  sa_1$FOtarget <- ""
  sa_1$FOobsCo <- "So"



  sa_1$FOtotal <- ""
  sa_1$FOsampled <- ""
  sa_1$FOprob <- ""              "FOselectMeth"        "FOselectMethCluster" "FOtotalClusters"     "FOsampledClusters"
  [46] "FOprobCluster"



  sa$FTsampler  <- "Observer"

  trsa <- left_join(tr, sa, by = c("tripId" = "tripId"))
  FTfoNum <-
    summarise(group_by(subset(trsa, gearQuality == "V"), tripId), FTfoNum =
                length(unique(sampleId)))

  tr_1 <- left_join(tr, FTfoNum)
  tr_1 <- arrange(mutate(tr_1, FTfoNum = ifelse(is.na(FTfoNum), 0, FTfoNum)), FTfoNum)


  tr_1$FTdepLoc <- "" #Not in our national database, but for vessel with logbook we can get the info from the logbook if needed - don't think it would be needed?
  tr_1$FTdepDate <- as.Date(tr_1$dateStart)
  tr_1$FTdepTime <- strftime(tr_1$dateStart, format = "%H:%M:%S")
  tr_1 <-
    left_join(tr_1, havn[, c("harbour", "harbourEU")], by = c("harbourLanding" =
                                                               "harbour"))
  tr_1$FTarvLoc <- tr_1$harbourEU
  tr_1$FTarvDate <- as.Date(tr_1$dateEnd)
  tr_1$FTarvTime <- strftime(tr_1$dateEnd, format = "%H:%M:%S")

  tr_1$FTtotal <- ""
  tr_1$FTsampled <- ""
  tr_1$FTprob <- ""
  tr_1$FTselectMeth <- ""

  tr_1$FTselectMethCluster <- ""
  tr_1$FTtotalClusters <- ""
  tr_1$FTsampledClusters <- ""
  tr_1$FTprobCluster <- ""
  tr_1$FTnoSampReason <- ""

  FT <- select(tr_1, one_of(ft_temp), tripId)

  return(list(FT, ft_temp))

  }
