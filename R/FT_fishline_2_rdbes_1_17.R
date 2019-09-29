
#' FishLine 2 RDBES, fishing trip (FT)
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

FT_fishline_2_rdbes <-
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

  ft_temp <- read.xlsx(data_model, sheet = 8)
  var_keep <- na.omit(ft_temp$Order)
  ft_temp <- t(ft_temp$R.Name)[1:max(var_keep)]

  # Get data from FishLine
  channel <- odbcConnect("FishLineDW")
  tr <- sqlQuery(channel, paste("select * FROM dbo.Trip
                                 WHERE (Trip.year = ", year , ")
                                 and Trip.cruise in ('", paste(cruises, collapse = "','"), "')", sep = ""))
  sa <- sqlQuery(channel, paste("select * FROM dbo.Sample
                                 WHERE gearQuality = 'V' and
                                 (Sample.year = ", year , ")
                                 and Sample.cruise in ('", paste(cruises, collapse = "','"), "')", sep = ""))
  close(channel)

  channel2 <- odbcConnect("FishLine")
  havn <- sqlQuery(channel2, paste("select * FROM dbo.L_harbour"))
  close(channel2)


  #Start coding the FT variables

  tr$FTid <- tr$tripId
  tr$OSid <- ""
  tr$VSid <- ""
  tr$SDid <- ""
  tr$VDid <- ""

  tr$FTrecType <- "FT"
  tr$FTnatCode <- paste(tr$cruise, tr$trip, sep = " - ")

  tr$FTstratification <- ""
  tr$FTstratum <- ""
  tr$FTclustering <- "No"
  tr$FTclusterName <- "U"

  tr$FTsampler  <- "Observer"

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
