

# Script for creating data_for_testing_fun_get_gear_info
library(usethis)
library(dplyr)
library(RODBC)

channel <- RODBC::odbcConnect("FishLineDW")
samp <- RODBC::sqlQuery(
  channel,
  paste(
    "select Sample.*, Trip.* FROM Sample INNER JOIN
                  Trip ON Sample.tripId = Trip.tripId
         WHERE (Trip.year = 2022)",
    sep = ""
  )
)
close(channel)

unique(samp$fisheryType)

samp <- subset(samp, !(substr(cruise, 1, 2) %in% c("BV", "To", "IB", "IE", "HE", "ME", "BI", "KA", "Tu")))

unique(samp$cruise)

# Mask cruise, trip, station

samp$cruise <- "CRUISE"
samp$trip <- "10"
samp$station <- "1"

data_for_testing_fun_get_gear_info <- select(samp, sampleId, year, cruise, trip, tripType, fisheryType, station, dfuArea, gearType, targetSpecies1, meshSize, selectionDevice,
                 numberTrawls, numNets, heightNets, lengthNets)

usethis::use_data(data_for_testing_fun_get_gear_info, overwrite = T)
