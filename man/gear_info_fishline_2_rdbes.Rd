% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/gear_info_fishline_2_rdbes.R
\name{gear_info_fishline_2_rdbes}
\alias{gear_info_fishline_2_rdbes}
\title{Gear information from FishLine required in the RDBES}
\usage{
gear_info_fishline_2_rdbes(
  df = samp,
  record_type,
  ices_area_already_addded = F,
  checks = F
)
}
\arguments{
\item{selection_device:}{TO DO}
}
\description{
The function populate nationalFishingActivity, metier5,
metier6, gear, meshSize, selectionDevice, selectionDeviceMeshSize,
targetSpecies, incidentalByCatchMitigationDeviceFirst,
incidentalByCatchMitigationDeviceTargetFirst,
incidentalByCatchMitigationDeviceSecond,
incidentalByCatchMitigationDeviceTargetSecond and gearDimensions
from gear informations in FishLine
}
\author{
Kirsten Birch Håkansson, DTU Aqua & people from the RCG metier group
}
