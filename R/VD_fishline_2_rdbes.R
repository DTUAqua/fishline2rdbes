
#' FishLine 2 RDBES, Vessel details (VD)
#'
#' @description Converts samples data from national database (fishLine) to RDBES.
#'
#' @param year sample year
#' @param cruises string with names of cruises in national database
#'
#' @author Kirsten Birch Håkansson, DTU Aqua
#'
#' @return
#' @export
#' @importFrom RODBC odbcConnect sqlQuery
#' @importFrom lubridate today
#' @importFrom plyr rbind.fill
#' @importFrom dplyr left_join distinct bind_rows
#' @importFrom haven read_sas
#'
#' @examples
#'
#'
#'

VD_fishline_2_rdbes <-
  function(year = 2022,
           cruises = c("MON", "SEAS", "IN-HIRT", "IN-LYNG"),
           data_model_path)
  {


    # Input for testing ----

    # data_model_baseTypes_path <- "Q:/mynd/RDB/create_RDBES_data/references"
    # years <- 2021
    # cruises <- c("MON", "SEAS", "IN-HIRT", "IN-LYNG")
    # type <- "only_mandatory"
    # encrypter_prefix <- "DNK11084"

    # Set-up ----
    library(RODBC)
    library(plyr)
    # library(sqldf)
    library(dplyr)
    # library(stringr)
    library(haven)

    # Get data model ----
    VD <- get_data_model_vd_sl("Vessel Details", data_model_path = data_model_path)

    # Get needed stuff ----

    ## Get data from FishLine ----
    channel <- RODBC::odbcConnect("FishLineDW")
    dat <- RODBC::sqlQuery(
      channel,
      paste(
        "select * FROM dbo.Trip
         WHERE (Trip.year between ", min(year), " and ", max(year) , ")
                and Trip.cruise in ('", paste(cruises, collapse = "','"),
        "')",
        sep = ""
      )
    )
    close(channel)

    ## Get references and codeliste ----

    # Get encrypted id's for DNK vessels form the Danish vessel registry

    ftj_id <- read.csv("Q:/dfad/data/Data/Ftjreg/encryptions_RDBES.csv")
    ftj_id$VDencryptedVesselCode <- paste0(ftj_id$Encrypted_ID, "_", ftj_id$Version_ID)
    ftj_id$vstart <- as.Date(ftj_id$vstart)
    ftj_id$vslut <- as.Date(ftj_id$vslut)
    ftj_id$vslut[is.na(ftj_id$vslut)] <- lubridate::today()
    ftj_id$bhavn <- as.character(ftj_id$bhavn)
    ftj_id$btbrt <- as.numeric(ftj_id$btbrt)

    # Get country code reference

    ctry <-
      read.csv("Q:/mynd/SAS Library/Country/country.csv", sep = ";")

    # Get LOCODE's for homeport
    locode <- read_sas("Q:/mynd/SAS Library/Lplads/lplads.sas7bdat")

    # Add needed stuff to dat ----
    # This only works for DNK vessels, since we only have vessel info about DNK vessels
    # Some of the info could probably be found in the EU fleet registre, but for now these just get a
    # VDencryptedVesselCode that tells where it is from

    vessel_dnk <- subset(dat, nationalityPlatform1 == "DNK")
    vessel_not_dnk <- subset(dat, nationalityPlatform1 != "DNK" |
                           is.na(nationalityPlatform1))

    ## DNK ----

    vessel_dnk$dateEnd <- as.Date(vessel_dnk$dateEnd)
    vessel_by <- dplyr::join_by(platform1 == fid,
                                dateEnd >= vstart, dateEnd <= vslut)

    vessel_dnk_1 <- dplyr::left_join(vessel_dnk, ftj_id, vessel_by)

    vessel_dnk_2 <-
      dplyr::left_join(vessel_dnk_1, locode[, c("start", "harbourEU")], by = c("bhavn" = "start"))

    ## Not DNK ----
    # no info at the momemt

    vessel_not_dnk_1 <- vessel_not_dnk

    # Combine DNK and not DNK

    dat_1 <- dplyr::bind_rows(vessel_dnk_2, vessel_not_dnk_1)

    # Add flagcountry

    dat_2 <- dplyr::left_join(dat_1, ctry, by = c("nationalityPlatform1" = "nationality"))

    # test <- unique(combined_1[c("nationalityPlatform1", "ISO_3166_ices")])

    # Recode for VD ----

    vd <- dat_2

    VDid <-
      dplyr::distinct(vd,
               platform1,
               VDencryptedVesselCode,
               oal,
               harbourEU,
               kw,
               btbrt)

    VDid$VDid <- c(1:nrow(VDid))

    vd <- dplyr::left_join(vd, VDid)
    vd$VDrecordType <- "VD"

    # Fix not known vessels and foreign

    vd$VDencryptedVesselCode[is.na(vd$VDencryptedVesselCode)] <-
      "DNK - Unknown vessel"

    vd$VDencryptedVesselCode[!(is.na(vd$ISO_3166_ices)) & vd$ISO_3166_ices != "DK"] <-
      paste0("DNK - ", vd$ISO_3166_ices[!(is.na(vd$ISO_3166_ices)) & vd$ISO_3166_ices != "DK"], " vessel")

    # dplyr::distinct(vd, ISO_3166_ices, VDencryptedVesselCode)

    vd$VDyear <- vd$year

    vd$VDcountry <- "DK" # Not always TRUE, but this is not recorded

    vd$VDhomePort <- vd$harbourEU

    vd$VDflagCountry <- vd$ISO_3166_ices
    vd$VDflagCountry[is.na(vd$VDflagCountry) & vd$VDencryptedVesselCode == "DNK - Unknown vessel"] <- "DK" # It is not possible to upload foreign vessels

    vd$VDlength <- round(vd$oal, digits = 2)
    vd$VDlengthCategory <- ifelse(vd$VDlength < 6,
                                  "VL0006",
                                  ifelse(
                                    6 <= vd$VDlength & vd$VDlength < 8,
                                    "VL0608",
                                    ifelse(
                                      8 <= vd$VDlength & vd$VDlength < 10,
                                      "VL0810",
                                      ifelse(
                                        10 <= vd$VDlength & vd$VDlength < 12,
                                        "VL1012",
                                        ifelse(
                                          12 <= vd$VDlength & vd$VDlength < 15,
                                          "VL1215",
                                          ifelse(
                                            15 <= vd$VDlength & vd$VDlength < 18,
                                            "VL1518",
                                            ifelse(
                                              18 <= vd$VDlength & vd$VDlength < 24,
                                              "VL1824",
                                              ifelse(24 <= vd$VDlength & vd$VDlength < 40,
                                                     "VL2440",
                                              ifelse(40 <= vd$VDlength, "VL40XX", "NK")
                                            ))
                                          )
                                        )
                                      )
                                    )
                                  ))

    vd$VDlengthCategory[is.na(vd$VDlengthCategory)] <- "NK"

    # test_VDlenCat <- dplyr::distinct(vd, VDlength, VDlengthCategory)

    vd$VDpower <- round(vd$kw, digits = 0)
    vd$VDtonnage <-
      round(vd$btbrt, digits = 0) #OBS needs to look at both bt and brt
    vd$VDtonUnit <- "GT"

    vd_ok <- subset(vd, !is.na(VDencryptedVesselCode))

    vd_not_ok <- subset(vd, is.na(VDencryptedVesselCode) | VDencryptedVesselCode == "DNK - Unknown vessel")

    vd_not_ok <-
      dplyr::right_join(
        select(dat, year, cruise, trip, tripId, samplingType, platform1),
        select(vd_not_ok, tripId, VDencryptedVesselCode)
      )

    # VD <- select(vd_ok, one_of(vd_temp_t), tripId)

    vd <- plyr::rbind.fill(VD, vd)

    vd <- vd[ , c(names(VD), "tripId", "VDid")]

    vd[is.na(vd) ] <- ""

    return(list(vd, VD, vd_not_ok))

  }
