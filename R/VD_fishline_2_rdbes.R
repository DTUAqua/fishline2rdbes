
#' FishLine 2 RDBES, Vessel details (VD)
#'
#' @description Converts samples data from national database (fishLine) to RDBES.
#' Data model v. 1.19
#'
#' @param data_model Where to find the baseTypes with the data model
#' @param year Only takes a single year for now
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
#'
#'
#'

VD_fishline_2_rdbes <-
  function(data_model = fields_sequence,
           year = 2016,
           cruises = c("MON", "SEAS", "IN-HIRT"),
           type = "only_mandatory")
  {


    # Input for testing ----
    data_model <-
      readRDS("Q:/mynd/RDB/create_RDBES_data/references/BaseTypes.rds")
    year = 2018
    cruises = c("MON", "SEAS", "IN-HIRT")
    type = "only_mandatory"

    # Set-up ----

    library(RODBC)
    library(sqldf)
    library(dplyr)
    library(stringr)
    library(haven)

    vd_temp <- filter(data_model, substr(name, 1, 2) == "VD")
    vd_temp_t <- c("VDrecordType", t(vd_temp$name)[1:nrow(vd_temp)])

    # Get needed stuff ----

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    tr <- sqlQuery(
      channel,
      paste(
        "select * FROM dbo.Trip
         WHERE (Trip.year = ", year , ")
                and Trip.cruise in ('", paste(cruises, collapse = "','"),
        "')",
        sep = ""
      )
    )
    close(channel)

    # Get id's for DNK vessels form the Danish vessel registry
    ftj_id <- read_sas("Q:/dfad/data/Data/Ftjreg/ftjid.sas7bdat")

    # Get id's for foreign vessels from FishLine
    channel <- odbcConnect("FishLine")
    fl_ftj_id <-
      sqlQuery(
        channel,
        paste(
          "SELECT L_platformId, platform
         FROM L_Platform
         WHERE (nationality <> 'DNK')"
        )
      )
    close(channel)

    fl_ftj_id$Vessel_identifier_Fid <- fl_ftj_id$L_platformId

    # Get country code reference

    ctry <-
      read.csv("Q:/mynd/SAS Library/Country/country.csv", sep = ";")

    # Get LOCODE's for homeport
    locode <- read_sas("Q:/mynd/SAS Library/Lplads/lplads.sas7bdat")

    # Add needed stuff ----

    # Vessel_identifier_Fid & Homeport
    # Depends on country

    vessel_dnk <- subset(tr, nationalityPlatform1 == "DNK")
    vessel_not_dnk <- subset(tr, nationalityPlatform1 != "DNK" |
                           is.na(nationalityPlatform1))

    # DNK

    vessel_dnk$arvDate <- as.Date(vessel_dnk$dateEnd)
    vessel_dnk_1 <-
      distinct(vessel_dnk, platform1, nationalityPlatform1, arvDate, tripId)
    vessel_dnk_1$platform1 <- as.character(vessel_dnk_1$platform1)

    ftj_id_1 <-
      select(ftj_id,
             fid,
             Vessel_identifier_Fid,
             vstart,
             vslut,
             oal,
             bhavn,
             kw,
             bt,
             brt)

    vessel_dnk_2 <-
      sqldf("select * from vessel_dnk_1 a left join ftj_id_1 b on a.platform1=b.fid")
    vessel_dnk_2 <- subset(vessel_dnk_2, arvDate >= vstart & arvDate <= vslut)
    vessel_dnk_3 <- full_join(vessel_dnk, vessel_dnk_2)
    vessel_dnk_4 <-
      left_join(vessel_dnk_3, locode[, c("start", "harbourEU")], by = c("bhavn" = "start"))

    # Not DNK

    vessel_not_dnk_1 <- left_join(vessel_not_dnk, fl_ftj_id, by = c("platform1" = "platform"))

    # Combine DNK and not DNK

    combined <- bind_rows(vessel_dnk_4, vessel_not_dnk_1)

    # Add flagcountry

    combined_1 <- left_join(combined, ctry, by = c("nationalityPlatform1" = "nationality"))

    test <- unique(combined_1[c("nationalityPlatform1", "ISO_3166_ices")])

    # Recode for VD ----

    vd <-
      distinct(combined_1,
               tripId,
               platform1,
               year,
               Vessel_identifier_Fid,
               oal,
               harbourEU,
               kw,
               bt,
               brt,
               ISO_3166_ices)

    VDid <-
      distinct(combined_1,
               platform1,
               Vessel_identifier_Fid,
               oal,
               harbourEU,
               kw,
               bt,
               brt)

    VDid$VDid <- c(1:nrow(VDid))

    vd <- left_join(vd, VDid)
    vd$VDrecordType <- "VD"
    vd$VDencryptedVesselCode <- as.character(vd$Vessel_identifier_Fid)
    vd$VDyear <- vd$year
    vd$VDcountry <- "DK"
    vd$VDhomePort <- vd$harbourEU
    vd$VDflagCountry <- vd$ISO_3166_ices
    vd$VDlength <- round(vd$oal, digits = 0)
    vd$VDlengthCategory <- ifelse(vd$VDlength < 8,
                                  "<8",
                                  ifelse(
                                    8 <= vd$VDlength & vd$VDlength < 10,
                                    "8-<10",
                                    ifelse(
                                      10 <= vd$VDlength & vd$VDlength < 12,
                                      "10-<12",
                                      ifelse(
                                        12 <= vd$VDlength & vd$VDlength < 15,
                                        "12-<15",
                                        ifelse(
                                          15 <= vd$VDlength & vd$VDlength < 18,
                                          "15-<18",
                                          ifelse(
                                            18 <= vd$VDlength & vd$VDlength < 24,
                                            "18-<24",
                                            ifelse(
                                              24 <= vd$VDlength & vd$VDlength < 40,
                                              "24-<40",
                                              ifelse(40 <= vd$VDlength, "40<", "Unknown")
                                            )
                                          )
                                        )
                                      )
                                    )
                                  ))

    test_VDlenCat <- distinct(vd, VDlength, VDlengthCategory)

    vd$VDpower <- round(vd$kw, digits = 0)
    vd$VDtonAge <-
      round(vd$bt, digits = 0) #OBS needs to look at both bt and brt
    vd$VDtonUnit <- "GT"

    if (type == "only_mandatory") {
      vd_temp_optional <-
        filter(data_model, substr(name, 1, 2) == "VD" & min == 0)
      vd_temp_optional_t <-
        factor(t(vd_temp_optional$name)[1:nrow(vd_temp_optional)])

      for (i in levels(vd_temp_optional_t)) {
        eval(parse(text = paste0("vd$", i, " <- NA")))

      }
    }

    VD <- select(vd, one_of(vd_temp_t), tripId)

    return(list(VD, vd_temp, vd_temp_t))

  }
