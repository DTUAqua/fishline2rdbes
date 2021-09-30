
#' FishLine 2 RDBES, Vessel selection (VS)
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
#'
#'
#'

VS_fishline_2_rdbes <-
  function(data_model_baseTypes_path = "Q:/mynd/RDB/create_RDBES_data/references",
           encryptedVesselCode_path = "Q:/mynd/RDB/create_RDBES_data/RDBES_data_call_2021/output/for_production",
           years = 2016,
           cruises = c("MON", "SEAS", "IN-HIRT"),
           type = "everything"
           )
  {


    # Input for testing ----

    # data_model_baseTypes_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/references"
    # encryptedVesselCode_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/RDBES_data_call_2021/output/for_production"
    # years <- c(2018:2020)
    # cruises <- c("MON",  "SEAS", "TBM20", "BLH18")
    # type <- "everything"

    # Set-up ----

    library(RODBC)
    library(sqldf)
    library(dplyr)
    library(stringr)
    library(haven)

    data_model <- readRDS(paste0(data_model_baseTypes_path, "/BaseTypes.rds"))

    vs_temp <- filter(data_model, substr(name, 1, 2) == "VS")
    vs_temp_t <- c("VSrecordType", t(vs_temp$name)[1:nrow(vs_temp)])

    # Get needed stuff ----

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    tr <- sqlQuery(
      channel,
      paste(
        "select * FROM dbo.Trip
         WHERE (Trip.year between ", min(years), " and ", max(years) , ")
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

    fl_ftj_id$Vessel_identifier_fid <- fl_ftj_id$L_platformId

    # Get VDencryptedVesselCode

    VDencryptedVesselCode <- read.csv(paste0(encryptedVesselCode_path, "/DNK_", min(years), "_", max(years), "_HVD.csv"), sep = ";")

    # Add needed stuff ----
    # VDencryptedVesselCode

    tr_1 <- left_join(tr, VDencryptedVesselCode[c("VDencryptedVesselCode", "tripId")])

    # Vessel_identifier_fid as unit name
    # Depends on country

    vessel_dnk <- subset(tr_1, nationalityPlatform1 == "DNK")
    vessel_not_dnk <- subset(tr_1, nationalityPlatform1 != "DNK" |
                               is.na(nationalityPlatform1))

    # DNK

    vessel_dnk$arvDate <- as.Date(vessel_dnk$dateEnd)
    vessel_dnk_1 <-
      distinct(vessel_dnk, platform1, nationalityPlatform1, arvDate, tripId)
    vessel_dnk_1$platform1 <- as.character(vessel_dnk_1$platform1)

    ftj_id_1 <-
      select(ftj_id,
             fid,
             Vessel_identifier_fid,
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

    # Not DNK

    vessel_not_dnk_1 <- left_join(vessel_not_dnk, fl_ftj_id, by = c("platform1" = "platform"))

    # Combine DNK and not DNK

    combined <- bind_rows(vessel_dnk_3, vessel_not_dnk_1)

    # Recode for VS ----

    vs <-
      distinct(combined,
               tripId,
               tripType,
               cruise,
               trip,
               VDencryptedVesselCode,
               Vessel_identifier_fid,
               year)

    vs$VSid <- vs$tripId
    vs$VSrecordType <- "VD"

    vs$VSencryptedVesselCode <- vs$VDencryptedVesselCode

    vs$VSsequenceNumber <- NA  #To be coded manual - depends on design
    vs$VSstratification <- NA  #To be coded manual - depends on design
    vs$VSstratumName <- NA     #To be coded manual - depends on design
    vs$VSclustering <- NA      #To be coded manual - depends on design
    vs$VSclusterName <- NA     #To be coded manual - depends on design

    vs$VSsampler[vs$cruise %in% c("MON", "SEAS")] <- "Observer"
    vs$VSsampler[substr(vs$cruise, 1, 3) %in% c("BLH", "BRS", "MAKK", "SIL", "SPE", "TBM")] <-
      "Self-Sampling"

    vs$VSnumberTotal <- NA      #To be coded manual - depends on design
    vs$VSnumberSampled <- NA    #To be coded manual - depends on design
    vs$VSselectionProb <- NA    #To be coded manual - depends on design
    vs$VSinclusionProb <- NA    #To be coded manual - depends on design
    vs$VSselectionMethod <- NA  #To be coded manual - depends on design

    vs$VSunitName <- vs$Vessel_identifier_fid

    vs$VSselectionMethodCluster <- NA  #To be coded manual - depends on design
    vs$VSnumberTotalClusters <- NA     #To be coded manual - depends on design
    vs$VSnumberSampledClusters <- NA   #To be coded manual - depends on design
    vs$VSselectionProbCluster <- NA    #To be coded manual - depends on design
    vs$VSinclusionProbCluster <- NA       #To be coded manual - depends on design

    vs$VSsampled <- "Y"                   #For now only including Y - N requires manual coding
    # Should be 0, but that is not possible at the moment
    # vs$VSsampled[vs$cruise %in% c("MON", "SEAS") & is.na(vs$numberOfHaulsOrSets)] <- 0
    vs$VSreasonNotSampled <- NA           #Reasoning requires manual coding

    if (type == "only_mandatory") {
      vs_temp_optional <-
        filter(data_model, substr(name, 1, 2) == "VS" & min == 0)
      vs_temp_optional_t <-
        factor(t(vs_temp_optional$name)[1:nrow(vs_temp_optional)])

      for (i in levels(vs_temp_optional_t)) {
        eval(parse(text = paste0("vs$", i, " <- NA")))

      }
    }

    VS <- select(vs, one_of(vs_temp_t), tripId, VSid)

    return(list(FT, ft_temp, ft_temp_t))

  }
