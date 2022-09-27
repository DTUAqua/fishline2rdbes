
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
  function(ref_path = "Q:/mynd/RDB/create_RDBES_data/references",
           encryptedVesselCode_path = "Q:/mynd/kibi/RDBES/create_RDBES_data/RDBES_data_call_2022/output/for_production",
           sampling_scheme = "DNK_Market_Sampling",
           years = 2016,
           type = "everything")
  {


    # Input for testing ----

    # ref_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/references"
    # encryptedVesselCode_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/output/data_call_2022/for_production"
    # sampling_scheme = "DNK_AtSea_Observer_Active"
    # years <- 2021
    # type <- "everything"

    # Set-up ----

    library(RODBC)
    library(sqldf)
    library(dplyr)
    library(stringr)
    library(haven)

    data_model <- readRDS(paste0(ref_path, "/BaseTypes.rds"))
    link <-
      read.csv(paste0(ref_path, "/link_fishLine_sampling_designs.csv"))

    link <- subset(link, DEsamplingScheme == sampling_scheme)

    vs_temp <- filter(data_model, substr(name, 1, 2) == "VS")
    vs_temp_t <- c("VSrecordType", t(vs_temp$name)[1:nrow(vs_temp)])

    trips <- unique(link$tripId[!is.na(link$tripId)])

    # Get needed stuff ----

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    tr <- sqlQuery(
      channel,
      paste(
        "select Sample.*, Trip.* FROM Sample INNER JOIN
                  Trip ON Sample.tripId = Trip.tripId
         WHERE (Trip.year between ",
        min(years),
        " and ",
        max(years),
        ")
                and Trip.tripId in (",
        paste(trips, collapse = ","),
        ")",
        sep = ""
      )
    )
    close(channel)

    # Get VDencryptedVesselCode

    VDencryptedVesselCode <- read.csv(paste0(encryptedVesselCode_path, "/DNK_", min(years), "_", max(years), "_HVD.csv"), sep = ";")

    # Add needed stuff ----
    # VDencryptedVesselCode

    tr_1 <- left_join(tr, VDencryptedVesselCode[c("VDencryptedVesselCode", "tripId")])

    # # Vessel_identifier_fid as unit name
    # # Depends on country
    #
    # vessel_dnk <- subset(tr_1, nationalityPlatform1 == "DNK")
    # vessel_not_dnk <- subset(tr_1, nationalityPlatform1 != "DNK" |
    #                            is.na(nationalityPlatform1))
    #
    # # DNK
    #
    # vessel_dnk$arvDate <- as.Date(vessel_dnk$dateEnd)
    # vessel_dnk_1 <-
    #   distinct(vessel_dnk, platform1, nationalityPlatform1, arvDate, tripId)
    # vessel_dnk_1$platform1 <- as.character(vessel_dnk_1$platform1)
    #
    # ftj_id_1 <-
    #   select(ftj_id,
    #          fid,
    #          Vessel_identifier_fid,
    #          vstart,
    #          vslut,
    #          oal,
    #          bhavn,
    #          kw,
    #          bt,
    #          brt)
    #
    # vessel_dnk_2 <-
    #   sqldf("select * from vessel_dnk_1 a left join ftj_id_1 b on a.platform1=b.fid")
    # vessel_dnk_2 <- subset(vessel_dnk_2, arvDate >= vstart & arvDate <= vslut)
    # vessel_dnk_3 <- full_join(vessel_dnk, vessel_dnk_2)
    #
    # # Not DNK
    #
    # vessel_not_dnk_1 <- left_join(vessel_not_dnk, fl_ftj_id, by = c("platform1" = "platform"))
    #
    # # Combine DNK and not DNK
    #
    # combined <- bind_rows(vessel_dnk_3, vessel_not_dnk_1)

    tr_2 <- left_join(tr_1, distinct(link[,grep("^[VS]|^[tripId]", names(link), value = T)]))

    # Recode for VS ----

    vs <- tr_2


    vs$VSid <- ""
    vs$VSrecordType <- "VS"

    vs$VSencryptedVesselCode <- vs$VDencryptedVesselCode

    vs$VSsequenceNumber <- NA   #To be coded manual - depends on design
    vs$VSclustering <- "N"      #To be coded manual - depends on design
    vs$VSclusterName <- "No"     #To be coded manual - depends on design

    vs$VSsampler[vs$cruise %in% c("MON", "SEAS")] <- "Observer"
    vs$VSsampler[substr(vs$cruise, 1, 3) %in% c("BLH", "BRS", "MAK", "SIL", "SPE", "TBM")] <-
      "Self-Sampling"

    vs$VSunitName <- vs$VDencryptedVesselCode

    vs$VSselectionMethodCluster <- ""  #To be coded manual - depends on design
    vs$VSnumberTotalClusters <- ""     #To be coded manual - depends on design
    vs$VSnumberSampledClusters <- ""   #To be coded manual - depends on design
    vs$VSselectionProbCluster <- ""    #To be coded manual - depends on design
    vs$VSinclusionProbCluster <- ""       #To be coded manual - depends on design

    vs$VSsampled <- "Y"                   #For now only including Y - N requires manual coding
    # Should be 0, but that is not possible at the moment
    # vs$VSsampled[vs$cruise %in% c("MON", "SEAS") & is.na(vs$numberOfHaulsOrSets)] <- 0
    vs$VSreasonNotSampled <- ""           #Reasoning requires manual coding

    if (type == "only_mandatory") {
      vs_temp_optional <-
        filter(data_model, substr(name, 1, 2) == "VS" & min == 0)
      vs_temp_optional_t <-
        factor(t(vs_temp_optional$name)[1:nrow(vs_temp_optional)])

      for (i in levels(vs_temp_optional_t)) {
        eval(parse(text = paste0("vs$", i, " <- ''")))

      }
    }

    VS <- select(vs, one_of(vs_temp_t), tripId)

    return(list(VS, vs_temp, vs_temp_t))

  }
