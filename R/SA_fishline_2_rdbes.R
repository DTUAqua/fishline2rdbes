
#' FishLine 2 RDBES, Landing event (LE)
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
SA_fishline_2_rdbes <-
  function(ref_path = "Q:/mynd/RDB/create_RDBES_data/references",
           sampling_scheme = "DNK_Market_Sampling",
           years = 2016,
           type = "everything") {
    # Input for testing ----

    # ref_path <- "Q:/mynd/kibi/RDBES/create_RDBES_data/references"
    # years <- c(2021)
    # sampling_scheme <- "DNK_Market_Sampling"
    # type <- "everything"

    # Set-up ----


    library(sqldf)
    library(tidyr)
    library(dplyr)
    library(stringr)
    library(haven)

    data_model <- readRDS(paste0(ref_path, "/BaseTypes.rds"))
    link <-
      read.csv(paste0(ref_path, "/link_fishLine_sampling_designs.csv"))

    link <- subset(link, DEsamplingScheme == sampling_scheme)

    sa_temp <- filter(data_model, substr(name, 1, 2) == "SA")
    sa_temp_t <- c("SArecordType", t(sa_temp$name)[1:nrow(sa_temp)])

    trips <- unique(link$tripId[!is.na(link$tripId)])

    # Get needed stuff ----

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    samp <- sqlQuery(
      channel,
      paste(
        "SELECT specieslist.*, Animal.animalId, Animal.individNum,
                  Animal.representative, Animal.number as ani_number
                  FROM SpeciesList INNER JOIN
                  Sample ON SpeciesList.sampleId = Sample.sampleId LEFT OUTER JOIN
                  Animal ON SpeciesList.speciesListId = Animal.speciesListId
         WHERE (Specieslist.year between ",
        min(years),
        " and ",
        max(years),
        ")
                and Sample.tripId in (",
        paste(trips, collapse = ","),
        ")",
        sep = ""
      )
    )
    close(channel)

    samp <- subset(samp, !(is.na(dfuArea)))

    channel <- odbcConnect("FishLine")
    area <- sqlQuery(
      channel,
      paste("SELECT DFUArea, areaICES FROM L_DFUArea",
        sep = ""
      )
    )
    art <-
      sqlQuery(
        channel,
        paste(
          "select speciesCode, latin, speciesFAO, aphiaID FROM dbo.L_species"
        )
      )
    close(channel)

    # Add needed stuff ----

    samp$dfuArea <- as.character(samp$dfuArea)

    sa <- left_join(samp, area, by = c("dfuArea" = "DFUArea"))
    sa <- left_join(sa, art)

    # Identify lower hierachy


    lh <-
      distinct(
        sa,
        sampleId,
        speciesListId,
        cruise,
        trip,
        station,
        speciesCode,
        sizeSortingEU,
        representative,
        individNum
      )
    lh_uniq <-
      distinct(
        sa,
        sampleId,
        speciesListId,
        cruise,
        trip,
        station,
        speciesCode,
        sizeSortingEU
      )

    test <- subset(sa, is.na(animalId))

    lh_1 <-
      summarise(group_by(subset(sa,!is.na(individNum)), speciesListId, representative),
                no_indi = length(unique(individNum)))

    lh_num_sum <- summarise(group_by(subset(sa, representative == "ja"), speciesListId), no_fish = sum(ani_number, na.rm = T))

    lh_1_num <- left_join(lh_1, lh_num_sum)

    lh_2 <- left_join(lh_uniq, lh_1_num)

    lh_2_t <- spread(lh_2, key = representative, value = no_indi)

    # TODO - code B
    lh_2_t$SAlowerHierarchy[is.na(lh_2_t$no_fish)] <- "D"
    lh_2_t$SAlowerHierarchy[is.na(lh_2_t$ja) & !(is.na(lh_2_t$nej)) & lh_2_t$no_fish > 0] <- "A"
    lh_2_t$SAlowerHierarchy[!is.na(lh_2_t$ja) & (is.na(lh_2_t$nej)) & lh_2_t$no_fish > 0] <- "C"

    sa_1 <- left_join(sa, lh_2_t)

    table(sa$SAlowerHierarchy)

    # sa_2 <- subset(sa_1, representative == "ja")

    # Recode for SA ----

    sa <- sa_1

    sa$SAid <- sa$speciesListId
    sa$SArecordType <- "SA"

    sa <- arrange(sa, sampleId, speciesListId)
    sa$SAsequenceNumber <- NA

    sa$SAparentSequenceNumber <- ""
    sa$SAstratification <- "Y"

    sa <-
      mutate(
        sa,
        SAstratumName = paste(
          landingCategory,
          sizeSortingEU,
          sizeSortingDFU,
          sexCode,
          ovigorous,
          sep = "-"
        )
      )
    sa <-
      mutate(sa, SAstratumName = str_replace_all(SAstratumName, "-NA", ""))

    sa$SAspeciesCode <- sa$aphiaID
    sa$SAspeciesCodeFAO <- sa$speciesFAO

    unique(sa$treatment)

    sa <-
      mutate(sa, SApresentation = ifelse(
        treatment == "UR",
        "WHL",
        ifelse(
          treatment == "RH",
          "GUT",
          ifelse(
            treatment == "RU",
            "GUH",
            ifelse(
              treatment %in% c("VV", "VK"),
              "WNG",
              ifelse(treatment == "TAL", "Tail", NA)
            )
          )
        )
      ))

    test <- filter(sa, is.na(SApresentation))

    sa$SAspecimensState <- "NotDetermined"

    sa$SAstateOfProcessing <- "UNK"

    sa <- mutate(sa, SAcatchCategory = ifelse(landingCategory == "BMS", "BMS",
                                              ifelse(landingCategory == "DIS", "Dis",
                                                     ifelse(landingCategory %in% c("KON", "IND"), "Lan", NA))))

    sa$SAlandingCategory[sa$landingCategory == "KON"] <- "HuC"
    sa$SAlandingCategory[sa$landingCategory %in% c("IND")] <- "Ind"
    sa$SAlandingCategory[sa$landingCategory %in% c("BMS")] <- "None"
    sa$SAcommSizeCatScale <- "EU"
    sa$SAcommSizeCat <- sa$sizeSortingEU
    sa$SAcommSizeCat[sa$SAcommSizeCat == "123"] <- "3"
    sa <- mutate(sa, SAsex = ifelse(is.na(sexCode), "U", as.character(sexCode)))

    sa$SAexclusiveEconomicZoneIndicator <- ""

    sa$SAarea <- sa$areaICES
    sa$SArectangle <- sa$statisticalRectangle
    sa$SArectangle[is.na(sa$SArectangle)] <- ""
    sa$SAgsaSubarea <- "NotApplicable"
    sa$SAjurisdictionArea <- ""

    sa$SAnationalFishingActivity <- ""
    sa$SAmetier5 <- ""
    sa$SAmetier6 <- "MIS_MIS_0_0_0"
    sa$SAgear <- sa$gearType
    sa$SAgear[is.na(sa$SAgear)] <- "MIS"
    sa$SAgear[sa$gearType == "LL"] <- "LLS"
    sa$SAgear[sa$gearType == "TBN"] <- "OTB"
    sa$SAgear[sa$gearType == "FIX"] <- "FPO"
    sa$SAgear[sa$gearType == "LHP"] <- "LHM"
    sa$SAmeshSize <- sa$meshSize
    sa$SAselectionDevice <- ""
    sa$SAselectionDeviceMeshSize <- ""

    sa <-
      transform(
        sa,
        SAtotalWeightMeasured = round(pmin(weightStep0, weightStep1, weightStep2, weightStep3, na.rm =
                                             T) * raisingFactor * 1000, digits = 0)
      )
    sa <-
      transform(
        sa,
        SAsampleWeightMeasured = round(pmin(weightStep0, weightStep1, weightStep2, weightStep3, na.rm =
                                              T) * 1000, digits = 0)
      )
    sa <-
      transform(
        sa,
        SAtotalWeightLive = round(pmin(weightStep0, weightStep1, weightStep2, weightStep3, na.rm =
                                             T) * raisingFactor * 1000 * treatmentFactor, digits = 0)
      )
    sa <-
      transform(
        sa,
        SAsampleWeightLive = round(pmin(weightStep0, weightStep1, weightStep2, weightStep3, na.rm =
                                              T) * 1000 * treatmentFactor, digits = 0)
      )

    sa$SAconversionFactorMeasLive <- sa$treatmentFactor


    sa$SAsampler <- "Observer" # sa$samplingMethod # That is not completely TRUE

    sa$SAnumberTotal <-
      "" # To be coded manual - depends on design
    sa$SAnumberSampled <-
      "" # To be coded manual - depends on design
    sa$SAselectionProb <-
      "" # To be coded manual - depends on design
    sa$SAinclusionProb <-
      "" # To be coded manual - depends on design
    sa$SAselectionMethod <-
      "NPQSRSWOR"

    sa$SAunitName <- paste(sa$speciesListId, sa$cruise, sa$trip, sa$station, sa$speciesCode, sa$landingCategory, sa$sizeSortingEU, sep = "-")

    sa$SAunitType <- "Box"

    # SAlowerHierarchy coded before

    sa$SAsampled <- "Y"
    sa$SAreasonNotSampled <- ""
    sa$SAreasonNotSampledFM <- "Unknown"
    sa$SAreasonNotSampledBV <- "Unknown"

    # For now only including Y - N requires manual coding
    # Should be 0, but that is not possible at the moment
    # sa$SAsampled[ft$cruise %in% c("MON", "SEAS") & is.na(ft$numberOfHaulsOrSets)] <- 0


    if (type == "only_mandatory") {
      sa_temp_optional <-
        filter(data_model, substr(name, 1, 2) == "SA" & min == 0)
      sa_temp_optional_t <-
        factor(t(sa_temp_optional$name)[1:nrow(sa_temp_optional)])

      for (i in levels(sa_temp_optional_t)) {
        eval(parse(text = paste0("sa$", i, " <- ''")))
      }
    }

    SA <-
      select(sa, one_of(sa_temp_t), speciesListId, sampleId, SAid, year)

    return(list(SA, sa_temp, sa_temp_t))
  }
