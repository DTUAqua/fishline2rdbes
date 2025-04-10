#' @param data
#'
#' @author Kirsten Birch Håkansson, DTU Aqua
#'
#' @return
#' @export
#'
#'
#' @examples
SA_fishline_2_rdbes <-
  function(data = SA_data) {
    # Input for testing ----

    # ref_path <- "C:/Users/kibi/OneDrive - Danmarks Tekniske Universitet/gits/create_RDBES_data/references/link_fishLine_sampling_designs_2023.csv"
    # years <- c(2023)
    # sampling_scheme <- "DNK_Industrial_Sampling"
    # data_model_path <-
    #   "Q:/dfad/data/Data/RDBES/sample_data/create_RDBES_data/input"

    # Set-up ----


    # library(sqldf)
    library(tidyr)
    library(plyr, include.only = c("rbind.fill"))
    library(dplyr)
    library(stringr)
    library(haven)
    library(lubridate)

    SA <- get_data_model("Sample")
    samp <- data$samp
    area <- data$area
    art <- data$art

    # link <- link
    # link <- subset(link, DEsamplingScheme %in% sampling_scheme)
    #
    # trips <- unique(link$tripId[!is.na(link$tripId)])

    # Get needed stuff ----

    # Get data from FishLine

    # channel <- odbcConnect("FishLineDW")
    # samp <- sqlQuery(
    #   channel,
    #   paste(
    #     "SELECT specieslist.*, Animal.animalId, Animal.individNum,
    #               Animal.representative, Animal.number as ani_number
    #               FROM FishLineDW.dbo.SpeciesList INNER JOIN
    #               FishLineDW.dbo.Sample ON SpeciesList.sampleId = Sample.sampleId LEFT OUTER JOIN
    #               FishLineDW.dbo.Animal ON SpeciesList.speciesListId = Animal.speciesListId
    #      WHERE (Specieslist.year between ",
    #     min(years),
    #     " and ",
    #     max(years),
    #     ")
    #             and Sample.tripId in (",
    #     paste(trips, collapse = ","),
    #     ")",
    #     sep = ""
    #   )
    # )
    # close(channel)
    #
    # samp$dateGearStart <- force_tz(samp$dateGearStart, tzone = "UTC")
    # samp <- subset(samp, !(is.na(dfuArea)))
    #
    # channel <- odbcConnect("FishLine")
    # area <- sqlQuery(
    #   channel,
    #   paste("SELECT DFUArea, areaICES FROM FishLine.dbo.L_DFUArea",
    #     sep = ""
    #   )
    # )
    # art <-
    #   sqlQuery(
    #     channel,
    #     paste(
    #       "select speciesCode, latin, speciesFAO, aphiaID FROM FishLine.dbo.L_species"
    #     )
    #   )
    # close(channel)

    # Add needed stuff ----

    samp$dateGearStart <- force_tz(samp$dateGearStart, tzone = "UTC")
    samp <- subset(samp, !(is.na(dfuArea)))

    error_no_area <- subset(samp, (is.na(dfuArea)))

    samp$dfuArea <- as.character(samp$dfuArea)

    sa <- left_join(samp, area, by = c("dfuArea" = "DFUArea"))
    sa <- left_join(sa, art)

    check_no_latin <- distinct(filter(sa, is.na(latin) | is.na(aphiaID)), speciesCode)

    # Delete all none species
    sa <- filter(sa, !(is.na(latin)) &
               !(speciesCode %in% c("INV")) & !is.na(aphiaID))

    # Identify lower hierachy

    # Fix lh for sandeel
    ## Saneel are read in group of ~3 - with the same animalId.
    ## Each fish need their own row in BV

    tbm_ej_rep <- subset(sa, speciesCode == "TBM" & representative == "ja"
                         & !is.na(age))

    tbm_ej_rep$individNum <- 100000 + as.integer(row.names(tbm_ej_rep))
    tbm_ej_rep$representative  <- "nej"

    tbm_ej_rep$ani_number <- tbm_ej_rep$age_number

    sa <- rbind(sa, tbm_ej_rep)

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
        landingCategory,
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
        sizeSortingEU,
        landingCategory
      )

    test <- subset(sa, is.na(animalId))

    lh_1 <-
      dplyr::summarise(group_by(subset(sa,!is.na(individNum)), speciesListId, representative),
                no_indi = length(unique(individNum)))


    lh_num_sum <-
      dplyr::summarise(group_by(subset(sa, representative == "ja"), speciesListId),
                no_fish = sum(ani_number, na.rm = T))

    lh_1_num <- full_join(lh_1, lh_num_sum)

    lh_2 <- left_join(lh_uniq, lh_1_num)

    lh_2_t <- spread(lh_2, key = representative, value = no_indi)


    lh_2_t$SAlowerHierarchy[is.na(lh_2_t$no_fish)] <- "D"
    lh_2_t$SAlowerHierarchy[is.na(lh_2_t$ja) & !(is.na(lh_2_t$nej)) & lh_2_t$no_fish > 0] <- "A"
    lh_2_t$SAlowerHierarchy[!is.na(lh_2_t$ja) & (is.na(lh_2_t$nej)) & lh_2_t$no_fish > 0] <- "C"
    lh_2_t$SAlowerHierarchy[is.na(lh_2_t$ja) & (is.na(lh_2_t$nej)) & lh_2_t$no_fish > 0] <- "B"

    sa_1 <- left_join(sa, lh_2_t)

    table(sa_1$SAlowerHierarchy)

    sa_2 <- subset(sa_1, !is.na(raisingFactor))

    error <- subset(sa_1, raisingFactor < 1)

    # sa_2 <- subset(sa_1, representative == "ja")

    # Recode for SA ----

    sa <- sa_1

    sa$SAid <- sa$speciesListId
    sa$SArecordType <- "SA"

    sa <- arrange(sa, sampleId, speciesListId)

    sa$SAstratification <- "Y"

    sa <-
      mutate(
        sa,
        SAstratumName = paste(
          speciesFAO,
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
        treatment %in% c("UR", "KH"), # KH is UR, but cooked
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
              ifelse(treatment == "HA", "TLD",
                     ifelse(treatment == "KL", "CLA", NA
                            ))
            )
          )
        )
      ))

    test <- filter(sa, is.na(SApresentation))

    sa$SAspecimensState <- "NotDetermined"

    sa$SAstateOfProcessing[sa$treatment != "KN"] <- "UNK"
    sa$SAstateOfProcessing[sa$treatment == "KN"] <- "BOI"

    sa <- mutate(sa, SAcatchCategory = ifelse(landingCategory == "BMS", "BMS",
                                              ifelse(landingCategory %in% c("DIS", "SÆL"), "Dis",
                                                     ifelse(landingCategory %in% c("KON", "IND"), "Lan", NA))))

    sa$SAlandingCategory[sa$landingCategory == "KON"] <- "HuC"
    sa$SAlandingCategory[sa$landingCategory %in% c("IND")] <- "Ind"
    sa$SAlandingCategory[sa$landingCategory %in% c("BMS")] <- "None"
    sa$SAlandingCategory[sa$landingCategory %in% c("DIS", "SÆL")] <- "None"
    sa$SAcommSizeCatScale <- "EU"
    sa$SAcommSizeCat <- sa$sizeSortingEU
    sa$SAcommSizeCat[sa$SAcommSizeCat == "123"] <- "3"
    sa <- mutate(sa, SAsex = ifelse(is.na(sexCode), "U", as.character(sexCode)))

    sa$SAarea <- sa$areaICES
    sa$SArectangle <- sa$statisticalRectangle
    sa$SArectangle[is.na(sa$SArectangle)] <- ""
    sa$SAgsaSubarea <- "NotApplicable"

    sa$SAmetier6 <- "MIS_MIS_0_0_0"
    sa$SAgear <- sa$gearType
    sa$SAgear[is.na(sa$SAgear)] <- "MIS"
    sa$SAgear[sa$gearType == "LL"] <- "LLS"
    sa$SAgear[sa$gearType == "TBN"] <- "OTB"
    sa$SAgear[sa$gearType == "FIX"] <- "FPO"
    sa$SAgear[sa$gearType == "LHP"] <- "LHM"
    sa$SAmeshSize <- sa$meshSize

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

    sa$SAselectionMethod <- "NPQSRSWOR"

    sa$SAunitName <- paste(sa$speciesListId, sa$cruise, sa$trip, sa$station, sa$speciesCode, sa$landingCategory, sa$sizeSortingEU, sep = "-")

    sa$SAunitType <- "Box"

    # SAlowerHierarchy coded before

    sa$SAsampled <- "Y"
    sa$SAreasonNotSampledFM <- "Unknown"
    sa$SAreasonNotSampledBV <- "Unknown"

    sa$SAsex[sa$SAsex == FALSE] <- "F"
    sa <- plyr::rbind.fill(SA, sa)
    sa <- sa[ , c(names(SA), "speciesListId", "sampleId", "SAid", "year")]

    sa <- unique(sa) #remove pr fish information and stick to by sample

    return(list(sa, SA))
  }
