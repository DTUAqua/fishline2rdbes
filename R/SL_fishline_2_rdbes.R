
#' FishLine 2 RDBES, species list (SL)
#'
#' @description Converts samples data from national database (fishLine) to RDBES. Data model v. 1.18. This function ...
#'
#' @param data_model_path Where to find the .xlsx with the data model
#' @param basis_years How many years should the species list be based on
#' @param cruises Which cruises are relevant
#' @param catch_fractions Catch fractions in the species list
#' @param specieslist_name Output name of the species list
#' @param species_to_add additinal AphaiaIds not included in the observations
#'
#' @return
#' @export
#'
#'
#' @examples
SL_fishline_2_rdbes <-
  function(data_model_path,
           basis_years = c(2016:2020),
           cruises = c("MON", "SEAS"),
           catch_fractions = c("Dis", "Lan"),
           specieslist_name = "DNK_AtSea_Observer_all_species_same_Dis_Lan",
           species_to_add = c(148776, 137117)) {

    # Input for testing ----

    # ref_path <- "Q:/dfad/data/Data/RDBES/sample_data/create_RDBES_data/references/link_fishLine_sampling_designs_2022.csv"
    # encryptedVesselCode_path <-
    #   "Q:/dfad/data/Data/RDBES/sample_data/create_RDBES_data/output/for_production"
    # years <- 2022
    # sampling_scheme <- "DNK_AtSea_Observer_Active"
    # data_model_path <-
    #   "Q:/dfad/data/Data/RDBES/sample_data/create_RDBES_data/input"
    # basis_years <-  c(2016:2020)
    # cruises <- c("MON", "SEAS")
    # catch_fractions <- c("Dis", "Lan")
    # specieslist_name <- "DNK_AtSea_Observer_all_species_Dis_Lan"
    # species_to_add <- c(148776, 137117)
    # type <- "everything"


    library(RODBC)
    library(sqldf)
    library(dplyr)
    library(stringr)
    library(haven)

    SL <-
      get_data_model_vd_sl("Species List Details", data_model_path = data_model_path)

    # Get link ----
    link <- read.csv(ref_path)

    link <- subset(link, DEsamplingScheme == sampling_scheme)

    trips <- unique(link$tripId[!is.na(link$tripId)])

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    sl <- sqlQuery(
      channel,
     paste(
        "select Sample.cruise, speciesCode FROM SpeciesList INNER JOIN
                  Sample ON SpeciesList.sampleId = Sample.sampleId
                  WHERE (Sample.year between ",
        min(basis_years),
        " and ",
        max(basis_years),
        ")
                and Sample.cruise in (",
        paste(toString(sprintf("'%s'", cruises)), collapse = ","),
        ")",
        sep = ""
      )
    )
    close(channel)

    channel2 <- odbcConnect("FishLine")
    art <-
      sqlQuery(
        channel2,
        paste(
          "select speciesCode, aphiaID, dkName, latin FROM dbo.L_species"
        )
      )
    close(channel2)

    # Selecting species per catchcategory and region

    sl <- left_join(sl, art)

    no_latin <-
      distinct(filter(sl, is.na(latin)), speciesCode, dkName)

    # Rename the following species to iNV from the observer program, since we normally include them in INV
    # This means that the species will be uploaded, but not be a part of the specieslist and therefore excluded before estimation

    # Blåmusling	Mytilus edulis		BMS
    # Almindelig hjertemusling	Cerastoderma edule		HMS
    # Kammusling	Aequipecten opercularis		KAM
    # Molboøsters	Arctica islandica		MOE
    # Stor kammusling	Pecten maximus		PEM
    # Vandmand	Aurelia aurita		VAN
    # *Slangestjerner	Ophiura		SLS
    # Almindelig søstjerne	Asterias rubens		SST
    # Konk	Buccinum undatum		AKO
    # Stor flæsketerning	Philine aperta		SFL
    # *Krabber			KRX
    # Stankelbenskrabbe	Macropodia rostrata		SBK
    # Strandkrabbe	Carcinus maenas		STK
    # Svømmekrabbe	Liocarcinus depurator		SVK


    sl$speciesCode[sl$cruise %in% c("MON", "SEAS") &
                     sl$speciesCode %in% c(
                       "BMS",
                       "HMS",
                       "KAM",
                       "MOE",
                       "PEM",
                       "VAN",
                       "SLS",
                       "SST",
                       "AKO",
                       "SFL",
                       "KRX",
                       "SBK",
                       "STK",
                       "SVK"
                     )] <- "INV"

    # Delete all none species
    sl <-
      filter(sl, !(is.na(latin)) &
        !(speciesCode %in% c("INV")) & !is.na(aphiaID))


    # Add species
    if (length(species_to_add) > 0) {
      add_species <- data.frame(aphiaID = species_to_add)

      sl <- bind_rows(sl, add_species)
    }

    # Code SL table

    sl$SLrecordType <- "SL"
    sl$SLcountry <- "DK"
    sl$SLinstitute <- "2195"
    sl$SLspeciesListName <- specieslist_name
    sl$SLcommercialTaxon <- sl$aphiaID
    sl$SLspeciesCode <- sl$aphiaID



    sl$SLcatchFraction <- ""

    if (length(catch_fractions) == 1) {
      sl$SLcatchFraction <- catch_fractions
    } else if (length(catch_fractions) == 2) {
      sl_1 <- mutate(sl, SLcatchFraction = catch_fractions[1])
      sl_2 <- mutate(sl, SLcatchFraction = catch_fractions[2])
      sl <- rbind(sl_1, sl_2)
    } else {
      print("Too many catch_fractions")
    }

    SLyear <- rep(years, nrow(sl))

    sl <-
      data.frame(sl[rep(seq_len(nrow(sl)), each = length(unique(SLyear))), ], SLyear)

    id <- distinct(ungroup(sl), SLspeciesListName)
    sl$SLid <- row.names(id)

    # sl <- plyr::rbind.fill(SL, sl)
    sl <- distinct(select(ungroup(sl), one_of(names(SL)), SLid))

    sl[is.na(sl) ] <- ""


    return(list(sl, SL))
  }
