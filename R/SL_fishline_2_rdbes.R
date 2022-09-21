
#' FishLine 2 RDBES, species list (SL)
#'
#' @description Converts samples data from national database (fishLine) to RDBES. Data model v. 1.18. This function ...
#'
#' @param data_model Where to find the .xlsx with the data model
#' @param year Only takes a single year for now
#' @param cruises Name of cruises in national database
#' @param type only_mandatory | everything
#'
#' @return
#' @export
#'
#'
#' @examples
#'
#'
#'

SL_fishline_2_rdbes <-
  function(data_model_baseTypes_path = "Q:/mynd/RDB/create_RDBES_data/references",
           years = 2016,
           basis_years = c(2016:2020),
           cruises = c("MON", "SEAS"),
           catch_fractions = c("Dis", "Lan"),
           specieslist_name = "DNK_AtSea_Observer_all_species_same_Dis_Lan",
           species_to_add = c(148776, 137117),
           type = "only_mandatory")
  {
    # Input for testing ----

    # data_model_baseTypes_path <-
    #   "Q:/mynd/kibi/RDBES/create_RDBES_data/references"
    # years <- c(2018:2020)
    # basis_years <-  c(2016:2020)
    # cruises <- c("MON",  "SEAS")
    # catch_fractions <- c("Dis", "Lan")
    # specieslist_name <- "DNK_AtSea_Observer_all_species_Dis_Lan"
    # species_to_add <- c(148776, 137117)
    # type <- "everything"


    library(RODBC)
    library(sqldf)
    library(dplyr)
    library(stringr)
    library(haven)

    data_model <-
      readRDS(paste0(data_model_baseTypes_path, "/BaseTypes.rds"))

    sl_temp <- filter(data_model, substr(name, 1, 2) == "SL")
    sl_temp_t <- c("SLrecordType", t(sl_temp$name)[1:nrow(sl_temp)])

    # Get data from FishLine
    channel <- odbcConnect("FishLineDW")
    sl <- sqlQuery(
      channel,
      paste(
        "select speciesCode FROM dbo.SpeciesList
                  WHERE (year between ",
        min(basis_years) ,
        " and ",
        max(basis_years),
        ")
                                 and cruise in ('",
        paste(cruises, collapse = "','"),
        "')",
        sep = ""
      )
    )
    close(channel)

    channel2 <- odbcConnect("FishLine")
    art <-
      sqlQuery(channel2,
               paste("select speciesCode, aphiaID, dkName, latin FROM dbo.L_species"))
    close(channel2)

    #Selecting species per catchcategory and region

    sl <- left_join(sl, art)

    no_latin <-
      distinct(filter(sl, is.na(latin)), speciesCode, dkName)

    #Delete all none species
    sl <- filter(sl, !(is.na(latin)) & !(speciesCode %in% c("INV")) & !is.na(aphiaID))

    # Add species
    if (length(species_to_add) > 0) {
      add_species <- data.frame(aphiaID = species_to_add)

      sl <- bind_rows(sl, add_species)
    }

    # Code SL table

    sl$SLrecordType <- "SL"
    sl$SLcountry = "DK"
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
      data.frame(sl[rep(seq_len(nrow(sl)), each = 3),], SLyear)

    id <- distinct(ungroup(sl), SLspeciesListName)
    sl$SLid <- row.names(id)

    if (type == "only_mandatory") {
      sl_temp_optional <-
        filter(data_model, substr(name, 1, 2) == "SL" & min == 0)
      sl_temp_optional_t <-
        factor(t(sl_temp_optional$name)[1:nrow(sl_temp_optional)])

      for (i in levels(sl_temp_optional_t)) {
        eval(parse(text = paste0("sl$", i, " <- NA")))

      }
    }

    SL <- distinct(select(ungroup(sl), one_of(sl_temp_t), SLid))

    return(list(SL, sl_temp, sl_temp_t))

  }
