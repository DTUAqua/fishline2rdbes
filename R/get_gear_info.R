#' Get gear information required in the RDBES
#'
#' @description
#'
#' @param variables
#' @param selection_device: TO DO
#'
#' @return
#' @export
#' @author Kirsten Birch Håkansson, DTU Aqua & people from the RCG metier group
#'
#' @import dplyr stringr data.table openxlsx purrr lubridate RODBC sqldf
#'
#' @examples
#'


get_gear_info <-
  function(df = samp,
           year = year,
           area = dfuArea,
           area_code_type = "fishLine",
           gear_type = gearType,
           target_spp = targetSpecies1,
           target_spp_code_type = "fishLine",
           mesh_size = meshSize) {

    # usethis::use_package("dplyr")
    library(stringr)
    library(data.table)
    library(openxlsx)
    library(purrr)
    library(lubridate)
    library(RODBC)
    library(sqldf)

    # Get refences from https://github.com/ices-eg/RCGs/tree/master/Metiers/Reference_lists ----
    ## Code taken from the same repo

    source(paste0("https://raw.githubusercontent.com/ices-eg/RCGs/master/Metiers/Scripts/Functions/loadSpeciesList.R"))

    url_area <- "https://github.com/ices-eg/RCGs/raw/master/Metiers/Reference_lists/AreaRegionLookup.csv"

    area_ref <- loadAreaList(url_area)

    # Add needed codes ----
    # The metier reference list requires a RCG region, gear and mesh size,
    #target assemblage, selection device and mesh size in the selection device

    ## RCG region ----

    if (area_code_type == "fishLine") {

      channel <- odbcConnect("FishLine")
      area <- sqlQuery(
        channel,
        paste("SELECT DFUArea, areaICES FROM L_DFUArea",
              sep = ""
        )
      )
      close(channel)

      df_1 <- left_join(df, area, by = c("dfuArea" = "DFUArea"))

      print(paste("Before area join: ", nrow(df)))
      print(paste("After area join: ", nrow(df_1)))

      df_1 <- rename(df_1, "area" = "areaICES")



    } else {

      print("get_gear_info only handles areas from fishLine (dfuArea) - feel free to implement area types :-)")
    }

    df_2 <- left_join(df_1, area_ref)

    print(paste("Before RCG region join: ", nrow(df_1)))
    print(paste("After RCG region join: ", nrow(df_2)))

    no_rcg_region <- subset(df_2, is.na(df_2$RCG))

    distinct(no_rcg_region, year, cruise, trip, station, dfuArea, RCG)

    ## Gear code







  }
