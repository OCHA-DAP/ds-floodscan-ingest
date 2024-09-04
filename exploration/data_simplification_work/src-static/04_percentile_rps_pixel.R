#' calculate day-of-year (doy) averages for "SFED" FloodScan
#' upload result as single tif with 365/366 bands (one for each day)

box::use(terra[...])
box::use(sf[...])
box::use(dplyr[...])
box::use(AzureStor[...])
box::use(stringr[...])
box::use(lubridate[...])
box::use(purrr[...])
box::use(logger)
box::use(glue)

box::use(../../../R/utils)
box::reload(utils)
box::use(../../../src/utils/blob)
box::use(paths = ../../../R/path_utils)

Sys.setenv(AZURE_SAS = Sys.getenv("DSCI_AZ_SAS_DEV"))
Sys.setenv(AZURE_STORAGE_ACCOUNT = Sys.getenv("DSCI_AZ_STORAGE_ACCOUNT"))

# averages will be calculated from historical data up to this date
END_DATE_BASELINE <- as.Date("2020-12-31")
SFED_THRESHOLD <- 0.01

fps <- paths$load_paths()
bc <- blob$load_containers()
gc <- bc$GLOBAL_CONT
pc <- bc$PROJECTS_CONT

df_urls <-  utils$floodscan_cog_meta_df(
  container = gc,
  prefix= "raster/cogs/aer"
  )


# haven't had any luck reading them all in at once so let's loop through
# years

lr_sfed_yearly <- split(df_urls ,df_urls$year) |>
  map(
    \(df_urls_year_tmp){
      yr_tmp <- unique(df_urls_year_tmp$year)
      logger$log_info(yr_tmp)
      r_tmp <- rast(
        df_urls_year_tmp$urls
      )
      r_sfed <- r_tmp[[names(r_tmp)=="SFED"]]
      set.names(r_sfed ,as.character(df_urls_year_tmp$date))
      r_sfed
    }
  )


# try writing local.
lr_sfed_yearly |>
  imap(
    \(r_tmp,yr_tmp){
      logger$log_info(yr_tmp)
      writeRaster(
        r_tmp,
        filename = glue$glue("cogs/aer_sfed_{yr_tmp}.tif"),
        filetype = "COG",
        gdal = c("COMPRESS=DEFLATE",
                 "SPARSE_OK=YES",
                 "OVERVIEW_RESAMPLING=AVERAGE")
      )

    }
  )

# # caculate 1 yearly max composite per year
# lr_sfed_yearly_max <- lr_sfed_yearly |>
#   imap(
#     \(r_tmp,yr_tmp){
#       logger$log_info(yr_tmp)
#       r_max <- max(r_tmp,na.rm=T)
#       set.names(r_max,yr_tmp)
#       return(r_max)
#     }
#   )
#
#
