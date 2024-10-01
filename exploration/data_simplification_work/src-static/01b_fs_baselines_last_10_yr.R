#' calculate day-of-year (doy) averages for "SFED" FloodScan for last 10 years
#' upload result as single tif with 365/366 bands (one for each day)

box::use(terra[...])
box::use(sf[...])
box::use(dplyr[...])
box::use(AzureStor[...])
box::use(stringr[...])
box::use(lubridate[...])
box::use(purrr[...])
box::use(glue)

box::use(../../../R/utils)
box::use(../../../src/utils/blob)
box::use(paths = ../../../R/path_utils)

Sys.setenv(AZURE_SAS = Sys.getenv("DSCI_AZ_SAS_DEV"))
Sys.setenv(AZURE_STORAGE_ACCOUNT = Sys.getenv("DSCI_AZ_STORAGE_ACCOUNT"))

BASELINE_NUMBER_YEARS <-  10
CURRENT_YEAR <-  year(Sys.Date())

BASELINE_YEARS <-  (CURRENT_YEAR - BASELINE_NUMBER_YEARS) : (CURRENT_YEAR - 1)

fps <- paths$load_paths()
bc <- blob$load_containers()
gc <- bc$GLOBAL_CONT
pc <- bc$PROJECTS_CONT

df_urls <- utils$floodscan_cog_meta_df(container = gc,prefix= "raster/cogs/aer")

df_urls_baseline <- df_urls |>
  filter(
    year(date) %in% BASELINE_YEARS
  )



# this took approx an hour.
# Given the speed to similar operations in xarray I'd think there is a faster
# way to do this. Regardless, trying to load all the urls with one
# rast() call seems to time out while this looping/mapping strategy works
# and is bearably effecient.

doys <- unique(df_urls_baseline$doy)
lr_doy_avg <- map(
  doys,
  \(doy_tmp){
    cat(doy_tmp,"\n")

    # subset raster catalogue df by DOY
    df_urls_filt <- df_urls_baseline |>
      filter(
        doy %in% doy_tmp
      )
    r <- rast(df_urls_filt$urls)

    r_sfed <- r[[names(r)=="SFED"]]
    r_mean <- mean(r_sfed, na.rm=T)

    set.names(r_mean,as.character(doy_tmp)) # name band based on doy
    r_mean
  }
)

# merge list of rasters into 1 spatRaster
r_doy_avg <- rast(lr_doy_avg)
r_doy_avg_sorted <- r_doy_avg[[as.character(1:366)]]


# Upload DOY unsmoothed ---------------------------------------------------


tf <- tempfile(fileext= ".tif")
writeRaster(
  r_doy_avg_sorted,
  filename = tf,
  filetype = "COG",
  gdal = c("COMPRESS=DEFLATE",
           "SPARSE_OK=YES",
           "OVERVIEW_RESAMPLING=AVERAGE")
)


upload_blob(
  container = pc,
  src = tf,
  dest = fps$FP_DOY_LAST_2014_2023,
)
