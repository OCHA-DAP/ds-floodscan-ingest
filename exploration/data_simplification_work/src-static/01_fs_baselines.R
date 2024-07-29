#' calculate day-of-year (doy) averages for "SFED" FloodScan
#' upload result as single tif with 365/366 bands (one for each day)

box::use(terra[...])
box::use(sf[...])
box::use(dplyr[...])
box::use(AzureStor[...])
box::use(stringr[...])
box::use(lubridate[...])
box::use(purrr[...])


box::use(../../../src/utils/blob)
box::use(paths = ../../../R/path_utils)


extract_date <-  function(x){
  as.Date(str_extract(x, "\\d{8}"),format = "%Y%m%d")
}




Sys.setenv(AZURE_SAS = Sys.getenv("DSCI_AZ_SAS_DEV"))
Sys.setenv(AZURE_STORAGE_ACCOUNT = Sys.getenv("DSCI_AZ_STORAGE_ACCOUNT"))

# averages will be calculated from historical data up to this date
END_DATE_BASELINE <- as.Date("2020-12-31")
SFED_THRESHOLD <- 0.01

fps <- paths$load_paths()
bc <- blob$load_containers()
gc <- bc$GLOBAL_CONT
pc <- bc$PROJECTS_CONT

df_urls <- list_blobs(
  container = gc,
  prefix= "raster/cogs/aer"
)


df_urls <- df_urls |>
  mutate(
    date= as.Date(str_extract(name, "\\d{8}"),format = "%Y%m%d"),
    doy = yday(date),
    urls = paste0("/vsiaz/global/",name)
  ) |>
  filter(
    str_detect(urls,"\\.tif$")
  )



# this took approx an hour.
# Given the speed to similar operations in xarray I'd think there is a faster
# way to do this. Regardless, trying to load all the urls with one
# rast() call seems to time out while this looping/mapping strategy works
# and is bearably effecient.

doys <- unique(df_urls$doy)
lr_doy_avg <- map(
  doys,
  \(doy_tmp){
    cat(doy_tmp,"\n")

    # subset raster catalogue df by DOY
    df_urls_filt <- df_urls |>
      filter(
        date<= END_DATE_BASELINE,
        doy == doy_tmp
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
  dest = "ds-floodscan-ingest/aer_area_300s_doy_mean_baseline_1998_2020.tif",
)


# Upload smoothed and thresholded -----------------------------------------



# so after playing w/ the data and threshold a bit what makes most sense is
# to apply the rolling average to the unthresholded doy data first and then
# apply theshold across all. If you apply a threshold fist to normal doy
# the rolling averages will all get much smaller than the doy and you won't be
# able to apply the theshold in an equivalent manner because the thresholding
# done in the doy step essentially gets magnified when you start rolling and
# stacking the bands. Results in a rolling average that doesn't really line up
# w/ doy as it should

r_doy_avg_sorted <- rast("/vsiaz/projects/ds-floodscan-ingest/aer_area_300s_doy_mean_baseline_1998_2020.tif")
# r_crop_viz <- crop(r_doy_avg_sorted, slice(gdf,1))
# plot(r_crop_viz[[1]])

r_smoothed <- roll(
  x= r_doy_avg_sorted,
  n = 30,
  fun = mean,
  type = "around",
  circular = TRUE,
  na.rm = TRUE
)

r_smoothed_20 <- roll(
  x= r_doy_avg_sorted,
  n = 20,
  fun = mean,
  type = "around",
  circular = TRUE,
  na.rm = TRUE
)

### Write 30d smoothed #####
tf <- tempfile(fileext= ".tif")
writeRaster(
  r_smoothed,
  filename = tf,
  filetype = "COG",
  gdal = c("COMPRESS=DEFLATE",
           "SPARSE_OK=YES",
           "OVERVIEW_RESAMPLING=AVERAGE"),
  overwrite = TRUE
)


upload_blob(
  container = pc,
  src = tf,
  dest = fps$FP_DOY_NO_THRESH_SMOOTHED30d
)

### Write 20d Smoothed ####
tf <- tempfile(fileext= ".tif")

writeRaster(
  r_smoothed_20,
  filename = tf,
  filetype = "COG",
  gdal = c("COMPRESS=DEFLATE",
           "SPARSE_OK=YES",
           "OVERVIEW_RESAMPLING=AVERAGE"),
  overwrite = TRUE
)

upload_blob(
  container = pc,
  src = tf,
  dest = fps$FP_DOY_NO_THRESH_SMOOTHED20d
)



## Threshold Smoothed 30d ####

r_smoothed[r_smoothed <= SFED_THRESHOLD]<-0

tf <- tempfile(fileext= ".tif")
writeRaster(
  r_smoothed,
  filename = tf,
  filetype = "COG",
  gdal = c("COMPRESS=DEFLATE",
           "SPARSE_OK=YES",
           "OVERVIEW_RESAMPLING=AVERAGE"),
  overwrite = TRUE
)


upload_blob(
  container = pc,
  src = tf,
  dest = fps$FP_DOY_SMOOTHED30D_THRESH_COG
  # dest = "ds-floodscan-ingest/aer_area_300s_doy_mean_thresh_gte0.01_30d_smoothed_baseline_1998_2020.tif",
)
## Threshold smoothed 20d ####

r_smoothed20 <- rast(vp(fps$FP_DOY_NO_THRESH_SMOOTHED20d))
r_smoothed20[r_smoothed20 <= SFED_THRESHOLD]<-0

tf <- tempfile(fileext= ".tif")
writeRaster(
  r_smoothed20,
  filename = tf,
  filetype = "COG",
  gdal = c("COMPRESS=DEFLATE",
           "SPARSE_OK=YES",
           "OVERVIEW_RESAMPLING=AVERAGE"),
  overwrite = TRUE
)


upload_blob(
  container = pc,
  src = tf,
  dest = fps$FP_DOY_SMOOTHED20D_THRESH_COG
)




# Upload Smooth and filtered ----------------------------------------------


# so after playing w/ the data and threshold a bit what makes most sense is
# to apply the rolling average to the unthresholded doy data first and then
# apply theshold across all. If you apply a threshold
r_doy_avg_sorted <- rast("/vsiaz/projects/ds-floodscan-ingest/aer_area_300s_doy_mean_baseline_1998_2020.tif")
r_doy_avg_sorted[r_doy_avg_sorted <= SFED_THRESHOLD]<-NA
r_smoothed <- roll(
  x= r_doy_avg_sorted,
  n = 30,
  fun = mean,
  type = "around",
  circular = TRUE,
  na.rm = TRUE
)
# r_doy_smoothed <- r_smoothed_nday

tf <- tempfile(fileext= ".tif")
writeRaster(
  r_smoothed,
  filename = tf,
  filetype = "COG",
  gdal = c("COMPRESS=DEFLATE",
           "SPARSE_OK=YES",
           "OVERVIEW_RESAMPLING=AVERAGE")
)


upload_blob(
  container = pc,
  src = tf,
  dest = "ds-floodscan-ingest/aer_area_300s_doy_mean_filtered_gte0.01_30d_smoothed_baseline_1998_2020.tif",
)



# Upload doy (non-smooth) and thresholded ---------------------------------
r_doy_avg_sorted <- rast("/vsiaz/projects/ds-floodscan-ingest/aer_area_300s_doy_mean_baseline_1998_2020.tif")


r_doy_avg_sorted[r_doy_avg_sorted <= SFED_THRESHOLD] <- 0

r_crop_viz <- crop(r_doy_avg_sorted, slice(gdf,1))
plot(r_crop_viz[[1]])

tf <- tempfile(fileext= ".tif")
writeRaster(
  r_doy_avg_sorted,
  filename = tf,
  filetype = "COG",
  gdal = c("COMPRESS=DEFLATE",
           "SPARSE_OK=YES",
           "OVERVIEW_RESAMPLING=AVERAGE"),
  overwrite = TRUE
)


upload_blob(
  container = pc,
  src = tf,
  dest = fps$FP_DOY_THRESH_COG
  # dest = "ds-floodscan-ingest/aer_area_300s_doy_mean_thresh_gte0.01_baseline_1998_2020.tif",
)



# Get recent history raster -----------------------------------------------

df_urls_recent <- df_urls |>
  arrange(
    desc(date)
  ) |>
  slice(1:365)

r_365d <- rast(df_urls_recent$urls)

# subset to SFED only
r_365d_sfed <- r_365d[[names(r_365d)=="SFED"]]

# rename lyr/band to date.
r_dates <- extract_date(basename(sources(r_365d_sfed)))
set.names(r_365d_sfed, r_dates)

r_365d_sfed[r_365d_sfed <= SFED_THRESHOLD] <- 0






tf <- tempfile(fileext= ".tif")
writeRaster(
  r_365d_sfed,
  filename = tf,
  filetype = "COG",
  gdal = c("COMPRESS=DEFLATE",
           "SPARSE_OK=YES",
           "OVERVIEW_RESAMPLING=AVERAGE"),
  overwrite = TRUE
)


upload_blob(
  container = pc,
  src = tf,
  dest = fps$FP_LAST365D_THRESH
)


# 90d Sample --------------------------------------------------------------
r_90d <- r_365d_sfed[[1:90]]
tf <- tempfile(fileext= ".tif")
writeRaster(
  r_90d,
  filename = tf,
  filetype = "COG",
  gdal = c("COMPRESS=DEFLATE",
           "SPARSE_OK=YES",
           "OVERVIEW_RESAMPLING=AVERAGE"),
  overwrite = TRUE
)

upload_blob(
  container = pc,
  src = tf,
  dest = fps$FP_LAST90D_THRESH
)

