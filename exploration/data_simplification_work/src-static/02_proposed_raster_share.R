FILE_DATE_SIMULATE <- "2024-01-15"
NUMBER_DAYS <- 90

box::use(terra[...])
box::use(sf[...])
box::use(dplyr[...])
box::use(AzureStor[...])
box::use(stringr[...])
box::use(lubridate[...])
box::use(purrr[...])
box::use(../../../src/utils/blob)
box::use(paths = ../../../R/path_utils)
box::use(../../../R/utils[download_fieldmaps_sf])
box::use(glue[...])
box::use(logger)

Sys.setenv(AZURE_SAS = Sys.getenv("DSCI_AZ_SAS_DEV"))
Sys.setenv(AZURE_STORAGE_ACCOUNT = Sys.getenv("DSCI_AZ_STORAGE_ACCOUNT"))

bc <- blob$load_containers()
pc <- bc$PROJECTS_CONT

# to name zipped tif
zip_date_prefix <- format(as.Date(FILE_DATE_SIMULATE),"%Y%m%d")

# load paths for project
fps <- paths$load_paths(virtual_path = T)

# get AOI
gdf_aoi <- download_fieldmaps_sf(iso = "SOM",layer= "som_adm0")

# raster of SFED for last 365D
r_365 <- rast(
  fps$FP_LAST365D,
  win  = gdf_aoi$som_adm0
  )

# historical smoothed baseline (20d)
r_historical <- rast(
  fps$FP_DOY_NO_THRESH_SMOOTHED20d,
  win = gdf_aoi$som_adm0
  )




# Demo Raster -------------------------------------------------------------

# Create DEMO raster w/ 365 days rather than 90 for flexible exploratory work
r_365_sorted <- deepcopy(r_365)
r_365_sorted <- r_365_sorted[[as.character(sort(as.Date(names(r_365_sorted))))]]
doys_sorted_to_last_365 <- yday(as.Date(names(r_365_sorted)))
r_historical_copy <-  deepcopy(r_historical)
r_historical_sorted_to_365 <- r_historical_copy[[doys_sorted_to_last_365]]

system.time(
  r_anom_365 <- r_365_sorted - r_historical_sorted_to_365
)
FP_LAST365D_ANOM_DEMO

tf <- tempfile(fileext= ".tif")
writeRaster(
  r_anom_365,
  filename = tf,
  filetype = "COG",
  gdal = c("COMPRESS=DEFLATE",
           "SPARSE_OK=YES",
           "OVERVIEW_RESAMPLING=AVERAGE")
)


FP_LAST365D_ANOM_DEMO <- paths$load_paths(
  virtual_path = F,
  path_name = "FP_LAST365D_ANOM_DEMO"
  )

upload_blob(
  container = pc,
  src = tf,
  dest = FP_LAST365D_ANOM_DEMO,
)


# 90D - 3 band raster for zipping -----------------------------------------

# Function to merge rasters for each day
fs_spatraster <- function(
  nrt_raster,
  historical_raster,
  date,
  n_days
){
  end_date <- as.Date(date)
  start_date <- end_date - (n_days-1)
  date_seq <-  seq(start_date, end_date,by = "day")

  map(
    date_seq, \(date_tmp){
      doy_tmp <- yday(date_tmp)
      date_chr <-  as.character(date_tmp)
      cat(date_chr,"\n")

      nrt_subset <- nrt_raster[[date_chr]]
      historical_subset <- historical_raster[[doy_tmp]]
      set.names(nrt_subset,"SFED")
      set.names(historical_subset,"SFED_BASELINE")
      r <- rast(list(nrt_subset, historical_subset))
      logger$log_info("performing  anomaly calculation")
      r[["SFED_ANOM"]] <- r[["SFED"]] - r[["SFED_BASELINE"]]
      time(r) <- as.Date(rep(date_tmp,nlyr(r)))
      r
    }
  )
}

lr <- fs_spatraster(
  nrt_raster = r_365,
  historical_raster= r_historical,
  date = FILE_DATE_SIMULATE,
  n_days = NUMBER_DAYS
)

td <- file.path(tempdir(),"aer_area_300s_SFED_90d")

# for some reason w/ in map writeRaster can't write to a dir
# that is not already actually created.
if (!dir.exists(td)) {
  dir.create(td, recursive = TRUE)
}

system.time(
  lr |>
    walk(
      \(r_tmp){
        date <- unique(time(r_tmp))
        date_prefix = format(date, "%Y%m%d")
        cat(date_prefix,"\n")

        # tempfilename
        tfn <- glue("{date_prefix}_aer_area_300s_SFED_processed.tif")
        tfp <- file.path(td, tfn)

        logger$log_info("writing raster")

        writeRaster(
          r_tmp,
          filename = tfp,
          filetype = "COG",
          gdal = c("COMPRESS=DEFLATE",
                   "SPARSE_OK=YES",
                   "OVERVIEW_RESAMPLING=AVERAGE"),
          overwrite = TRUE
        )
      }
    )
)
# exerimenting w/ gdalcubes
zip_name <- glue("{zip_date_prefix}_aer_area_300s_SFED_90d.zip")
zip(
  zipfile = zip_name,
  files = td,
  extras = "-j"
)
