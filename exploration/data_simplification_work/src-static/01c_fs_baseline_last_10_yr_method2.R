
SMOOTHING_DAYS <- 10
WINDOW_SIZE <- ifelse (SMOOTHING_DAYS %% 2==0,SMOOTHING_DAYS+1,SMOOTHING_DAYS)
BATCH_SIZE <- 365

BASELINE_LENGTH <-  10 # years
CURRENT_YEAR <-  2024
BASELINE_END <- CURRENT_YEAR -1
BASELINE_START <- BASELINE_END - (BASELINE_LENGTH-1)

#' method where we rollmean first
box::use(
  terra,
  dplyr,
  purrr,
  logger,
  lubridate,
  glue,
  sf
)
box::use(../../../R/utils) # func to get fieldmaps/
box::use(../../../src/utils/blob)
box::use(paths =../../../R/path_utils)
box::use(cogger =../../../R/fs_cogs)
box::reload(cogger)


cogger$cog_cloud_config()


lgdf <- utils$download_fieldmaps_sf("SOM", layer = c("som_adm0"))
gdf_bbox <- sf$st_bbox(lgdf$som_adm0) |>
  sf$st_as_sfc() |>
  sf$st_as_sf()

logger$log_info("starting")



Sys.setenv(AZURE_SAS = Sys.getenv("DSCI_AZ_SAS_DEV"))
Sys.setenv(AZURE_STORAGE_ACCOUNT = Sys.getenv("DSCI_AZ_STORAGE_ACCOUNT"))


bc <- blob$load_containers()
gc <- bc$GLOBAL_CONT
pc <- bc$PROJECTS_CONT


fs_meta <- utils$floodscan_cog_meta_df(container = gc)


baseline_start_date <- lubridate$as_date(paste0(BASELINE_START,"-01-01"))
baseline_end_date <- lubridate$as_date(paste0(BASELINE_END,"-12-31"))


# this pads w/ extra
start_padded <- baseline_start_date - lubridate$days(SMOOTHING_DAYS)
end_padded <- baseline_end_date + lubridate$days(SMOOTHING_DAYS)


DATE_SEQ <-  seq(start_padded, end_padded, by ="day")

df_urls <- fs_meta |>
  dplyr$filter(
    date %in% DATE_SEQ
  )

ldf_urls <- split(df_urls,df_urls$year) #|>
  # purrr$map(
  #   ~.x |> dplyr$slice(1:20)
  # )

# ldf_urls <- ldf_urls[1:2]
logger$log_info("loading rasters from blob")
lr <- ldf_urls |>
  purrr$map(
    \(df_url){
    yr <-  unique(df_url$year)
    cat(yr,"\n")
    r <- terra$rast(df_url$urls,win  = terra$ext(gdf_bbox))
    r_sfed <- r[[names(r)=="SFED"]]
    return(r_sfed)
    }
  )
# install.packages("doParallel")
# library(doParallel)
# library(parallel)
# # makeCluster(2L)
# # box::use(foreach[...])
# # box::use(doParallel[...])
# # foreach:::
# cls <- makeCluster(6L)
# registerDoParallel(cls)
# clust_list_t <- foreach(i = 1:10,
#                         .packages = c("terra"),
#                         .export = c("f"),
#                         .inorder = TRUE) %dopar% {
#                           p <- rast(f)
#                           sidx <- sample(1:nlyr(p), size = 10,
#                                          replace = FALSE)
#                           b <- p[[sidx]]
#                           b <- global(b, mean, na.rm = TRUE)
#                           return(b)
#                         }
# stopCluster(cls)
# clust_list_t


logger$log_info("rasters loaded from blob")

r_sfed <- terra$rast(lr)

logger$log_info("Subsetting")

terra$set.names(r_sfed,utils$extract_date(terra$sources(r_sfed)))


num_layers <- terra$nlyr(r_sfed)
batch_size <- BATCH_SIZE  # Total layers to process at a time
n <- WINDOW_SIZE  # Window size for rolling mean
overlap <- floor(n / 2)  # Calculate overlap based on n

# Preallocate a list to store results
results <- list()

# Initialize variables
idx_start <- 1
iteration <- 1

logger$log_info("Starting loop")
# Loop through layers using a while loop
while (idx_start+n <= num_layers) {
  # Define end index based on the batch size
  end <- min(idx_start + batch_size - 1, num_layers)
  cat(idx_start:end,"\n")

  # Extract the current batch of layers
  r_batch <- r_sfed[[idx_start:end]]

  # Apply rolling mean
  smoothed_batch <- terra::roll(
    x = r_batch,
    n = n,
    fun = mean,
    type = "around"
  )

  # Store the smoothed batch, excluding the edges
  smoothed_batch_full <- smoothed_batch[[(overlap + 1):(terra$nlyr(smoothed_batch) - overlap)]]

  # Store results
  results[[iteration]] <- smoothed_batch_full

  # Update index for the next batch
  max_date_iter <- max(as.Date(names(smoothed_batch_full)))
  start_date_next <- max_date_iter - lubridate$days(overlap)
  idx_start <- which(as.Date(names(r_sfed)) == start_date_next)

  # Increment iteration counter
  iteration <- iteration + 1
}

r_sfed_smoothed <- terra$rast(results)
num_layers_merged <-  terra$nlyr(r_sfed_smoothed)

logger$log_info(glue$glue("Number Layers merged: {num_layers_merged}"))

# smoothed_batch_valid <- terra::roll(
#   x = r_sfed,
#   n = n,
#   fun = mean,
#   type = "around"
# )

# names(r_sfed)
# r_sfed_smoothed<- rast(results)
nm_dt <- as.Date(names(r_sfed_smoothed))
df_gaps <- dplyr$tibble(
  nm_dt
) |>
  # dplyr$slice(-c(3)) |>
  dplyr$mutate(
    diff = as.numeric(nm_dt - lag(nm_dt))
  ) |>
  dplyr$filter(diff>1)
prob_dates <- glue$glue_collapse(df_gaps$nm_dt,", ")
logger$log_info(glue$glue("Date Gaps: {nrow(df_gaps)}: {prob_dates}"))

#
# nm_dt-lag(nm_dt)
# logger$log_info("Rolling")
# r_sfed_smoothed <- terra$roll(
#   x = r_sfed,
#   n = SMOOTHING_DAYS+1,
#   fun = mean,
#   type = "around"
#   )

# chop off padding to calculate DOY means
r_sfed_smoothed_filtered <- r_sfed_smoothed[[lubridate$year(names(r_sfed_smoothed)) %in% (BASELINE_START:BASELINE_END) ]]
doys <- unique(lubridate$yday(names(r_sfed_smoothed_filtered)))
logger$log_info("Averaging DOYS")

lr_doys <- doys |>
  purrr$map(
    \(doy){
      cat(doy,"\n")
      r_subset_doy <- r_sfed_smoothed_filtered[[lubridate$yday(names(r_sfed_smoothed_filtered))==doy ]]
      r_mean <-  terra$mean(r_subset_doy,na.rm=T)
      terra$set.names(r_mean,doy)
      terra$writeRaster(
        r_mean,
        filename = file.path(
          "data",
          "doy_rasters",
          glue$glue("{SMOOTHING_DAYS}d"),
          "last10y",
          glue$glue("{doy}.tif")
        ),
        filetype = "COG",
        gdal = c("COMPRESS=DEFLATE",
                 "SPARSE_OK=YES",
                 "OVERVIEW_RESAMPLING=AVERAGE"),
        overwrite =TRUE
      )
    }
  )

# r_doys <- terra$rast(lr_doys)
#
# logger$log_info("Writing Final")
# terra$writeRaster(
#   r_doys,
#   filename = "r_baseline_doys_last10_yrs_method2.tif",
#   filetype = "COG",
#   gdal = c("COMPRESS=DEFLATE",
#            "SPARSE_OK=YES",
#            "OVERVIEW_RESAMPLING=AVERAGE"),
#   overwrite =TRUE
# )
logger$log_info("Finished")
