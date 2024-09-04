box::use(purrr)
box::use(readr)
box::use(dplyr)
box::use(terra)
box::use(stringr)
box::use(logger)
box::use(AzureStor)
box::use(glue)
box::use(logger)
box::use(./utils/blob)

# if dry_run will not upload cogs to azure
dry_run <- as.logical(Sys.getenv("DRY_RUN", unset = TRUE))
logger$log_info("Dry run: {dry_run}")

run_date <- Sys.Date()
run_date_chr <- format(run_date,"%Y%m%d")

# In case of run fail (i.e URL not updated in time). This function identifies
# all needed dates to update on subsequent run (including latest)
dates_needed <- blob$blob_date_gaps()

if(is.null(dates_needed)){
  logger$log_info("No Updates Available")
}

# SFED & MFED 90d zip file provided as 2 URLs
# therefore, we loop through the links one by one processing the required
# files respectively

if(!is.null(dates_needed)) {
  lr <- c("SFED","MFED") |>
    purrr::map(
      \(frac_type){

        # Download zip to temp file
        TMP_NAME <-  paste0("FloodScan_",frac_type,"_90d_",run_date_chr,".zip")
        TMP_PATH <- file.path(tempdir(), TMP_NAME)
        DL_VAR <- paste0("FLOODSCAN_",frac_type,"_URL")
        DL_URL <- Sys.getenv(DL_VAR)
        download.file(DL_URL,TMP_PATH, quiet=TRUE)

        # get tif metadata before unzipping so we can selectively unzip
        # only relevant files
        tif_meta <- dplyr$tibble(
          unzip(TMP_PATH,list=T)
        )|>
          dplyr$select(Name) |>
          dplyr$mutate(
            date_tif = blob$extract_date(Name)
          )

        df_tifs_needed <- tif_meta |>
          dplyr$filter(
            date_tif %in% dates_needed
          )

        # unzip the relevant files to tempdir
        unzip(
          TMP_PATH,
          exdir = td <- tempdir(),
          files = df_tifs_needed$Name
        )
        logger$log_info("COGS needed unzipped")

        # read all rasters in as SpatRaster objects
        tf <- file.path(
          td,
          df_tifs_needed$Name
        )

        ret <- purrr$map(
          tf,
          \(tmp_tf){
            r <- terra$rast(tmp_tf)
            terra$set.names(r,frac_type)
            r
          }
        )
        tifs_downloaded_to_tmp <- glue$glue_collapse(basename(df_tifs_needed$Name),"\n")
        logger$log_info("Downloaded to tmp:\n{tifs_downloaded_to_tmp}")
        ret
      }
    )
  # merge all SFED & MFED rasters (in lr) into  one single spatRaster object
  r <- terra$rast(purrr$flatten(lr))

  # the spatRaster `source` attribute contains the original file name
  # from that we can extract the date. Do that and create a unique list
  # of dates
  date_str_match <- unique(
    stringr$str_extract(
      basename(
        terra$sources(r)),"\\d{8}"
    )
  )
  # then we loop through the dates and extract the relevant `layers`/`bands`
  # from the large spatRaster object to get a list where each list item is
  # one spatRaster object for a single day with 2 bands `SFED` & `MFED`

  lr_merged <- purrr$map(
    date_str_match,
    \(tmp_date_str){

      src_basename <- basename(terra$sources(r))

      r_sfed_mfed <- r[[
        stringr$str_detect(
          string = src_basename,
          pattern = tmp_date_str

        )
      ]]
      r_sfed_mfed

    }
  )

  # if dry_run = TRUE, the data is not actually uploaded to the blob
  # if dry_run = FALSE, the data is uploaded to the blob
  if(!dry_run){
    logger$log_info("Uploading processed rasters to blob storage")
    td <- tempdir()

    purrr$map(
      lr_merged,
      \(r_tmp){
        blob_name_upload <- stringr$str_remove(
          string = basename(
            terra$sources(r_tmp)[1]
          ),
          pattern = "sfed_|mfed_")

        tf <- file.path(td, blob_name_upload)

        terra$writeRaster(r_tmp,
                          filename = tf,
                          filetype = "COG",
                          gdal = c("COMPRESS=DEFLATE",
                                   "SPARSE_OK=YES",
                                   "OVERVIEW_RESAMPLING=AVERAGE")
        )
        cog_container <-  blob$load_containers(containers = "global")$GLOBAL_CONT

        invisible(
          capture.output(
            AzureStor$upload_blob(
              container = cog_container,
              src = tf,
              dest = paste0("raster/cogs/",blob_name_upload)
            )
          )
        )
      }
    )
  }
}


