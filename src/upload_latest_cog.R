box::use(purrr)
box::use(readr)
box::use(dplyr)
box::use(terra)
box::use(stringr)
box::use(logger)
box::use(AzureStor)
box::use(glue)

box::use(./utils/blob)

# if dry_run will not upload cogs to azure
dry_run <- as.logical(Sys.getenv("DRY_RUN", unset = TRUE))

run_date <- Sys.Date()
run_date_chr <- format(run_date,"%Y%m%d")


lr <- c("SFED","MFED") |>
  purrr::map(
    \(frac_type){
      # frac_type <- "SFED"

      TMP_NAME <-  paste0("FloodScan_",frac_type,"_90d_",run_date_chr,".zip")
      TMP_PATH <- file.path(tempdir(), TMP_NAME)
      DL_VAR <- paste0("FLOODSCAN_",frac_type,"_URL")
      DL_URL <- Sys.getenv(DL_VAR)

      logger$log_info("Downloading {frac_type}")
      download.file(DL_URL,TMP_PATH, quiet=TRUE)
      logger$log_info("Download {frac_type} to memory - succesful!")


      tif_meta <- dplyr$tibble(
        unzip(TMP_PATH,list=T)
      )|>
        dplyr$select(Name) |>
        dplyr$mutate(
          date_tif = blob$extract_date(Name)
        )
      logger$log_info("Created zip contents meta table")

      dates_needed <- blob$blob_date_gaps()

      logger$log_info("Found dates missing on blob")


      df_tifs_needed <- tif_meta |>
        dplyr$filter(
          date_tif %in% dates_needed
        )

      if(nrow(df_tifs_needed)==0){
        logger$log_info("No {frac_type} tif updates available")
        ret <- NULL
      }

      if(nrow(df_tifs_needed)>0){

      unzip(
        TMP_PATH,
        exdir = td <- tempdir(),
        files = df_tifs_needed$Name
      )
        logger$log_info("COGS needed unzipped")


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

      return(ret)

    }
  )


logger$log_info("Processing/merging SFED & MFED rasters")
# will merge them all and then subset based on date.


lr_non_null <- purrr$keep(lr,\(x) !is.null(x))

if(length(lr_non_null)>0){
  r <- terra$rast(purrr$flatten(lr))

# loop through each unique date string that has SFED & MFED
# and merge them
date_str_match <- unique(
  stringr$str_extract(
    basename(
      terra$sources(r)),"\\d{8}"
  )
)

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

# loop through each raster and write COG to blob
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

if(length(lr_non_null)==0){
  logger$log_info("no updates made")
}
