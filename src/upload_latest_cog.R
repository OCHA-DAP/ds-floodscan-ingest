box::use(purrr)
box::use(readr)
box::use(dplyr)
box::use(terra)
box::use(stringr)
box::use(logger)
box::use(utils)
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

      cat("Downloading " , frac_type,"\n")
      download.file(DL_URL,TMP_PATH, quiet=TRUE)


      tif_meta <- dplyr$tibble(
        unzip(TMP_PATH,list=T)
      )|>
        dplyr$select(Name) |>
        dplyr$mutate(
          date_tif = blob$extract_date(Name)
        )

      dates_needed <- blob$blob_date_gaps()


      df_tifs_needed <- tif_meta |>
        dplyr$filter(
          date_tif %in% dates_needed
        )

      unzip(
        TMP_PATH,
        exdir = td <- tempdir(),
        files = df_tifs_needed$Name
      )


      tf <- file.path(
        td,
        df_tifs_needed$Name
      )

      lr <- purrr$map(
        tf,
        \(tmp_tf){
          r <- terra$rast(tmp_tf)
          terra$set.names(r,frac_type)
          r
        }
      )
      tifs_downloaded_to_tmp <- glue$glue_collapse(basename(df_tifs_needed$Name),"\n")
      logger$log_info("Downloaded to tmp:\n{tifs_downloaded_to_tmp}")
      lr

    }
  )

# will merge them all and then subset based on date.
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
        utils$capture.output(
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


