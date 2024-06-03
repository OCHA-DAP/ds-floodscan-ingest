library(purrr)
library(readr)
library(dplyr)


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

      tif_meta <- tibble(
        unzip(TMP_PATH,list=T)
        )|>
        select(Name) |>
        mutate(
          yyyymmdd = stringr::str_extract(Name,
                                          pattern = "\\d{8}") ,
          date_tif = as_date(yyyymmdd,format = "%Y%m%d")
        )

      df_most_recent <- tif_meta |>
        filter(date_tif ==max(date_tif,na.rm=T))

      unzip(
        TMP_PATH,
        exdir = td <- tempdir(),
        files = df_most_recent$Name
      )
      tf <- file.path(
        td,
        df_most_recent$Name
      )
      r <- terra::rast(tf)
      terra::set.names(r,frac_type)
      r
    }
  )

r <- terra::rast(lr)
src_name <- str_remove(basename(terra::sources(r)[1]),"sfed_|mfed_")

cumulus::write_az_file(
        service = "blob",
        stage = "dev",
        x = r,
        name = paste0("raster/cogs/",src_name),
        container = "global",
        endpoint_template =  Sys.getenv("DSCI_AZ_ENDPOINT"),
        sas_key = Sys.getenv("DSCI_AZ_SAS_DEV")
      )

