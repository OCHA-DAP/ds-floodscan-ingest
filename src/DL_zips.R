library(googledrive)
library(purrr)
library(readr)

drive_auth(
  path=Sys.getenv("FS_SA_JSON")
)

drive_dribble <- drive_ls(
  corpus = "user"
)

run_date <- Sys.Date()
run_date_chr <- format(latest_file_date,"%Y%m%d")

dl_log <- c("SFED","MFED") |>
  purrr::map(
    \(frac_type){
      TMP_NAME <-  paste0("FloodScan_",frac_type,"_90d_",run_date_chr,".zip")
      TMP_PATH <- file.path(tempdir(), TMP_NAME)
      DL_VAR <- paste0("FLOODSCAN_",frac_type,"_URL")
      DL_URL <- Sys.getenv(DL_VAR)

      cat("Downloading " , frac_type,"\n")
      download.file(DL_URL,TMP_PATH, quiet=TRUE)
      zip_contents <- unzip(TMP_PATH,list=T) # cool list trick doesn't actually unzip
      yyyymmdd<- stringr::str_extract(zip_contents$Name,
                                      pattern = "\\d{8}")

      yyyymmdd_filt <- yyyymmdd[!is.na(yyyymmdd)]

      yyyymmdd_dates <- as.Date(yyyymmdd_filt, format = "%Y%m%d")
      latest_file_date_chr <- format(latest_file_date,"%Y%m%d")

      file_name_zip <- paste0("aer_floodscan_",
                              tolower(frac_type),
                              "_area_flooded_fraction_africa_90days_",
                              latest_file_date_chr,
                              ".zip")

      drive_target_dir_id <- drive_dribble[drive_dribble$name == paste0(frac_type,"_zips"),]$id
      cat("Uploading " , frac_type,"to drive\n")

      drive_upload(
        media = TMP_PATH,
        path = as_id(drive_target_dir_id),
        name = file_name_zip
      )

      dl_log <- data.frame(
        file_name = file_name_zip,
        min_date = min(yyyymmdd_dates),
        max_date= max(yyyymmdd_dates),
        download_date= run_date,
        type = frac_type
      )
      unlink(TMP_PATH)
      return(dl_log)
    }
  )

df_dl_log <- dl_log %>%
  list_rbind()

temp_csv_file <- file.path(tempdir(), "FloodScan_zip_DL_log.csv")
drive_dribble
drive_upload(
    media = temp_csv_file,
    path = drive_dribble[drive_dribble$name =="FloodScan",]$id,

    name = basename(temp_csv_file)
  )
