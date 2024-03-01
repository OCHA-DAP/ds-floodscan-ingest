library(googledrive)
library(purrr)
library(readr)
library(dplyr)

# good for testing log
upload_to_drive <- c(T,F)[1]

drive_auth(
  path=Sys.getenv("FS_SA_JSON")
)

drive_dribble <- drive_ls(
  corpus = "user"
)
file_name_DL_log <- "FloodScan_zip_DL_log.csv"
# Get Download Log If initiated

dl_log_id <- drive_dribble[drive_dribble$name==file_name_DL_log,]$id
drive_download(
  file = as_id(
    dl_log_id
  ),
  path = f <- tempfile(fileext = ".csv")
)




previous_dl_log <- read_csv(f)



previous_dl_log_compare <-  previous_dl_log |>
  select(-any_of(c("update_available","download_date")))


run_date <- Sys.Date()
run_date_chr <- format(run_date,"%Y%m%d")

dl_log <- c("SFED","MFED") |>
  purrr::map(
    \(frac_type){
      # frac_type <- "SFED"

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

      latest_file_date_chr <- format(max(yyyymmdd_dates),"%Y%m%d")

      file_name_zip <- paste0("aer_floodscan_",
                              tolower(frac_type),
                              "_area_flooded_fraction_africa_90days_",
                              latest_file_date_chr,
                              ".zip")

      drive_target_dir_id <- drive_dribble[drive_dribble$name == paste0(frac_type,"_zips"),]$id


      df_dl_log <- data.frame(
        file_name = file_name_zip,
        min_date = min(yyyymmdd_dates),
        max_date= max(yyyymmdd_dates),
        download_date= run_date,
        type = frac_type,
        update_available =T
      )

      df_dl_log_new <- anti_join(df_dl_log,previous_dl_log_compare)
      new_records <- nrow(df_dl_log_new)>0
      if(!new_records){
        cat("no new records \n")
        df_dl_log <- df_dl_log |>
          mutate(
            update_available =F
          )
      }

      # if new records add to drive
      if(new_records){
        cat("New records: uploading " , frac_type,"to drive\n")
        if(upload_to_drive){
          drive_upload(
            media = TMP_PATH,
            path = as_id(drive_target_dir_id),
            name = file_name_zip,
            overwrite = T
          )
        }


      }
      unlink(TMP_PATH)
      return(df_dl_log)
    }
  )

df_dl_log <- dl_log |>
  list_rbind()

# if(unique(df_dl_log$download_date) != max(previous_dl_log$download_date)){
  new_record_log_df <- dplyr::anti_join(
    df_dl_log,
    previous_dl_log
  )

# }

dl_log_updated <- bind_rows(
  previous_dl_log,
  new_record_log_df
)

# write csv to temp
write_csv(dl_log_updated,
          file = temp_csv_file <- file.path(
            tempdir(),
            file_name_DL_log
          )
)

# upload csv to temp
if(upload_to_drive){
  drive_upload(
    media = temp_csv_file,
    path = as_id(drive_dribble[drive_dribble$name =="FloodScan",]$id),
    name = basename(temp_csv_file),
    overwrite = T
  )


}


# google drive not syncing so have to access programmaticaly for troubleshooting log.
# write csv to temp
# write_csv(previous_dl_log |>
#             filter(download_date!="2024-03-01"),
#           file = temp_csv_file <- file.path(
#             tempdir(),
#             file_name_DL_log
#           )
# )
#
# # upload csv to temp
# if(upload_to_drive){
#   drive_upload(
#     media = temp_csv_file,
#     path = as_id(drive_dribble[drive_dribble$name =="FloodScan",]$id),
#     name = basename(temp_csv_file),
#     overwrite = T
#   )
#
# }

