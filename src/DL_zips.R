library(googledrive)
library(purrr)
library(readr)
library(dplyr)

drive_auth(
  path=Sys.getenv("FS_SA_JSON")
)

drive_dribble <- drive_ls(
  corpus = "user"
)

# Get Download Log If initiated
# file_name_DL_log <- "FloodScan_zip_DL_log.csv"
# dl_log_id <- drive_dribble[drive_dribble$name==file_name_DL_log,]$id
# dl_log_initiated <- length(dl_log_id)>0
#
# if(dl_log_initiated){
#   drive_download(
#     file = as_id(
#       dl_log_id
#     ),
#     path = f <- tempfile(fileext = ".csv")
#   )
#
#   previous_dl_log <- read_csv(f)
#
#   # remove these cols
#   previous_dl_log_compare <-  previous_dl_log |>
#     select(-any_of(c("download_date","update_available")))
#
# }
#


run_date <- Sys.Date()
run_date_chr <- format(run_date,"%Y%m%d")

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

      latest_file_date_chr <- format(max(yyyymmdd_dates),"%Y%m%d")

      file_name_zip <- paste0("aer_floodscan_",
                              tolower(frac_type),
                              "_area_flooded_fraction_africa_90days_",
                              latest_file_date_chr,
                              ".zip")

      drive_target_dir_id <- drive_dribble[drive_dribble$name == paste0(frac_type,"_zips"),]$id
      cat("Uploading " , frac_type,"to drive\n")

      dl_log <- data.frame(
        file_name = file_name_zip,
        min_date = min(yyyymmdd_dates),
        max_date= max(yyyymmdd_dates),
        download_date= run_date,
        type = frac_type,
        update_available =T
      )

        drive_upload(
          media = TMP_PATH,
          path = as_id(drive_target_dir_id),
          name = file_name_zip
        )

      unlink(TMP_PATH)
      return(dl_log)
    }
  )

df_dl_log <- dl_log |>
  list_rbind()

# if(dl_log_initiated){
#
# }
#
# current_dl_log_compare<- df_dl_log |>
#   select(-download_date)
#
#
#
# new_record_log_df <- dplyr::anti_join(
#   current_dl_log_compare,
#   previous_dl_log_compare
#   )
# if(nrow(new_record_log_df)){
#
# }
# df_dl_log[[-c("download_date")]]
# df_dl_log |>
#   mutate(
#     max_date
#   )
# merge(df_dl_log,df_dl_log,all = T)

#


# drive_download(aoi_drive,
#                path = f <- tempfile(fileext = ".rds")
#                )
# drive_download(file = ,
#                path = as_id(
#   drive_dribble[drive_dribble$name ==file_name_DL_log]$id
#   ),
#   type = ,
#   overwrite = ,verbose = )

# write csv to temp
write_csv(df_dl_log,
          file = temp_csv_file <- file.path(
            tempdir(),
            file_name_DL_log
          )
)

# upload csv to temp
drive_upload(
  media = temp_csv_file,
  path = as_id(drive_dribble[drive_dribble$name =="FloodScan",]$id),
  name = basename(temp_csv_file)
)
