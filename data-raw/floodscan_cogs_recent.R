library(googledrive)
library(tidyverse)

# good for testing log
upload_to_drive <- c(T,F)[2]

drive_auth(
  path=Sys.getenv("FS_SA_JSON")
)

drive_dribble <- drive_ls(
  corpus = "user"
)


# write regrex to extract values that contain _sfed_ and end with .zip

filenames <- str_subset(
  drive_dribble$name,
  pattern = "_sfed_|_mfed_.*\\.zip$"
)

drive_fs_zips <-  drive_dribble |>
  filter(
    name %in% filenames
  ) |>
  mutate(
    date_filename = as_date(
      str_extract(
        name,
        pattern = "\\d{8}"
      ), format = "%Y%m%d"
    )
  )


# time of writing, most recent is : 2024-05-28
max(drive_fs_zips$date_filename)
#> [1]  "2024-05-28"


zip_dates <- list(
  "ZIP_DATE2" = as_date("2024-05-28"),
  "ZIP_DATE1" =  as_date("2024-05-28")-90
)

map(
  zip_dates,
  \(dt){
    drive_tmp <- drive_fs_zips |>
      filter(date_filename==dt) |>
      mutate(
        parent_unzip = fs::path_ext_remove(name)
      )


    map(
      drive_tmp$id,
      \(drive_id_tmp,parent_tmp){
        drive_download(
          file = as_id(
            drive_id_tmp
          ),
          path = f <- tempfile(fileext = ".zip")
        )

        inside_zip <- unzip(f,exdir = td <- tempdir(),list=TRUE)
        inside_zip_tifs <- str_subset(inside_zip$Name,"\\.tif$")
        unzip(f,exdir = td <- tempdir(),files = inside_zip_tifs)


      }
    )
  }
)


fp_tifs <- list.files(
  file.path(
    tempdir()
  ),
  recursive = TRUE,
  pattern = "\\.tif$",
  full.names = TRUE
)

df_tif_lookup <- tibble(
  full = fp_tifs,
  base = basename(fp_tifs),
  date= as_date(
    str_extract(
      fp_tifs,
      pattern = "\\d{8}"
    ), format = "%Y%m%d"
  ),
  band = str_extract(base, "sfed|mfed")
  )

day_seq <- seq(
  as_date("2024-01-01"),
  as_date("2024-05-28"), by ="day"
)


cog_dir_out <- file.path(
  Sys.getenv("AA_DATA_DIR_NEW"),
  "private",
  "processed",
  "glb",
  "floodscan_cogs"
)
df_tif_lookup$date |> range()

map(
  day_seq,
  \(dt){
    print(as_date(dt))
    df_tif_tmp <- df_tif_lookup |>
      filter(
        date==dt
      )
    df_split <- split(df_tif_tmp,df_tif_tmp$band)


      lr_processed <- map(
        set_names(df_split,names(df_split)),
        \(dft){
          r <- rast(dft$full)
          time(r) <- as_date(dft$date)
          set.names(r,toupper(dft$band))
          r

        }
      )
      r_processed <- rast(
        lr_processed
      )

#
#     r_sfed <- rast(df_split$sfed$full)
#     time(r_sfed) <- as_date(df_split$sfed$date)
#     set.names(r_sfed,"SFED")
#
#     r_mfed <- rast(df_split$mfed$full)
#     time(r_mfed) <- as_date(df_split$mfed$date)
#     set.names(r_mfed,"MFED")


    out_fn <- str_remove(df_split$sfed$base,"_sfed")
    terra::writeRaster(r_processed,
                       filename = file.path(cog_dir_out,out_fn),
                       filetype = "COG",
                       overwrite= TRUE,
                       gdal = c("COMPRESS=DEFLATE",
                                "SPARSE_OK=YES",
                                "OVERVIEW_RESAMPLING=AVERAGE")
    )


  }
)


