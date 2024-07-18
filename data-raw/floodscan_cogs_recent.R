library(googledrive)
library(tidyverse)
library(terra)
# good for testing log
upload_to_drive <- c(T,F)[2]


# previously had GHA downloading zip files directly from link to drive
# so we will connect to drive and grab the zip files and process them further
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

# basically a look up table with dates
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


# time of writing, most recent is : 2024-06-01
max(drive_fs_zips$date_filename)
#> [1]  "2024-06-01"

# since they are 90d rotating zips we can get away
# with just processing 2 of them to get full 5 month coverage
update_date <- as_date("2024-06-01")
zip_dates <- list(
  "ZIP_DATE2" = update_date,
  "ZIP_DATE1" =  update_date-90
)

# unzip the 2
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

# list all unzipped files and make lookup
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


# let's now loop through each day
day_seq <- seq(
  as_date("2024-01-01"),
  as_date("2024-06-01"), by ="day"
)


# combine sfed and mfed and write a cog to blob
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
        set_names(df_split,toupper(names(df_split))),
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

    out_fn <- str_remove(df_split$sfed$base,"_sfed")
    cumulus::write_az_file(
      service = "blob",
      stage = "dev",
      x = r_processed,
      name = paste0("raster/cogs/",out_fn),
      container = "global",
      endpoint_template =  Sys.getenv("DSCI_AZ_ENDPOINT"),
      sas_key = Sys.getenv("DSCI_AZ_SAS_DEV")
    )

  }
)


