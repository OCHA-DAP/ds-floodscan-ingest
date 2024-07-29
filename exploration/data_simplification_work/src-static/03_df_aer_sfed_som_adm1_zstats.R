#' Run zonal stats on SFED raster stack and Somalia admin 1
#'
#' Reason: trying to see the implication of running zonal stats on pixel level data
#' rather than running zonal stats on SFED and then calculating  the admin
#' level anomaly. My guess is that averageing the pixel-level anomaly will be
#' much different and much lower than admin level anomaly.

#' This will take a while. The bottleneck is loading the raster stack directly
#' from blob. Not sure why it takes so long, but for this reason I run in
#' background with:
#'
#' from project root directory you can run:
#' caffeinate -i -s Rscript data-raw/df_aer_sfed_som_adm1_zstats.R

box::use(terra[...])
box::use(sf[...])
box::use(dplyr[...])
box::use(stringr[...])
box::use(lubridate[...])
box::use(purrr[...])
box::use(tidyr[...])
box::use(arrow)
box::use(logger)
box::use(extract = exactextractr)
box::use(AzureStor[...])

box::use(paths=../../../R/path_utils[load_paths,vp])
box::use(../../../R/utils) # func to get fieldaps/
box::use(../../../src/utils/blob)

sf_use_s2(FALSE)
Sys.setenv(AZURE_SAS = Sys.getenv("DSCI_AZ_SAS_DEV"))
Sys.setenv(AZURE_STORAGE_ACCOUNT = Sys.getenv("DSCI_AZ_STORAGE_ACCOUNT"))

# Run the R scripts in the R/ folder with your custom functions:
bc <- blob$load_containers()
gc <- bc$GLOBAL_CONT
pc <- bc$PROJECTS_CONT

logger$log_info("download admin files from field maps")
lgdf <- utils$download_fieldmaps_sf("SOM", layer = c("som_adm0","som_adm1"))

logger$log_info("getting cog contents")
df_cog_contents <- AzureStor::list_blobs(
  container = gc,
  prefix = "raster/cogs/aer_area_300s"
)


df_cog_contents <- df_cog_contents |>
  dplyr::mutate(
    date = utils$extract_date(name),
    vp = paste0("/vsiaz/global/",name)
  ) |>
  dplyr::filter(
    str_detect(string = name,pattern = ".tif$")
  )

df_cog_contents <- df_cog_contents|>
  arrange(
    date
  ) |>
  mutate(
    year = year(date)
  )

df_zonal <- split(df_cog_contents,df_cog_contents$year) |>
  map(
    \(df_tmp){
      yr_tmp <- unique(df_tmp$year)
      cog_urls <- df_tmp$vp

      logger$log_info(
        "loading {yr_tmp} urls into large raster stack"
      )
      r <- rast(cog_urls)

      logger$log_info("subsetting to SFED")
      r_sfed <- r[[names(r)=="SFED"]]
      date_names <- utils$extract_date(basename(sources(r_sfed)))
      terra::set.names(r_sfed, date_names)

      logger$log_info("Starting Zonal Stats")

      extract$exact_extract(
        r_sfed,
        lgdf$som_adm1,
        fun = "mean",
        append_cols = c("ADM1_EN","ADM1_PCODE"),
        progress = TRUE
      ) |>
        pivot_longer(-starts_with("ADM")) |>
        separate(name, into = c("stat","date"), sep = "\\.") |>
        mutate(
          date = as.Date(date)
        )
    }
  ) |>
  list_rbind()

logger$log_info("Writing to Blob")
tf <- tempfile(fileext = ".parquet")

arrow$write_parquet(
  df_zonal,
  tf
)
out_file<- "ds-floodscan-ingest/df_aer_sfed_som_adm1_zstats.parquet"

upload_blob(
  container = pc,
  src = tf,
  dest = out_file
)

# download_blob(
#   container = pc,
#   src = out_file,
#   dest = tf <- tempfile(fileext = ".parquet")
#
# )
# arrow$read_parquet(tf)
