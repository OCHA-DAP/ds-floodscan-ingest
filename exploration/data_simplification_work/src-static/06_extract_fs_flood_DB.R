box::use(dplyr[...])
# box::use(ggplot2[...])
box::use(lubridate[...])
box::use(janitor[clean_names])
box::use(purrr[...])
box::use(extRemes[...])
box::use(gghdx[...])
box::use(reactable[...])
box::use(tidyr[...])
# box::use(patchwork)
box::use(readr)
box::use(sf)
box::use(terra[...])
# box::use(stringr[...])
box::use(rne = rnaturalearth)
# box::use(leaflet[...])

box::use(
  AzureStor,
  arrow,
  logger
)


box::use(paths = .. /../../ R / path_utils)
box::use(.. /../../ src / utils / blob)
box::use(../../../R/utils) #

sf$sf_use_s2(use_s2 =F)
Sys.setenv(AZURE_SAS = Sys.getenv("DSCI_AZ_SAS_DEV"))
Sys.setenv(AZURE_STORAGE_ACCOUNT = Sys.getenv("DSCI_AZ_STORAGE_ACCOUNT"))
fp_floods_db <- paths$load_paths(path_name = "FP_GLOBAL_FLOODS_DB", virtual_path = F)

fps <- paths$load_paths()

bc <- blob$load_containers()
gc <- bc$GLOBAL_CONT
pc <- bc$PROJECTS_CONT

AzureStor$download_blob(
  container = pc,
  src = fp_floods_db,
  dest = tf <- tempfile(fileext = ".csv"),
  overwrite = T
)
df_floods_db <- readr$read_csv(tf) |>
  clean_names()


gdf_floods <- sf$st_as_sf(
  df_floods_db,
  coords= c("dfo_centroid_x","dfo_centroid_y"),
  crs= 4326
)



gdf_adm_africa <- rne$ne_countries(type ="countries",
                                   continent = "Africa") |>
  sf$st_make_valid() |>
  summarise(
    do_union =TRUE
  )

df_urls <- utils$floodscan_cog_meta_df(container = gc,prefix= "raster/cogs/aer")

gdf_floods_afr <- gdf_floods[gdf_adm_africa,]

time_load <- system.time(
  r <- rast(df_urls$urls)
)

cat(time_load,"\n")

logger$log_info("subsetting SFED")
r <- r[[names(r)=="SFED"]]
set.names(
  r,utils$extract_date(sources(r))
)

logger$log_info("Extracting pixel values")
df_point_values <- extract( r,gdf_floods_afr)



logger$log_info("Writing to Blob")
tf <- tempfile(fileext = ".parquet")

arrow$write_parquet(
  df_point_values,
  tf
)
out_file<- "ds-floodscan-ingest/df_aer_flood_db_historical.parquet"

AzureStor$upload_blob(
  container = pc,
  src = tf,
  dest = out_file
)


