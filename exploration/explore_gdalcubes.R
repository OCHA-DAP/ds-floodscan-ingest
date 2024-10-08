
# rast arithmetic operates band-wise based on band index. Therefore
# if we have 2 spatRasters w/ the same number of bands aligned they will
# subtract from eachother by index. Sets w/ different number of bands will
# get recycled (like vector math)

c(3,2,1) - c(3,2,1)
c(3,2,1,0) - c(3,2,1)

diff_check <- r_365 - r_historical
diff_check[[366]]
r_historical[[366]]

test_wrap <- r_365[[1]] - r_historical[[366]]
test_idx1 <- r_365[[1]] - r_historical[[1]]
test_idx10 <- r_365[[10]] - r_historical[[10]]


test_wrap_res <- diff_check[[366]]-test_wrap
test_idx1_res <- diff_check[[1]]-test_idx1
test_idx10_res <- diff_check[[10]]-test_idx10










dest_dir = tempdir()
td_fs <- file.path(dest_dir,"AER")


unzip(zip_name, exdir = td_fs)
tif_files <- list.files(td_fs,full.names =  T,pattern = ".tif$")
# unlink(file.path(dest_dir, "MOD11A2.zip"))

extract_date <-  function(x){
  as.Date(str_extract(x, "\\d{8}"),format = "%Y%m%d")
}
date_vec <- extract_date(basename(tif_files))
fs_collection = gdalcubes$create_image_collection(files = tif_files,date_time = date_vec,band_names = c("SFED","SFED_BASELINE"))

# needa get ressolution
r_ex <- rast(tif_files[1])

fs_cubeview <- gdalcubes$cube_view(extent = fs_collection,ny = 164,nx =125,nt = 3,srs= "EPSG:4326")
fs_cube <- gdalcubes$raster_cube(image_collection = fs_collection, view = fs_cubeview)
plot(fs_cube)






# gdalcubes azure? --------------------------------------------------------

box::use(../src/utils/blob)
box::use(stringr[...])
box::use(paths = ../R/path_utils)
box::use(AzureStor)
box::use(gdalcubes[...])
box::use(terra[...])
box::use(dplyr[...])
box::use(../R/utils)
pc <- blob$load_containers()
Sys.setenv(AZURE_STORAGE_SAS_TOKEN = Sys.getenv("DSCI_AZ_SAS_DEV"))
Sys.setenv(AZURE_STORAGE_ACCOUNT = Sys.getenv("DSCI_AZ_STORAGE_ACCOUNT"))


cog_contents <- AzureStor$list_blobs(
  container = pc$GLOBAL_CONT,
  prefix = "raster/cogs/aer_area"
)

df_cog_contents <- cog_contents |>
  mutate(
    date = extract_date(name),
    virt_path = paste0("/vsiaz/global",name),
    virt_path_curl = paste0("/vsicur/global/",name)
  )

df_cog_contents <- utils$floodscan_cog_meta_df(container = gc)
tibble(df_cog_contents)

df_cog_contents_sample <- df_cog_contents |>
  filter(str_detect(name,".tif$"),
         date>= as.Date("2024-01-01")
  )


date_vec <-  as.Date(df_cog_contents_sample$date)
# gdalcubes::gdalcubes_set_gdal_config(
#     "AZURE_SAS" , Sys.getenv("DSCI_AZ_SAS_DEV")
# )
# gdalcubes::gdalcubes_set_gdal_config(
#   "AZURE_STORAGE_ACCOUNT" , Sys.getenv("DSCI_AZ_STORAGE_ACCOUNT")
# )
gdalcubes_cloud_config <- function() {
  # set recommended variables for cloud access:
  # https://gdalcubes.github.io/source/concepts/config.html#recommended-settings-for-cloud-access
  gdalcubes::gdalcubes_set_gdal_config("VSI_CACHE", "TRUE")
  gdalcubes::gdalcubes_set_gdal_config("GDAL_CACHEMAX","30%")
  gdalcubes::gdalcubes_set_gdal_config("VSI_CACHE_SIZE","10000000")
  gdalcubes::gdalcubes_set_gdal_config("GDAL_HTTP_MULTIPLEX","YES")
  gdalcubes::gdalcubes_set_gdal_config("GDAL_INGESTED_BYTES_AT_OPEN","32000")
  gdalcubes::gdalcubes_set_gdal_config("GDAL_DISABLE_READDIR_ON_OPEN","EMPTY_DIR")
  gdalcubes::gdalcubes_set_gdal_config("GDAL_HTTP_VERSION","2")
  gdalcubes::gdalcubes_set_gdal_config("GDAL_HTTP_MERGE_CONSECUTIVE_RANGES","YES")
  gdalcubes::gdalcubes_set_gdal_config("GDAL_NUM_THREADS", "ALL_CPUS")
}
gdalcubes_cloud_config()
tibble(df_cog_contents_sample)
length(date_vec)
ic_samp <- gdalcubes::create_image_collection(
  df_cog_contents_sample$urls[1:10],
  date_time = date_vec[1:10],
  band_names = c("SFED","MFED")
  )

library(sf)
gdf_rect <- mapedit::drawFeatures()
bbox <- st_bbox(gdf_rect)
rc_samp <- raster_cube(ic_samp,incomplete_ok= FALSE)
zoo::rollmean()
rc_samp_sfed <- select_bands(rc_samp,"SFED")
reduce_time(rc_samp, "mean(SFED)") # seems to work
?cube_view

v <- cube_view (
  extent = list(
    left = bbox$xmin,
    right = bbox$xmax,
    top = bbox$ymax,
    bottom = bbox$ymin,
    t0 = "2024-01-01",
    t1 = "2024-01-10"

  ),
  srs = "EPSG:4326",
  dx= 0.08333333,dy= 0.08333333,
  dt= "P1D"
)
rc <- raster_cube(ic_samp, view = v) |>
  select_bands(c("SFED"))

uniform_kernel <- rep(1/3, 3)
rc_w = window_time(rc, window = c(1,1), kernel = uniform_kernel)
rc_w = window_time(rc, window = c(1,1),expr= "mean")
# rc_w |> slice_time(datetime="2024-01-01") |> plot()

stars1 <- rc_w |> stars::st_as_stars()

tr_from_gc <- terra::rast(stars1)
tr_from_gc[[nlyr(tr_from_gc)]]
names(tr_from_gc)
plot(tr_from_gc[[1]])
plot(tr_r3[[2]])

crop(tr_from_gc, tr_r3)
crop(tr_r3,tr_from_gc )

ext(tr_r3)<- ext(tr_from_gc)
plot(tr_r3[[2]])
plot(tr_from_gc[[2]],add=T)

tr <- rast(df_cog_contents_sample$urls[1:10])
tr <- tr[[names(tr)=="SFED"]]
tr_cropped <- terra::crop(tr,bbox)
set.names(tr_cropped, date_vec[1:10])
tr_r3 <- terra::roll( x = tr_cropped,n=3, fun = mean, type = "around", circular = FALSE)
tr_r3[[nlyr(tr_r3)]]
df_tr_r3 <- terra::extract(tr_r3,gdf_rect, fun ="mean") |>
  tidyr::pivot_longer(-ID)
exactextractr::exact_extract(tr_r3,gdf_rect, fun = "mean")

df_tr_gc <- terra::extract(tr_from_gc,gdf_rect, fun ="mean") |>
  tidyr::pivot_longer(-ID)

tibble(
  df_tr_gc$value*1000,
  df_tr_r3$value*1000
)
exactextractr::exact_extract(tr_from_gc,gdf_rect, fun = "mean")


tibble(df_tr_r3) |>
  count(ID)
# manual check
mean(tr_cropped[[1:3]])
r_mean_test <- rc |>
  # select_time()
  select_time(t = c("2024-01-01","2024-01-02","2024-01-03")) |>
  reduce_time("mean(SFED)")
rmtf <- r_mean_test |>
  filter_geom(geom=gdf_rect$geom)

rmtf |> plot()

stars_mean_test <- r_mean_test |> stars::st_as_stars()

terra::rast(stars_mean_test)





rc_w |> gdalcubes::slice_time(datetime = "2024-01-01") |> plot()
rc |> gdalcubes::slice_time(datetime = "2024-01-01") |> plot()

?cube_view
# create image collection from example Landsat data only
# if not already done in other examples
if (!file.exists(file.path(tempdir(), "L8.db"))) {
  L8_files <- list.files(system.file("L8NY18", package = "gdalcubes"),
                         ".TIF", recursive = TRUE, full.names = TRUE)
  create_image_collection(L8_files, "L8_L1TP", file.path(tempdir(), "L8.db"), quiet = TRUE)
}

L8.col = image_collection(file.path(tempdir(), "L8.db"))
v = cube_view(extent=list(left=388941.2, right=766552.4,
                          bottom=4345299, top=4744931, t0="2018-01", t1="2018-07"),
              srs="EPSG:32618", nx = 400, dt="P1M")
L8.cube = raster_cube(L8.col, v)
L8.nir = select_bands(L8.cube, c("B05"))
L8.nir.min = window_time(L8.nir, window = c(2,2), "min(B05)")


L8.nir.kernel = window_time(L8.nir, kernel=c(-1,1), window=c(1,0))
L8.nir.kernel



rc_samp_sfed_roll <- apply_time(
  rc_samp_sfed,
  names = "rollmean_SFED",
  FUN = function(x){
    zoo::rollmean(x, k = 3, fill = NA, align = "center")
  }
)

