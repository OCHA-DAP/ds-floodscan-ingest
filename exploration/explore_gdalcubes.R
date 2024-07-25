
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
pc <- blob$load_containers()
Sys.setenv(AZURE_SAS = Sys.getenv("DSCI_AZ_SAS_DEV"))
Sys.setenv(AZURE_STORAGE_ACCOUNT = Sys.getenv("DSCI_AZ_STORAGE_ACCOUNT"))


cog_contents <- AzureStor$list_blobs(
  container = pc$GLOBAL_CONT,
  prefix = "raster/cogs/aer_area"
)

df_cog_contents <- cog_contents |>
  mutate(
    date = extract_date(name),
    virt_path = paste0("/vsiaz/global",name)
  )


df_cog_contents_sample <- df_cog_contents |>
  filter(str_detect(name,".tif$"),
         date>= as.Date("2024-01-01")
  )

date_vec <-  as.Date(df_cog_contents_sample$date)
ic_samp <- gdalcubes::create_image_collection(df_cog_contents_sample$virt_path[1:3],date_time = date_vec[1:3])
