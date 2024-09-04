box::use(purrr)
box::use(glue)
box::use(rnc= RNetCDF)
box::use(cumulus)
box::use(cogger =../R/fs_cogs)

fs_base_path <- file.path(
  Sys.getenv("AA_DATA_DIR_NEW"),
  "private",
  "raw",
  "glb",
  "FloodScan"
)

sfed_path <-  file.path(
  fs_base_path,
  "SFED",
  "SFED_historical",
  "aer_sfed_area_300s_19980112_20231231_v05r01.nc"
)
mfed_path <-  file.path(
   fs_base_path,
   "MFED",
   "MFED_historical",
   "aer_mfed_area_300s_19980112_20231231_v05r01.nc"
)
sfed_nc <- rnc$open.nc(sfed_path)
mfed_nc <- rnc$open.nc(mfed_path)

fs_times <- rnc$var.get.nc(sfed_nc,variable  = "time") +1


purrr$map(
  fs_times,
  \(time_idx){
    cat(cogger$fs_date(time_idx),"\n")
    r <- cogger$fs_merge_sfed_mfed(
      sfed_ob=sfed_nc,
      mfed_ob = mfed_nc,
      time_index = time_idx
    )
    yyyymmdd <- unique(format(as.Date(time(r)),format = "%Y%m%d"))
    fn <- glue$glue("aer_area_300s_{yyyymmdd}_v05r01.tif")

    cumulus$write_az_file(
      service = "blob",
      stage = "dev",
      x = r,
      name = paste0("raster/cogs/",fn),
      container = "global",
      endpoint_template =  Sys.getenv("DSCI_AZ_ENDPOINT"),
      sas_key = Sys.getenv("DSCI_AZ_SAS_DEV")
    )

  }
)




#' map(
# c(1:100),
# \(time_index_tmp){
#   rtmp <- fs_merge_sfed_mfed(sfed_ob=sfed_nc,
#                              mfed_ob=mfed_nc,
#                              time_index=time_index_tmp)
#   fs_write_cog(rtmp,path = "cogs")
#
# }
# )
