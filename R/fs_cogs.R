box::use(googledrive[...])

box::use(purrr[...])
box::use(readr[...])
box::use(dplyr[...])
box::use(stringr[...])
box::use(tnc= tidync)
box::use(terra[...])
box::use(fs=./R/fs_to_raster)
box::use(rnc= RNetCDF)

# fs_base_path <- file.path(
#   Sys.getenv("AA_DATA_DIR_NEW"),
#   "private",
#   "raw",
#   "glb",
#   "FloodScan"
# )
# sfed_path <-  file.path(
#  fs_base_path,
#  "SFED",
#  "SFED_historical",
#  "aer_sfed_area_300s_19980112_20231231_v05r01.nc"
# )
#
# mfed_path <-  file.path(
#  fs_base_path,
#  "MFED",
#  "MFED_historical",
#  "aer_mfed_area_300s_19980112_20231231_v05r01.nc"
# )



#' Title
#'
#' @param r
#'
#' @return
#' @export
#'
#' @examples \dontrun{
#' sfed_path <-  file.path(
#'   fs_base_path,
#'   "SFED",
#'   "SFED_historical",
#'   "aer_sfed_area_300s_19980112_20231231_v05r01.nc"
#' )
#' mfed_path <-  file.path(
#'    fs_base_path,
#'    "MFED",
#'    "MFED_historical",
#'    "aer_mfed_area_300s_19980112_20231231_v05r01.nc"
#' )
#' sfed_nc <- rnc$open.nc(sfed_path)
#' mfed_nc <- rnc$open.nc(mfed_path)
#'
#' r <- fs_merge_sfed_mfed(sfed_ob=sfed_nc,
#'                         mfed_ob=mfed_nc,
#'                         time_index=1)
#' af <- rnaturalearth::ne_countries(type="countries",continent="Africa")
#' r[r==0]<-NA
#' plot(af);plot(r[[1]],add=T)
#' fs_write_cog(r)
#'
#' }





fs_write_cog <- function(r,path){

  date_suffix <- format(as.Date(unique(time(r))), "%Y%m%d")
  out_cog_file_name <- paste0("aer_fs_300s_",date_suffix,".tif")

  if(!is.null(path)){
    out_cog_file_name <- file.path(path,
                                   out_cog_file_name)
  }

  terra::writeRaster(r,
                     filename = out_cog_file_name,
                     filetype = "COG",
                     gdal = c("COMPRESS=DEFLATE",
                              "SPARSE_OK=YES",
                              "OVERVIEW_RESAMPLING=AVERAGE")
  )
}





#' Title
#'
#' @param sfed_ob
#' @param mfed_ob
#' @param time_index
#'
#' @return
#' @export
#'
#' @examples
#'
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
fs_merge_sfed_mfed <- function(sfed_ob=sfed_nc,
                               mfed_ob=mfed_nc,
                               time_index=1){

  lr <- list("MFED_AREA"=mfed_ob,"SFED_AREA"=sfed_ob) |>
    imap(\(nc_ob_tmp,bname_tmp){
      fs_to_raster(
        nc_ob=nc_ob_tmp,
        band=bname_tmp,
        time_index = time_index)
    }
    )

  r <- rast(lr)
  return(r)

}

# fs_to_raster(nc_ob=sfed_nc,
#                  band ="SFED_AREA",
#                    time_index=1)

# fs_to_raster(nc_ob=nc_mfed,
#              band="MFED_AREA",
#              time_index=1)

fs_to_raster <- function(
    nc_ob,
    band="MFED_AREA",
    time_index
                         ){
  fs_array <- fs_get_array(nc_ob,
                           band=band,
                           time_index=time_index)
  ex <- fs_extent(nc_ob)
  r <- rast(
    fs_array,
    ext = ex,
    crs = "OGC:CRS84"
  )
  time(r) <- fs_date(time_index)
  set.names(r, band)
  return(r)
}


fs_get_array <- function(nc_ob, band, time_index=1){
  fs_array <- rnc$var.get.nc(
    nc = nc_ob,
    variable  = band,
    start = c(time_index,1 ,1 ),
    count = c(1, 1080,1080)
  )
  fs_array_perm <-  aperm(fs_array, c(2,1))
  return(fs_array_perm)

}

fs_date <- function(time_index){
  as.Date(time_index-1, origin = "1998-01-12")
}


fs_extent <- function(nc_ob){
  lat <- rnc$var.get.nc(nc_ob, "lat")
  lon <- rnc$var.get.nc(nc_ob, "lon")
  dx <- diff(lon[1:2])
  dy <- abs(diff(lat[1:2]))
  ex <- c(min(lon) - dx/2, max(lon) + dx/2,
          min(lat) - dy/2, max(lat) + dy/2)
  return(ex)
}
