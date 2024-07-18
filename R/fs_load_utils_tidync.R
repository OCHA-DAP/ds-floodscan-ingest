box::use(terra)
box::use(tnc = tidync)
box::use(dplyr)
box::use(sf)
box::use(lubridate[as_date,floor_date,year])


#' fs_to_raster
#'
#' @param fs_obj
#' @param band
#'
#' @return
#' @export
#'
#' @examples

fs_to_raster <- function(fs_obj, band){

  df_time_idx <- fs_time_idx_lookup(fs_obj = fs_obj)

  r_ext <-  fs_extent(fs_obj)

  fs_h_array <- tnc$hyper_array(
    x = fs_obj,
    select_var = band
    )

  # reorder dims -- would need a more flexible application to generalize
  fs_array <- aperm(fs_h_array[[band]], c(3, 2, 1))

  r <-   terra$rast(
    x=fs_array,
    extent =r_ext,
    crs= "EPSG:4326"
  )
  terra$set.names(x = r,df_time_idx$date)
  return(r)
}

fs_extent <-  function(fs_obj){
  lon_array<-  fs_obj |>
    tnc$activate("lon") |>
    tnc$hyper_array()

  lat_array<-  fs_obj |>
    tnc$activate("lat") |>
    tnc$hyper_array()

  # trick adapted from the OG https://github.com/rspatial/terra/blob/master/R/rast.R
  x <- list()
  x$x <- lon_array$lon
  x$y <- lat_array$lat
  resx <- abs(( x$x[length(x$x)] - x$x[1] ) / (length(x$x)-1))
  resy <- abs(( x$y[length(x$y)] - x$y[1] ) / (length(x$y)-1))
  xmn <- min(x$x) - 0.5 * resx
  xmx <- max(x$x) + 0.5 * resx
  ymn <- min(x$y) - 0.5 * resy
  ymx <- max(x$y) + 0.5 * resy

  r_ext <-  terra$ext(xmn, xmx, ymn, ymx)
  return(r_ext)
}


fs_time_idx_lookup <-  function(fs_obj){

  # pull time indices
  time_dim <- fs_obj |>
    tnc$activate("time") |>
    tnc$hyper_array()

  # create date - time idx lookup df
  fs_time_tibble <- dplyr$tibble(
    time_index = time_dim$time,
    date = as.Date(time_index, origin = "1998-01-12")
  )
  return(fs_time_tibble)
}


#' Title
#'
#' @param fs_obj
#' @param geometry
#'
#' @return
#' @export
#'
#' @examples
fs_filter_bounds <-  function(fs_obj,geometry){
  geo_bbox <- sf$st_bbox(geometry)

  fs_obj |>
    tnc$hyper_filter(
      lat = dplyr$between(lat, geo_bbox[2],geo_bbox[4]),
      lon = dplyr$between(lon, geo_bbox[1],geo_bbox[3]),
    )
}



#' floodscan_lookup
#' @description
#' helper function used within `zonal_pop_exposure()` to help organize, group, and aggregate FloodScan raster
#'
#' @param r_fs `spatRaster` object returned from fs_to_raster() (floodScan to raster)
#'
#' @return `tibble` lookup table with dates and months of floodscan data to be used to group and summarise FloodScan raster
#' @export
floodscan_lookup <-  function(r_fs){
  # make a lookup table to be used for raster manipulations
  fs_mos<- floor_date(as_date(names(r_fs)),"month")

  fs_lookup <- dplyr$tibble(
    fs_name = as_date(names(r_fs))
  ) |>
    dplyr$mutate(
      fs_mo = floor_date(fs_name,"month"),
      fs_yr = floor_date(fs_name,"year")
    )
  return(fs_lookup)

}
