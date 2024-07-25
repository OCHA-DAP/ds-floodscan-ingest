box::use(purrr[map])

#' load_paths
#' @param virtual_path `logical` if FALSE (default) returns blob path, if
#'     TRUE attaches  prefix for azure gdal virtual path. This gdal virtual path
#'     is useful if you want to read the data directly from blob into raster
#'     format without downloading. Where as the default non-virtual isused for
#'     writing files to blob
#'
#' @export
load_paths <-function(virtual_path = FALSE){
  ret <- list()
  # Thresholded
  ret$FP_DOY_THRESH_COG <-  "ds-floodscan-ingest/aer_area_300s_doy_mean_thresh_gte0.01_baseline_1998_2020.tif"
  # thresholded and smoothed
  ret$FP_DOY_SMOOTHED20D_THRESH_COG <-  "ds-floodscan-ingest/aer_area_300s_doy_mean_thresh_gte0.01_20d_smoothed_baseline_1998_2020.tif"

  ret$FP_DOY_SMOOTHED30D_THRESH_COG <-  "ds-floodscan-ingest/aer_area_300s_doy_mean_thresh_gte0.01_30d_smoothed_baseline_1998_2020.tif"

  # no thresholds

  ret$FP_DOY_NO_THRESH <-  "ds-floodscan-ingest/aer_area_300s_doy_mean_baseline_1998_2020.tif"

  ret$FP_DOY_NO_THRESH_SMOOTHED30d = "ds-floodscan-ingest/aer_area_300s_doy_no_thresh_30d_smoothed_baseline_1998_2020.tif"

  ret$FP_DOY_NO_THRESH_SMOOTHED20d <- "ds-floodscan-ingest/aer_area_300s_doy_no_thresh_20d_smoothed_baseline_1998_2020.tif"

  ret$FP_LAST365D = "ds-floodscan-ingest/aer_area_300s_last_365d.tif"

  ret$FP_LAST365D_THRESH = "ds-floodscan-ingest/aer_area_300s_last_365d_sfed_gte0.01.tif"

  ret$FP_LAST90D_THRESH = "ds-floodscan-ingest/aer_area_300s_90d_sfed_example.tif"

  if(virtual_path){
   ret <-  map(
      ret,
      \(path_tmp){
        vp(path_tmp, "projects")
      }
    )
  }

  ret

}



#' virtual path (gdal)
#'
#' @param blob_path `character` directory/path of blob
#' @param container `character` name of container

#' @export
vp <- function(blob_path,container="projects"){
  paste0(
    "/vsiaz/",container,"/",blob_path
  )
}
