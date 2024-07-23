

#' @export
load_paths <-function(){
  ret <- list()

  # Thresholded
  ret$FP_DOY_THRESH_COG <-  "ds-floodscan-ingest/aer_area_300s_doy_mean_thresh_gte0.01_baseline_1998_2020.tif"
  ret$FVP_DOY_THRESH_COG = vp(ret$FP_DOY_THRESH_COG )

  # thresholded and smoothed


  ret$FP_DOY_SMOOTHED30D_THRESH_COG <-  "ds-floodscan-ingest/aer_area_300s_doy_mean_thresh_gte0.01_30d_smoothed_baseline_1998_2020.tif"
  ret$FVP_DOY_SMOOTHED30D_THRESH_COG = vp(ret$FP_DOY_SMOOTHED30D_THRESH_COG )

  # no thresholds

  ret$FP_DOY_NO_THRESH <-  "ds-floodscan-ingest/aer_area_300s_doy_mean_baseline_1998_2020.tif"
  ret$FVP_DOY_NO_THRESH <-  vp(ret$FP_DOY_NO_THRESH)

  ret$FP_DOY_NO_THRESH_SMOOTHED30d = "ds-floodscan-ingest/aer_area_300s_doy_no_thresh_30d_smoothed_baseline_1998_2020.tif"
  ret$FVP_DOY_NO_THRESH_SMOOTHED30d = vp(ret$FP_DOY_NO_THRESH_SMOOTHED30d)


  ret$FP_LAST365D = "ds-floodscan-ingest/aer_area_300s_last_365d.tif"
  ret$FVP_LAST365D = vp(ret$FP_LAST365D)

  ret$FP_LAST365D_THRESH = "ds-floodscan-ingest/aer_area_300s_last_365d_sfed_gte0.01.tif"
  ret$FVP_LAST365D_THRESH = vp(ret$FP_LAST365D_THRESH)


  ret$FP_LAST90D_THRESH = "ds-floodscan-ingest/aer_area_300s_90d_sfed_example.tif"

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
