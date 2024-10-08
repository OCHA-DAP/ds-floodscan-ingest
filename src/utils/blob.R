box::use(AzureStor)
box::use(rlang)
box::use(purrr)
box::use(glue)
box::use(stringr)
box::use(dplyr)
box::use(logger)
box::use(glue)
box::use(forcats)
box::use(tidyr)


#' blob_date_gaps
#'
#' @param run_date `date` date of run (default = Sys.Date())
#' @param test `logical` if TRUE artificially injects gaps into dates so that
#'     user can test functionality. By default set tot FALSE (no gaps as of
#'     2024-07-19)
#'
#' @return `character` vector of COG blob paths IF any blobs are missing from
#'     the last 90 days. If blobs >= 90d days old (should be impossible) it
#'     logs informative message.
#'
#' @export
#'
#' @examples
#' blob_date_gaps(test=TRUE)
#' blob_date_gaps(test=FALSE)
blob_date_gaps <- function(run_date = Sys.Date(),test=FALSE){

  most_recent_fs_update <- run_date - 1
  last_90d <- seq(most_recent_fs_update-90,most_recent_fs_update, by="day")

  cog_container <-  load_containers(containers = "global")$GLOBAL_CONT

  blob_names <- AzureStor$list_blobs(
    container = cog_container,
    prefix = "raster/cogs/aer_area_300s"
  )

  df_blob_names <- blob_names |>
    dplyr$mutate(
      date = extract_date(name)
    )

  df_blob_names <- df_blob_names |>
      tidyr$complete(
        date= last_90d
      )


  if(test){
    df_blob_names <-  df_blob_names |>
      dplyr$mutate(
        date = dplyr$if_else(date==as.Date("2000-01-01"),as.Date("2000-01-02"),date),
        date = dplyr$if_else(date==as.Date("2024-07-04"),as.Date("2024-07-05"),date)
      )
  }

  df_date_gap <- df_blob_names |>
    dplyr$arrange(date) |>
    dplyr$mutate(
      in_last_90d = date %in% last_90d,
      date_gap = (date - dplyr$lag(x = date, n = 1 )) > 1,
      date_gap = dplyr$if_else(is.na(date_gap), FALSE,date_gap),
      missing_recent = is.na(name)
    ) |>
    dplyr$filter(
      date_gap|missing_recent
    ) |>
    dplyr$mutate(
      type_missing = dplyr$case_when(
        in_last_90d == FALSE ~ "old_gap",
        in_last_90d == TRUE & missing_recent == FALSE ~ "new_gap",
        missing_recent == TRUE ~ "missing_recent_update"
      ),
      type_missing = forcats$fct_expand(type_missing, c("old_gap","new_gap","missing_recent_update"))
    )

  ldf_missing <- split(df_date_gap,df_date_gap$type_missing)

  ret_list <- dplyr$lst()

  if( nrow(ldf_missing$old_gap)>0 ){
    old_tifs_missing <- glue$glue_collapse(basename(ldf_missing$old_gap$name),sep ="\n")
    logger$log_info("REVIEW PIPELINE: the following tifs are missing from blob:\n{old_tifs_missing}")
  }
  if( nrow(ldf_missing$new_gap)>0 ){
    tifs_90d_missing <- glue$glue_collapse(basename(ldf_missing$new_gap$name),sep ="\n")
    logger$log_info("The following tifs are missing from blob and will be backfilled by current zip:\n{tifs_90d_missing}")
    new_gaps_cog_names <- ldf_missing$new_gap$name
    new_gaps_dates <- ldf_missing$new_gap$date
    ret_list <- append(ret_list, as.character(new_gaps_dates))
  }
  if(nrow(ldf_missing$missing_recent_update)>0){
    last_update <- min(ldf_missing$missing_recent_update$date) -1
    logger$log_info("no update since {last_update}. Will backfill up to latest available date in zip")
    # recent_missing_cog_names <- paste0("raster/cogs/aer_area_300s_",format(ldf_missing$missing_recent_update$date,"%Y%m%d"),"_v05r01.tif")
    recent_missing_dates <- ldf_missing$missing_recent_update$date
    ret_list <- append(ret_list, as.character(recent_missing_dates))

  }
  ret <- as.Date(unlist(ret_list))
  if(length(ret)==0){
    logger$log_info("No missing blobs found")
    ret <- NULL
  }
  ret
}



#' Return blob container
#'
#' Create endpoint URL to access Azure blob or file storage on either the
#' `dev` or `prod` stage from specified storage account
#' @param containers `character` vector containing the name of container to
#'     load (default, "global", "projects")
#' @param sas_key Shared access signature key to access the storage account or
#'    blob. Default is set to use  dev stage sas key via an a env var named
#'    "DSCI_AZ_SAS_DEV"
#' @param service Service to access, either `blob` (default) or `file.`
#' @param stage Store to access, either `prod` (default) or `dev`. `dev`
#' @param storage_account Storage account to access. Default is `imb0chd0`
#' @examples
#' # load project containers
#' pc <- load_containers(containers = "projects")
#' AzureStor::list_blobs(
#'   container = pc$PROJECTS_CONT,
#'   dir = "ds-contingency-pak-floods"
#' )
#'
#' # You can also list as many containers as you want.
#' pc <- load_containers(containers = c("global", "projects"))
#' AzureStor::list_blobs(
#'   container = pc$GLOBAL_CONT,
#'   dir = "raster/cogs"
#' )
#' @export
load_containers <- function(
    containers = c("global", "projects"),
    sas = Sys.getenv("DSCI_AZ_SAS_DEV"),
    service = c("blob", "file"),
    stage = c("dev", "prod"),
    storage_account = "imb0chd0") {

  ep_url <- azure_endpoint_url(
    service = service,
    stage = stage,
    storage_account = storage_account
  )

  se <- AzureStor$storage_endpoint(ep_url, sas = sas)

  # storage container
  item_labels <- paste0(toupper(containers), "_CONT")
  containers <- rlang$set_names(containers, item_labels)

  l_containers <- purrr$map(containers, \(container_name){
    AzureStor$storage_container(se, container_name)
  })

  l_containers
}

#' Create the endpoint URL
#'
#' Create endpoint URL to access Azure blob or file storage on either the
#' `dev` or `prod` stage from specified storage account
#'
#' @param service Service to access, either `blob` (default) or `file.`
#' @param stage Store to access, either `prod` (default) or `dev`. `dev`
#' @param storage_account Storage account to access. Default is `imb0chd0`
#' @export
azure_endpoint_url <- function(
    service = c("blob", "file"),
    stage = c("dev", "prod"),
    storage_account = "imb0chd0") {
  blob_url <- "https://{storage_account}{stage}.{service}.core.windows.net/"
  service <- rlang$arg_match(service)
  stage <- rlang$arg_match(stage)
  storae_account <- rlang$arg_match(storage_account)
  endpoint <- glue$glue(blob_url)
  return(endpoint)
}


#' extract_date
#'
#' @param x
#'
#' @return
#' @export
extract_date <-  function(x){

  yyyymmdd = stringr$str_extract(
    x,
    pattern = "\\d{8}"
  )

  as.Date(yyyymmdd,format = "%Y%m%d")
}
