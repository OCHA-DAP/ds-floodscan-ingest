#' script sends a message to DS-Pipelines Slack Channel Indicating Status of
#' FloodScan Pipeline GHA run

box::use(logger)
box::use(slack=../R/utils_slack)
box::use(stringr)

dry_run <- as.logical(Sys.getenv("DRY_RUN", unset = TRUE))
# job name
run_id <- "floodscan-cog-blob"
logger$log_info(paste0("Checking GitHub Actions status for ", run_id, "..."))

status <- slack$slack_build_workflow_status(run_id = run_id, include_dispatch = FALSE,branch = "main")

run_failed <- stringr$str_detect(status,"Failure")

notify_at <-  ifelse(run_failed,"<!channel>","")

header <- paste0(
  ":rotating_light: ",
  notify_at,
  format(Sys.Date(),"%e %B %Y"),
  " FloodScan Pipeline"
)

slack$slack_post_message(
  header_text = header,
  status_text= status,
  dry_run = dry_run
)

logger$log_info("Successfully posted message to Slack")
