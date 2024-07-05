#' script sends a message to DS-Pipelines Slack Channel Indicating Status of
#' FloodScan Pipeline GHA run

box::use(logger)
box::use(slack=../R/utils_slack)

# job name
run_id <- "floodscan-cog-blob"
logger$log_info(paste0("Checking GitHub Actions status for ", run_id, "..."))

status <- slack$slack_build_workflow_status(run_id = run_id)
header <- slack$slack_build_header()

slack$slack_post_message(
  header_text = header,
  status_text= status,
  dry_run = TRUE
)

logger$log_info("Successfully posted message to Slack")
