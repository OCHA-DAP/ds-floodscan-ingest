box::use(jsonlite)
box::use(httr2)
box::use(purrr)
box::use(logger)
box::use(stringr)
box::use(dplyr)


#' @export
slack_post_message <- function(
    header_text,
    status_text,
    dry_run = TRUE

) {
  slack_url <-  ifelse(
    dry_run,
    Sys.getenv("DS_PIPELINES_TEST_SLACK"),
    # temporarily make irrelevant and just use TEST webhook no matter what
    Sys.getenv("DS_PIPELINES_TEST_SLACK")
    )
  # See https://app.slack.com/block-kit-builder for prototyping layouts in JSON
  msg <- list(
    blocks = list(
      list(
        type = "section",
        text = list(
          type = "mrkdwn",
          text = header_text
        )
      ),
      list(type = "divider"),
      list(
        type = "context",
        elements = list(
          list(
            type = "mrkdwn",
            text = status_text
          )
        )
      ),
      list(type = "divider")
    )
  )

  # Create and perform the POST request using httr2
  response <- httr2$request(slack_url) |>
    httr2$req_body_json(msg) |>
    httr2$req_perform()

  if (response$status_code != 200) {
    stop("Error posting Slack message")
  }
}


#' Builds the header text, depending on how many signals are reported
#'
#'
#' @returns String header text
#' @export
slack_build_header <- function() {
    paste0(
      ":rotating_light: <!channel> ",
      format(Sys.Date(),"%e %B %Y"),
      " FloodScan Pipeline"
    )
  }



#' Takes the response from a GitHub Actions run of a single indicator
#' and outputs a status message to be posted to Slack
#'
#' @param run_id ID of the indicator
#'
#' @returns String status message to be posted to Slack
#' @export
slack_build_workflow_status <- function(run_id) {
  df_runs <- tryCatch({
    df_runs <- query_github(run_id = run_id )
  },
  error = function(e) {
    logger$log_error(e$message)
    e$message
  })

  if (is.character(df_runs)) {
    return(paste0(
      ":red_circle: ",
      run_id,
      ": Failed request for workflow status -",
      df_runs,
      "\n"
    ))
  }
  # Get today's scheduled runs from the main branch
  df_runs$date <- as.Date(df_runs$workflow_runs.created_at)
  df_sel <- dplyr$filter(
    df_runs,
    workflow_runs.event == "schedule",
    workflow_runs.head_branch == "main",
    date == Sys.Date()
  )

  if (nrow(df_sel) == 1) {
    status <- df_sel$workflow_runs.conclusion
    if (status == "failure") {
      base_logs_url <- "https://github.com/ocha-dap/ds-floodscan-ingest/actions/runs/"
      run_id <- df_sel$workflow_runs.id
      run_link <- paste0(base_logs_url, run_id)
      paste0(":red_circle: ", run_id, ": Failed update - <", run_link, "|Check logs> \n")
    } else if (status == "success") {
      paste0(":large_green_circle: ", run_id, ": Successful update \n")
    }
    # If no scheduled runs happened off of main today
  } else if (nrow(df_sel) == 0) {
    paste0(":heavy_minus_sign: ", run_id, ": No scheduled update \n")
  } else {
    paste0(":red_circle: ", run_id, ": More than one scheduled run today \n")
  }
}

#' Returns a DataFrame of all workflow runs on GitHub actions
#' for monitoring a given indicator
#'
#' @param run_id ID of the indicator
#'
#' @returns DataFrame with metadata for all runs

query_github <- function(run_id){
  httr2$request(
    "https://api.github.com/repos/ocha-dap/ds-floodscan-ingest/actions/workflows"
  ) |>
    httr2$req_url_path_append(
      paste0(run_id,".yml"),
      "runs"
    ) |>
    httr2$req_auth_bearer_token(
      token = Sys.getenv("GH_TOKEN")
    ) |>
    httr2$req_perform() |>
    httr2$resp_body_string() |>
    jsonlite$fromJSON(flatten = TRUE) |>
    as.data.frame()
}
