
install.packages(
  c(
    "box",
    "dplyr",
    "purrr",
    "googledrive",
    "readr",
    "stringr",
    "remotes",
    "terra",
    "logger",
    "httr2",
    "jsonlite"
    )
)



remotes::install_github(repo = "OCHA-DAP/cumulus",ref = "read_az_file_v1")
