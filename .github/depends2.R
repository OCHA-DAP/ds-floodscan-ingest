#
# install.versions(
#   c("purrr",
#     "googledrive",
#     "readr"),
#   c(
#     "1.0.2",
#     "2.1.1",
#     "2.1.5"
#   )
# )
#



install.packages(
  c(
    "dplyr",
    "purrr",
    "googledrive",
    "readr",
    "stringr",
    "remotes",
    "terra"
    )
)


remotes::install_github(repo = "OCHA-DAP/cumulus",ref = "read_az_file_v1")

