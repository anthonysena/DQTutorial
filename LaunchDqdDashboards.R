source("R/dqd_helpers.R")

defaults <- list(
  jsonPaths = paste(
    c(
      "dqd_output/results/synthea-clean-dqd.json",
      "dqd_output/results/synthea-tutorial-fatal.json",
      "dqd_output/results/synthea-tutorial-convention.json",
      "dqd_output/results/synthea-tutorial-characterization.json"
    ),
    collapse = ","
  )
)

args <- parse_named_args(defaults)
json_paths <- split_csv_arg(args$jsonPaths)

for (json_path in json_paths) {
  if (!file.exists(json_path)) {
    warning("Skipping missing JSON file: ", json_path)
    next
  }

  DataQualityDashboard::viewDqDashboard(
    jsonPath = normalizePath(json_path, winslash = "/", mustWork = TRUE)
  )
}

json_path <- "dqd_output/results/synthea-tutorial-fatal.json"
DataQualityDashboard::viewDqDashboard(
  jsonPath = normalizePath(json_path, winslash = "/", mustWork = TRUE)
)
