source("R/dqd_helpers.R")

defaults <- list(
  dbPath = "data/syntheaCDM.duckdb",
  cdmSourceName = "Synthea",
  checkSeverity = "fatal,convention,characterization",
  outputJsonPath = "dqd_output/results/synthea-clean-dqd.json",
  outputFolder = "",
  verboseMode = "true"
)

args <- parse_named_args(defaults)
check_severity <- split_csv_arg(args$checkSeverity)
if (length(check_severity) == 0) {
  stop("checkSeverity must contain at least one DQD severity.")
}

verbose_mode <- as_flag(args$verboseMode, default = TRUE)
output_folder <- args$outputFolder
if (!nzchar(output_folder)) {
  output_folder <- dirname(args$outputJsonPath)
}

results <- run_dqd(
  db_path = args$dbPath,
  cdm_source_name = args$cdmSourceName,
  check_severity = check_severity,
  output_json_path = args$outputJsonPath,
  output_folder = output_folder,
  verbose_mode = verbose_mode,
  write_to_table = FALSE
)

cat("DQD JSON written to:", args$outputJsonPath, "\n")
cat("Checks executed:", nrow(results$CheckResults), "\n")
cat("Overall failed checks:", results$Overview$countOverallFailed[[1]], "\n")
