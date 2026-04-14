source("R/dqd_helpers.R")

defaults <- list(
  sourceRawDir = "data/syntheaRaw",
  targetRawDir = "data/syntheaRawWithDqIssues",
  overwrite = "true"
)

args <- parse_named_args(defaults)
overwrite <- as_flag(args$overwrite, default = TRUE)

if (!dir.exists(args$sourceRawDir)) {
  stop("Source raw directory not found: ", args$sourceRawDir)
}

if (dir.exists(args$targetRawDir)) {
  if (!overwrite) {
    stop("Target raw directory already exists: ", args$targetRawDir)
  }
  unlink(args$targetRawDir, recursive = TRUE, force = TRUE)
}

dir.create(args$targetRawDir, recursive = TRUE, showWarnings = FALSE)
copied <- file.copy(
  from = list.files(args$sourceRawDir, full.names = TRUE),
  to = args$targetRawDir,
  recursive = FALSE,
  overwrite = overwrite
)
if (!all(copied)) {
  stop("Failed to copy one or more raw CSV files into ", args$targetRawDir)
}

library(readr)

# These observation mutations mark visits that the tutorial later traces into the
# measurement ETL path. The replacement code maps to the Observation domain, so the
# post-ETL tutorial adjustment can deterministically create fkDomain on
# MEASUREMENT.MEASUREMENT_CONCEPT_ID while preserving raw-to-CDM lineage in one DB.
observations_path <- file.path(args$targetRawDir, "observations.csv")
observations <- readr::read_csv(
  observations_path,
  col_types = readr::cols(.default = readr::col_character()),
  show_col_types = FALSE
)
observation_candidates <- which(observations$CODE == "8302-2")
observation_rows <- utils::head(observation_candidates, 8)
if (length(observation_rows) < 8) {
  stop("Not enough observation rows with CODE 8302-2 to inject the fkDomain tutorial issue.")
}
observations$CODE[observation_rows] <- "72166-2"
observations$DESCRIPTION[observation_rows] <- "Tobacco smoking status [tutorial fkDomain issue]"
readr::write_csv(observations, observations_path, na = "")

# These medication mutations mark raw rows that will later receive a narrow post-ETL
# concept-id adjustment so DQD's isStandardValidConcept check fails deterministically.
medications_path <- file.path(args$targetRawDir, "medications.csv")
medications <- readr::read_csv(
  medications_path,
  col_types = readr::cols(.default = readr::col_character()),
  show_col_types = FALSE
)
medication_candidates <- which(medications$CODE == "314231")
medication_rows <- utils::head(medication_candidates, 6)
if (length(medication_rows) < 6) {
  stop("Not enough medication rows with CODE 314231 to inject the tutorial standard-validity issue.")
}
medications$CODE[medication_rows] <- "1870230"
medications$DESCRIPTION[medication_rows] <- paste0(
  "NDA020800 0.3 ML Epinephrine 1 MG/ML Auto-Injector ",
  "[tutorial standard validity issue]"
)
readr::write_csv(medications, medications_path, na = "")

cat("Prepared tutorial raw issue set at:", normalizePath(args$targetRawDir, winslash = "/", mustWork = TRUE), "\n")
