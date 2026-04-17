getDqdChecksBySeverityLevel <- function(severityLevel, cdmVersion = "5.4") {
  severityLevelChecks <- c("fatal", "convention", "characterization")
  if (!severityLevel %in% severityLevelChecks) {
    stop(paste("Invalid severityLevel must be one of:", paste(severityLevelChecks, collapse = ", ")))
  }

  dqChecks <- DataQualityDashboard::listDqChecks(
    cdmVersion = cdmVersion
  )
  filteredChecks <- dqChecks$checkDescriptions |>
    dplyr::filter(.data$severity == severityLevel ) |>
    dplyr::select(checkName) |>
    dplyr::pull()

  # Adding required checks for DQ
  filteredChecks <- c(
    filteredChecks,
    c("cdmTable", "cdmField", "measureValueCompleteness")
  )
  filteredChecks <- unique(filteredChecks)
  return(filteredChecks)
}

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "duckdb",
  server = "data/syntheaCDMWithDq.duckdb"
)

severityLevelChecks <- c("fatal", "convention", "characterization")
for (severityLevel in severityLevelChecks) {
  cli::cli_alert_info(severityLevel)
  checkNames <- getDqdChecksBySeverityLevel(severityLevel)

  # NOTES: 1) The resultsDatabaseSchema should be set to the CDM
  # schema since it will check if the cohort table exists.
  # 2) The checkSeverity doesn't work as intended b/c it messes up the
  #    dashboard so for now we'll use checkNames as a way to run by
  #    severity level
  DataQualityDashboard::executeDqChecks(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "cdm",
    resultsDatabaseSchema = "cdm",
    cdmSourceName = "DqTutorial",
    numThreads = 1,
    sqlOnly = FALSE,
    outputFolder = getwd(),
    outputFile = paste0(severityLevel, ".json"),
    verboseMode = TRUE,
    writeToTable = FALSE,
    checkLevels = c("TABLE", "FIELD", "CONCEPT"),
    checkNames = checkNames,
    cdmVersion = "5.4"
  )
}

json_path <- "fatal.json"
json_path <- "convention.json"
json_path <- "characterization.json"
DataQualityDashboard::viewDqDashboard(
  jsonPath = normalizePath(json_path, winslash = "/", mustWork = TRUE)
)
