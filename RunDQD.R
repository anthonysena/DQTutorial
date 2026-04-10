library(DataQualityDashboard)

# Inspect the checks - returns a list of 
# items for the checkDescriptions, etc
# dqChecks <- DataQualityDashboard::listDqChecks(
#   cdmVersion = "5.4"
# )

# Connect to the Synthea CDM
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "duckdb",
  server = "data/syntheaCDM.duckdb"
)

# Some code to test the connection 
# connection <- DatabaseConnector::connect(
#   connectionDetails = connectionDetails
# )

# DatabaseConnector::querySql(
#   connection = connection,
#   sql = "SELECT * FROM main.cdm_source;"
# )

# DatabaseConnector::disconnect(connection = connection)
DataQualityDashboard::executeDqChecks(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "main",
  resultsDatabaseSchema = "main",
  cdmSourceName = "Synthea",
  numThreads = 1,
  sqlOnly = FALSE,
  outputFolder = "dqd_output",
  verboseMode = TRUE,
  writeToTable = TRUE,
  writeTableName = "dqddashboard_results",
  checkLevels = c("TABLE", "FIELD", "CONCEPT"),
  checkSeverity = c("fatal", "convention", "characterization"),
  cdmVersion = "5.4"
)

writeDqdResultsToFileSystem <- function(connectionDetails, outputFolder, outputFile) {
  connection = DatabaseConnector::connect(
    connectionDetails = connectionDetails
  )
  on.exit(DatabaseConnector::disconnect(connection))

  outputFolder <- normalizePath(
    path = outputFolder
  )
  if (!dir.exists(outputFolder)) {
    dir.create(outputFolder)
  }
  DataQualityDashboard::writeDBResultsToJson(
    connection = connection,
    resultsDatabaseSchema = "main",
    cdmDatabaseSchema = "main",
    writeTableName = "dqddashboard_results",
    outputFolder = outputFolder,
    outputFile = outputFile
  )
}

writeDqdResultsToFileSystem(
  connectionDetails = connectionDetails,
  outputFolder = "./dqd_output/results",
  outputFile = "synthea-full-dqd.json"
)

DataQualityDashboard::viewDqDashboard(
  jsonPath = normalizePath("./dqd_output/results/synthea-full-dqd.json"),
)
