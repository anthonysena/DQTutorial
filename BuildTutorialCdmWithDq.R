source("R/dqd_helpers.R")
source("R/tutorial_sql_helpers.R")
source(file.path("etl", "etl-synthea", "schemaFix.R"))

defaults <- list(
  syntheaDir = "data/syntheaRawWithDqIssues",
  vocabDir = "data/vocabulary",
  outputDbPath = "data/syntheaCDMWithDq.duckdb",
  logPath = "dqd_output/tutorial_cdm_with_dq_log.txt",
  overwrite = "true"
)

args <- parse_named_args(defaults)
overwrite <- as_flag(args$overwrite, default = TRUE)

if (!dir.exists(args$syntheaDir)) {
  stop("Tutorial raw directory not found: ", args$syntheaDir, ". Run `Rscript PrepareRawIssueSet.R` first.")
}
if (!dir.exists(args$vocabDir)) {
  stop("Vocabulary directory not found: ", args$vocabDir)
}

if (file.exists(args$outputDbPath)) {
  if (!overwrite) {
    stop("Output database already exists: ", args$outputDbPath)
  }
  unlink(args$outputDbPath, force = TRUE)
}
ensure_parent_dir(args$outputDbPath)

library(ETLSyntheaBuilder)
library(DatabaseConnector)
library(SqlRender)

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "duckdb",
  server = normalizePath(args$outputDbPath, mustWork = FALSE)
)

cdmVersion <- "5.4"
syntheaVersion <- "3.3.0"
cdmSchema <- tutorial_cdm_schema
syntheaSchema <- tutorial_raw_schema

conn <- DatabaseConnector::connect(connectionDetails)
for (schemaName in c(cdmSchema, syntheaSchema)) {
  schemaSql <- SqlRender::render("create schema if not exists @schema;", schema = schemaName)
  schemaSql <- SqlRender::translate(schemaSql, targetDialect = connectionDetails$dbms)
  DatabaseConnector::executeSql(conn, schemaSql)
}
DatabaseConnector::disconnect(conn)

ETLSyntheaBuilder::CreateCDMTables(connectionDetails, cdmSchema, cdmVersion)
ETLSyntheaBuilder::CreateSyntheaTables(connectionDetails, syntheaSchema, syntheaVersion)
ETLSyntheaBuilder::LoadSyntheaTables(connectionDetails, syntheaSchema, args$syntheaDir)
materializeSchemaQualifiedTables(connectionDetails, syntheaSchema, syntheaSourceTables)

ETLSyntheaBuilder::LoadVocabFromCsv(connectionDetails, cdmSchema, args$vocabDir, delimiter = ",")
materializeSchemaQualifiedTables(connectionDetails, cdmSchema, cdmVocabularyTables)

ETLSyntheaBuilder::CreateMapAndRollupTables(connectionDetails, cdmSchema, syntheaSchema, cdmVersion, syntheaVersion)
ETLSyntheaBuilder::CreateExtraIndices(connectionDetails, cdmSchema, syntheaSchema, syntheaVersion)
ETLSyntheaBuilder::LoadEventTables(connectionDetails, cdmSchema, syntheaSchema, cdmVersion, syntheaVersion)
materializeSchemaQualifiedTables(connectionDetails, cdmSchema, cdmEventTables)

connection <- DatabaseConnector::connect(connectionDetails)
on.exit(DatabaseConnector::disconnect(connection), add = TRUE)

DBI::dbBegin(connection)
tryCatch(
  {
    inject_tutorial_issues(connection, cdm_schema = cdmSchema, raw_schema = syntheaSchema)
    DBI::dbCommit(connection)
  },
  error = function(error) {
    DBI::dbRollback(connection)
    stop(error)
  }
)

write_tutorial_log(
  db_path = args$outputDbPath,
  log_path = args$logPath,
  title = "Single tutorial DuckDB with raw and CDM tutorial issues"
)

DBI::dbDisconnect(connection)

cat("Wrote tutorial DuckDB to:", normalizePath(args$outputDbPath, winslash = "/", mustWork = TRUE), "\n")
