source("R/dqd_helpers.R")
source("R/tutorial_sql_helpers.R")

defaults <- list(
  sourceDbPath = "data/syntheaCDM.duckdb",
  targetDbPath = "data/syntheaCDM_tutorial_broken.duckdb",
  logPath = "dqd_output/tutorial_issue_log.txt",
  overwrite = "true"
)

args <- parse_named_args(defaults)
overwrite <- as_flag(args$overwrite, default = TRUE)

copy_database_file(args$sourceDbPath, args$targetDbPath, overwrite = overwrite)

connection <- connect_duckdb(args$targetDbPath, read_only = FALSE)
on.exit(disconnect_duckdb(connection), add = TRUE)

DBI::dbBegin(connection)
tryCatch(
  {
    inject_tutorial_issues(connection)
    DBI::dbCommit(connection)
  },
  error = function(error) {
    DBI::dbRollback(connection)
    stop(error)
  }
)

write_tutorial_log(
  db_path = args$targetDbPath,
  log_path = args$logPath,
  title = "Tutorial issue injection summary"
)

cat("Wrote broken tutorial database to:", args$targetDbPath, "\n")
