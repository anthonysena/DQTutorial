source("R/dqd_helpers.R")
source("R/tutorial_sql_helpers.R")

defaults <- list(
  sourceDbPath = "data/syntheaCDM.duckdb",
  brokenDbPath = "data/syntheaCDM_tutorial_broken.duckdb",
  afterFatalDbPath = "data/syntheaCDM_tutorial_after_fatal.duckdb",
  afterConventionDbPath = "data/syntheaCDM_tutorial_after_convention.duckdb",
  afterCharacterizationDbPath = "data/syntheaCDM_tutorial_after_characterization.duckdb",
  overwrite = "true"
)

args <- parse_named_args(defaults)
overwrite <- as_flag(args$overwrite, default = TRUE)

build_broken_snapshot <- function() {
  copy_database_file(args$sourceDbPath, args$brokenDbPath, overwrite = overwrite)
  connection <- connect_duckdb(args$brokenDbPath, read_only = FALSE)
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
    db_path = args$brokenDbPath,
    log_path = "dqd_output/tutorial_issue_log.txt",
    title = "Broken tutorial snapshot"
  )
}

build_fixed_snapshot <- function(source_db_path, target_db_path, fix_fun, log_path, title, stage) {
  copy_database_file(source_db_path, target_db_path, overwrite = overwrite)
  connection <- connect_duckdb(target_db_path, read_only = FALSE)
  on.exit(disconnect_duckdb(connection), add = TRUE)

  DBI::dbBegin(connection)
  tryCatch(
    {
      fix_fun(connection, stage = stage)
      DBI::dbCommit(connection)
    },
    error = function(error) {
      DBI::dbRollback(connection)
      stop(error)
    }
  )

  write_tutorial_log(
    db_path = target_db_path,
    log_path = log_path,
    title = title
  )
}

build_broken_snapshot()
build_fixed_snapshot(
  source_db_path = args$brokenDbPath,
  target_db_path = args$afterFatalDbPath,
  fix_fun = apply_fatal_fixes,
  log_path = "dqd_output/tutorial_after_fatal_log.txt",
  title = "Tutorial snapshot after fatal fixes",
  stage = "fatal"
)
build_fixed_snapshot(
  source_db_path = args$afterFatalDbPath,
  target_db_path = args$afterConventionDbPath,
  fix_fun = apply_convention_fixes,
  log_path = "dqd_output/tutorial_after_convention_log.txt",
  title = "Tutorial snapshot after convention fixes",
  stage = "convention"
)
build_fixed_snapshot(
  source_db_path = args$afterConventionDbPath,
  target_db_path = args$afterCharacterizationDbPath,
  fix_fun = apply_characterization_fixes,
  log_path = "dqd_output/tutorial_after_characterization_log.txt",
  title = "Tutorial snapshot after characterization fixes",
  stage = "characterization"
)

cat("Prepared tutorial snapshots.\n")
