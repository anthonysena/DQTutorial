parse_named_args <- function(defaults = list()) {
  args <- commandArgs(trailingOnly = TRUE)
  parsed <- defaults

  for (arg in args) {
    if (!grepl("=", arg, fixed = TRUE)) {
      next
    }

    pieces <- strsplit(arg, "=", fixed = TRUE)[[1]]
    key <- pieces[[1]]
    value <- paste(pieces[-1], collapse = "=")
    parsed[[key]] <- value
  }

  parsed
}

as_flag <- function(value, default = FALSE) {
  if (is.null(value) || length(value) == 0 || identical(value, "")) {
    return(default)
  }

  tolower(as.character(value)[1]) %in% c("true", "t", "1", "yes", "y")
}

split_csv_arg <- function(value) {
  if (is.null(value) || !nzchar(value)) {
    return(character())
  }

  parts <- trimws(strsplit(value, ",", fixed = TRUE)[[1]])
  parts[nzchar(parts)]
}

ensure_dir <- function(path) {
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE, showWarnings = FALSE)
  }
}

ensure_parent_dir <- function(path) {
  ensure_dir(dirname(path))
}

copy_database_file <- function(source, target, overwrite = TRUE) {
  if (!file.exists(source)) {
    stop("Source database does not exist: ", source)
  }

  if (file.exists(target)) {
    if (!overwrite) {
      stop("Target database already exists: ", target)
    }
    unlink(target, force = TRUE)
  }

  ensure_parent_dir(target)

  copied <- file.copy(from = source, to = target, overwrite = overwrite)
  if (!copied) {
    stop("Failed to copy database from ", source, " to ", target)
  }

  invisible(normalizePath(target, winslash = "/", mustWork = TRUE))
}

connect_duckdb <- function(db_path, read_only = FALSE) {
  DBI::dbConnect(
    drv = duckdb::duckdb(),
    dbdir = db_path,
    read_only = read_only
  )
}

disconnect_duckdb <- function(connection) {
  if (!is.null(connection)) {
    DBI::dbDisconnect(connection, shutdown = TRUE)
  }
}

sql_literal <- function(value) {
  if (length(value) == 0 || is.na(value)) {
    return("NULL")
  }

  if (inherits(value, "POSIXt")) {
    text_value <- format(value, "%Y-%m-%d %H:%M:%S")
  } else if (inherits(value, "Date")) {
    text_value <- format(value, "%Y-%m-%d")
  } else {
    text_value <- as.character(value)
  }

  escaped <- gsub("'", "''", text_value, fixed = TRUE)

  if (is.numeric(value) && !is.factor(value)) {
    escaped
  } else {
    paste0("'", escaped, "'")
  }
}

run_single_dqd <- function(db_path,
                           cdm_source_name,
                           check_severity,
                           output_json_path,
                           output_folder = NULL,
                           verbose_mode = TRUE,
                           write_to_table = FALSE) {
  if (is.null(output_folder) || !nzchar(output_folder)) {
    output_folder <- dirname(output_json_path)
  }

  ensure_dir(output_folder)
  ensure_parent_dir(output_json_path)

  connection_details <- DatabaseConnector::createConnectionDetails(
    dbms = "duckdb",
    server = db_path
  )

  DataQualityDashboard::executeDqChecks(
    connectionDetails = connection_details,
    cdmDatabaseSchema = "main",
    resultsDatabaseSchema = "main",
    cdmSourceName = cdm_source_name,
    numThreads = 1,
    sqlOnly = FALSE,
    outputFolder = output_folder,
    outputFile = basename(output_json_path),
    verboseMode = verbose_mode,
    writeToTable = write_to_table,
    writeTableName = "dqddashboard_results",
    checkLevels = c("TABLE", "FIELD", "CONCEPT"),
    checkSeverity = check_severity,
    cdmVersion = "5.4"
  )
}

combine_dqd_results <- function(results_list) {
  check_results <- do.call(
    rbind,
    lapply(results_list, function(result) result$CheckResults)
  )

  start_times <- as.POSIXct(
    vapply(results_list, function(result) as.character(result$startTimestamp[[1]]), character(1)),
    tz = "UTC"
  )
  end_times <- as.POSIXct(
    vapply(results_list, function(result) as.character(result$endTimestamp[[1]]), character(1)),
    tz = "UTC"
  )

  start_time <- min(start_times)
  end_time <- max(end_times)
  delta <- difftime(end_time, start_time, units = "secs")

  list(
    startTimestamp = start_time,
    endTimestamp = end_time,
    executionTime = sprintf("%.0f secs", as.numeric(delta)),
    executionTimeSeconds = as.numeric(delta),
    CheckResults = check_results,
    Metadata = results_list[[1]]$Metadata,
    Overview = DataQualityDashboard:::.summarizeResults(check_results)
  )
}

run_dqd <- function(db_path,
                    cdm_source_name,
                    check_severity,
                    output_json_path,
                    output_folder = NULL,
                    verbose_mode = TRUE,
                    write_to_table = FALSE) {
  if (length(check_severity) <= 1) {
    return(run_single_dqd(
      db_path = db_path,
      cdm_source_name = cdm_source_name,
      check_severity = check_severity,
      output_json_path = output_json_path,
      output_folder = output_folder,
      verbose_mode = verbose_mode,
      write_to_table = write_to_table
    ))
  }

  if (is.null(output_folder) || !nzchar(output_folder)) {
    output_folder <- dirname(output_json_path)
  }

  ensure_dir(output_folder)
  tmp_dir <- tempfile(pattern = "dqd-severity-", tmpdir = output_folder)
  ensure_dir(tmp_dir)
  on.exit(unlink(tmp_dir, recursive = TRUE, force = TRUE), add = TRUE)

  results_list <- lapply(check_severity, function(severity) {
    run_single_dqd(
      db_path = db_path,
      cdm_source_name = cdm_source_name,
      check_severity = severity,
      output_json_path = file.path(tmp_dir, paste0(severity, ".json")),
      output_folder = tmp_dir,
      verbose_mode = verbose_mode,
      write_to_table = FALSE
    )
  })

  combined_results <- combine_dqd_results(results_list)
  DataQualityDashboard:::.writeResultsToJson(
    combined_results,
    output_folder,
    basename(output_json_path)
  )
  combined_results
}

read_dqd_json <- function(json_path) {
  jsonlite::fromJSON(json_path)
}

filter_dqd_results <- function(results, check_names = character()) {
  check_results <- results$CheckResults
  if (is.null(check_results) || nrow(check_results) == 0) {
    return(check_results)
  }

  if (length(check_names) == 0) {
    return(check_results)
  }

  check_results[check_results$checkName %in% check_names, , drop = FALSE]
}

write_lines_file <- function(lines, path) {
  ensure_parent_dir(path)
  writeLines(lines, con = path, useBytes = TRUE)
}
