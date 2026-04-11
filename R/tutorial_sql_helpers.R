if (file.exists("R/dqd_helpers.R")) {
  source("R/dqd_helpers.R")
}

table_id <- function(name) {
  DBI::Id(schema = "main", table = name)
}

tutorial_issue_catalog <- data.frame(
  issue_code = c(
    "fatal_duplicate_measurement_pk",
    "fatal_observation_period_overlap",
    "convention_procedure_person_completeness",
    "convention_condition_era_completeness",
    "characterization_observation_within_visit_dates",
    "characterization_drug_start_before_end"
  ),
  severity = c(
    "fatal",
    "fatal",
    "convention",
    "convention",
    "characterization",
    "characterization"
  ),
  target_check = c(
    "isPrimaryKey",
    "measureObservationPeriodOverlap",
    "measurePersonCompleteness",
    "measureConditionEraCompleteness",
    "withinVisitDates",
    "plausibleStartBeforeEnd"
  ),
  table_name = c(
    "MEASUREMENT",
    "OBSERVATION_PERIOD",
    "PROCEDURE_OCCURRENCE",
    "CONDITION_ERA",
    "OBSERVATION",
    "DRUG_EXPOSURE"
  ),
  stringsAsFactors = FALSE
)

initialize_tutorial_tables <- function(connection) {
  DBI::dbExecute(connection, "
    CREATE OR REPLACE TABLE main.tutorial_issue_registry (
      issue_code VARCHAR,
      severity VARCHAR,
      target_check VARCHAR,
      table_name VARCHAR,
      row_identifier VARCHAR,
      mutation_summary VARCHAR
    )
  ")

  DBI::dbExecute(connection, "
    CREATE OR REPLACE TABLE main.tutorial_fix_registry (
      stage VARCHAR,
      issue_code VARCHAR,
      table_name VARCHAR,
      row_identifier VARCHAR,
      fix_summary VARCHAR,
      applied_timestamp TIMESTAMP
    )
  ")
}

append_table <- function(connection, name, data) {
  if (nrow(data) == 0) {
    return(invisible(NULL))
  }

  if (DBI::dbExistsTable(connection, table_id(name))) {
    DBI::dbAppendTable(connection, table_id(name), data)
  } else {
    DBI::dbWriteTable(connection, table_id(name), data, overwrite = TRUE)
  }
}

append_issue_registry <- function(connection,
                                  issue_code,
                                  row_identifiers,
                                  mutation_summary) {
  issue_meta <- tutorial_issue_catalog[tutorial_issue_catalog$issue_code == issue_code, ]
  registry <- data.frame(
    issue_code = issue_code,
    severity = issue_meta$severity,
    target_check = issue_meta$target_check,
    table_name = issue_meta$table_name,
    row_identifier = row_identifiers,
    mutation_summary = mutation_summary,
    stringsAsFactors = FALSE
  )

  append_table(connection, "tutorial_issue_registry", registry)
}

append_fix_registry <- function(connection,
                                stage,
                                issue_code,
                                table_name,
                                row_identifiers,
                                fix_summary) {
  fix_registry <- data.frame(
    stage = stage,
    issue_code = issue_code,
    table_name = table_name,
    row_identifier = row_identifiers,
    fix_summary = fix_summary,
    applied_timestamp = as.POSIXct(Sys.time(), tz = "UTC"),
    stringsAsFactors = FALSE
  )

  append_table(connection, "tutorial_fix_registry", fix_registry)
}

get_candidate_scalar <- function(connection, sql, column_name) {
  result <- DBI::dbGetQuery(connection, sql)
  if (nrow(result) == 0) {
    stop("No rows returned for candidate selection query: ", sql)
  }

  result[[column_name]][1]
}

build_row_predicate <- function(row, columns, table_alias = NULL) {
  qualifiers <- vapply(columns, function(column) {
    reference <- if (is.null(table_alias)) column else paste0(table_alias, ".", column)
    value <- row[[column]][1]
    if (is.na(value)) {
      paste(reference, "IS NULL")
    } else {
      paste(reference, "=", sql_literal(value))
    }
  }, character(1))

  paste(qualifiers, collapse = " AND ")
}

inject_duplicate_measurement_pk <- function(connection) {
  ids <- DBI::dbGetQuery(
    connection,
    "SELECT measurement_id FROM main.measurement ORDER BY measurement_id LIMIT 2"
  )$measurement_id

  if (length(ids) < 2) {
    stop("Not enough measurement rows to create duplicate primary key issue.")
  }

  original_row <- DBI::dbGetQuery(
    connection,
    sprintf("SELECT * FROM main.measurement WHERE measurement_id = %s", ids[2])
  )
  original_row$issue_code <- "fatal_duplicate_measurement_pk"
  original_row$duplicate_target_id <- ids[1]
  append_table(connection, "tutorial_backup_measurement_pk", original_row)

  DBI::dbExecute(
    connection,
    sprintf(
      "UPDATE main.measurement SET measurement_id = %s WHERE measurement_id = %s",
      ids[1],
      ids[2]
    )
  )

  append_issue_registry(
    connection = connection,
    issue_code = "fatal_duplicate_measurement_pk",
    row_identifiers = sprintf("measurement_id=%s", ids[2]),
    mutation_summary = sprintf(
      "Updated measurement_id %s to duplicate existing measurement_id %s.",
      ids[2],
      ids[1]
    )
  )
}

inject_observation_period_overlap <- function(connection) {
  candidate <- DBI::dbGetQuery(
    connection,
    "
    WITH one_period_person AS (
      SELECT person_id
      FROM main.observation_period
      GROUP BY person_id
      HAVING COUNT(*) = 1
      ORDER BY person_id
      LIMIT 1
    )
    SELECT *
    FROM main.observation_period
    WHERE person_id IN (SELECT person_id FROM one_period_person)
    "
  )

  if (nrow(candidate) != 1) {
    stop("Could not find an eligible observation_period row for overlap injection.")
  }

  new_row <- candidate
  new_row$observation_period_id <- get_candidate_scalar(
    connection,
    "SELECT COALESCE(MAX(observation_period_id), 0) + 1 AS next_id FROM main.observation_period",
    "next_id"
  )
  append_table(connection, "tutorial_backup_observation_period_overlap", new_row)
  DBI::dbAppendTable(connection, table_id("observation_period"), new_row)

  append_issue_registry(
    connection = connection,
    issue_code = "fatal_observation_period_overlap",
    row_identifiers = sprintf("person_id=%s", candidate$person_id[1]),
    mutation_summary = sprintf(
      "Inserted overlapping observation_period_id %s for person_id %s.",
      new_row$observation_period_id[1],
      candidate$person_id[1]
    )
  )
}

inject_procedure_person_completeness <- function(connection) {
  persons <- DBI::dbGetQuery(
    connection,
    "
    SELECT person_id
    FROM main.procedure_occurrence
    GROUP BY person_id
    ORDER BY person_id
    LIMIT 8
    "
  )$person_id

  if (length(persons) < 8) {
    stop("Not enough procedure_occurrence person candidates for completeness issue.")
  }

  rows <- DBI::dbGetQuery(
    connection,
    sprintf(
      "SELECT * FROM main.procedure_occurrence WHERE person_id IN (%s)",
      paste(persons, collapse = ", ")
    )
  )
  rows$issue_code <- "convention_procedure_person_completeness"
  append_table(connection, "tutorial_backup_procedure_occurrence", rows)

  DBI::dbExecute(
    connection,
    sprintf(
      "DELETE FROM main.procedure_occurrence WHERE person_id IN (%s)",
      paste(persons, collapse = ", ")
    )
  )

  append_issue_registry(
    connection = connection,
    issue_code = "convention_procedure_person_completeness",
    row_identifiers = sprintf("person_id=%s", persons),
    mutation_summary = "Deleted all procedure_occurrence rows for selected persons."
  )
}

inject_condition_era_completeness <- function(connection) {
  person_id <- get_candidate_scalar(
    connection,
    "
    SELECT ce.person_id
    FROM main.condition_era ce
    INNER JOIN main.condition_occurrence co
      ON ce.person_id = co.person_id
    GROUP BY ce.person_id
    ORDER BY ce.person_id
    LIMIT 1
    ",
    "person_id"
  )

  rows <- DBI::dbGetQuery(
    connection,
    sprintf("SELECT * FROM main.condition_era WHERE person_id = %s", person_id)
  )
  rows$issue_code <- "convention_condition_era_completeness"
  append_table(connection, "tutorial_backup_condition_era", rows)

  DBI::dbExecute(
    connection,
    sprintf("DELETE FROM main.condition_era WHERE person_id = %s", person_id)
  )

  append_issue_registry(
    connection = connection,
    issue_code = "convention_condition_era_completeness",
    row_identifiers = sprintf("person_id=%s", person_id),
    mutation_summary = "Deleted all condition_era rows for one person with condition_occurrence history."
  )
}

inject_observation_within_visit_dates <- function(connection) {
  rows <- DBI::dbGetQuery(
    connection,
    "
    SELECT
      o.observation_id,
      o.visit_occurrence_id,
      o.observation_date,
      v.visit_end_date
    FROM main.observation o
    INNER JOIN main.visit_occurrence v
      ON o.visit_occurrence_id = v.visit_occurrence_id
    WHERE o.visit_occurrence_id IS NOT NULL
    ORDER BY o.observation_id
    LIMIT 10
    "
  )

  if (nrow(rows) < 10) {
    stop("Not enough observation rows with visit_occurrence_id for withinVisitDates issue.")
  }

  backup <- rows
  backup$issue_code <- "characterization_observation_within_visit_dates"
  backup$new_observation_date <- as.Date(backup$visit_end_date) + 30
  append_table(connection, "tutorial_backup_observation_dates", backup)

  for (row_index in seq_len(nrow(backup))) {
    DBI::dbExecute(
      connection,
      sprintf(
        "UPDATE main.observation SET observation_date = %s WHERE observation_id = %s",
        sql_literal(backup$new_observation_date[row_index]),
        backup$observation_id[row_index]
      )
    )
  }

  append_issue_registry(
    connection = connection,
    issue_code = "characterization_observation_within_visit_dates",
    row_identifiers = sprintf("observation_id=%s", backup$observation_id),
    mutation_summary = "Shifted observation_date outside the associated visit window."
  )
}

inject_drug_start_before_end <- function(connection) {
  rows <- DBI::dbGetQuery(
    connection,
    "
    SELECT
      drug_exposure_id,
      drug_exposure_start_date,
      drug_exposure_end_date
    FROM main.drug_exposure
    WHERE drug_exposure_end_date IS NOT NULL
    ORDER BY drug_exposure_id
    LIMIT 10
    "
  )

  if (nrow(rows) < 10) {
    stop("Not enough drug_exposure rows for plausibleStartBeforeEnd issue.")
  }

  backup <- rows
  backup$issue_code <- "characterization_drug_start_before_end"
  backup$new_drug_exposure_end_date <- as.Date(backup$drug_exposure_start_date) - 30
  append_table(connection, "tutorial_backup_drug_exposure_dates", backup)

  for (row_index in seq_len(nrow(backup))) {
    DBI::dbExecute(
      connection,
      sprintf(
        "UPDATE main.drug_exposure SET drug_exposure_end_date = %s WHERE drug_exposure_id = %s",
        sql_literal(backup$new_drug_exposure_end_date[row_index]),
        backup$drug_exposure_id[row_index]
      )
    )
  }

  append_issue_registry(
    connection = connection,
    issue_code = "characterization_drug_start_before_end",
    row_identifiers = sprintf("drug_exposure_id=%s", backup$drug_exposure_id),
    mutation_summary = "Set drug_exposure_end_date to occur before drug_exposure_start_date."
  )
}

inject_tutorial_issues <- function(connection) {
  initialize_tutorial_tables(connection)
  inject_duplicate_measurement_pk(connection)
  inject_observation_period_overlap(connection)
  inject_procedure_person_completeness(connection)
  inject_condition_era_completeness(connection)
  inject_observation_within_visit_dates(connection)
  inject_drug_start_before_end(connection)
}

apply_fatal_fixes <- function(connection, stage = "fatal") {
  measurement_backup <- DBI::dbReadTable(connection, table_id("tutorial_backup_measurement_pk"))
  if (nrow(measurement_backup) > 0) {
    row <- measurement_backup[1, , drop = FALSE]
    match_columns <- setdiff(
      names(row),
      c("issue_code", "duplicate_target_id", "measurement_id")
    )
    predicate <- build_row_predicate(row, match_columns)
    DBI::dbExecute(
      connection,
      sprintf(
        "DELETE FROM main.measurement WHERE measurement_id = %s AND %s",
        row$duplicate_target_id[1],
        predicate
      )
    )
    restore_row <- row[, setdiff(names(row), c("issue_code", "duplicate_target_id")), drop = FALSE]
    DBI::dbAppendTable(connection, table_id("measurement"), restore_row)
    append_fix_registry(
      connection = connection,
      stage = stage,
      issue_code = "fatal_duplicate_measurement_pk",
      table_name = "MEASUREMENT",
      row_identifiers = sprintf("measurement_id=%s", restore_row$measurement_id[1]),
      fix_summary = "Removed duplicated measurement row and restored original measurement_id."
    )
  }

  overlap_backup <- DBI::dbReadTable(connection, table_id("tutorial_backup_observation_period_overlap"))
  if (nrow(overlap_backup) > 0) {
    DBI::dbExecute(
      connection,
      sprintf(
        "DELETE FROM main.observation_period WHERE observation_period_id = %s",
        overlap_backup$observation_period_id[1]
      )
    )
    append_fix_registry(
      connection = connection,
      stage = stage,
      issue_code = "fatal_observation_period_overlap",
      table_name = "OBSERVATION_PERIOD",
      row_identifiers = sprintf("observation_period_id=%s", overlap_backup$observation_period_id[1]),
      fix_summary = "Deleted the injected overlapping observation_period row."
    )
  }
}

apply_convention_fixes <- function(connection, stage = "convention") {
  procedure_backup <- DBI::dbReadTable(connection, table_id("tutorial_backup_procedure_occurrence"))
  if (nrow(procedure_backup) > 0) {
    restore_rows <- procedure_backup[, setdiff(names(procedure_backup), "issue_code"), drop = FALSE]
    DBI::dbAppendTable(connection, table_id("procedure_occurrence"), restore_rows)
    append_fix_registry(
      connection = connection,
      stage = stage,
      issue_code = "convention_procedure_person_completeness",
      table_name = "PROCEDURE_OCCURRENCE",
      row_identifiers = sprintf("person_id=%s", sort(unique(restore_rows$person_id))),
      fix_summary = "Restored backed-up procedure_occurrence rows for the selected persons."
    )
  }

  condition_era_backup <- DBI::dbReadTable(connection, table_id("tutorial_backup_condition_era"))
  if (nrow(condition_era_backup) > 0) {
    restore_rows <- condition_era_backup[, setdiff(names(condition_era_backup), "issue_code"), drop = FALSE]
    DBI::dbAppendTable(connection, table_id("condition_era"), restore_rows)
    append_fix_registry(
      connection = connection,
      stage = stage,
      issue_code = "convention_condition_era_completeness",
      table_name = "CONDITION_ERA",
      row_identifiers = sprintf("person_id=%s", sort(unique(restore_rows$person_id))),
      fix_summary = "Restored backed-up condition_era rows."
    )
  }
}

apply_characterization_fixes <- function(connection, stage = "characterization") {
  observation_backup <- DBI::dbReadTable(connection, table_id("tutorial_backup_observation_dates"))
  if (nrow(observation_backup) > 0) {
    for (row_index in seq_len(nrow(observation_backup))) {
      DBI::dbExecute(
        connection,
        sprintf(
          "UPDATE main.observation SET observation_date = %s WHERE observation_id = %s",
          sql_literal(as.Date(observation_backup$observation_date[row_index])),
          observation_backup$observation_id[row_index]
        )
      )
    }
    append_fix_registry(
      connection = connection,
      stage = stage,
      issue_code = "characterization_observation_within_visit_dates",
      table_name = "OBSERVATION",
      row_identifiers = sprintf("observation_id=%s", observation_backup$observation_id),
      fix_summary = "Restored original observation_date values."
    )
  }

  drug_backup <- DBI::dbReadTable(connection, table_id("tutorial_backup_drug_exposure_dates"))
  if (nrow(drug_backup) > 0) {
    for (row_index in seq_len(nrow(drug_backup))) {
      DBI::dbExecute(
        connection,
        sprintf(
          "UPDATE main.drug_exposure SET drug_exposure_end_date = %s WHERE drug_exposure_id = %s",
          sql_literal(as.Date(drug_backup$drug_exposure_end_date[row_index])),
          drug_backup$drug_exposure_id[row_index]
        )
      )
    }
    append_fix_registry(
      connection = connection,
      stage = stage,
      issue_code = "characterization_drug_start_before_end",
      table_name = "DRUG_EXPOSURE",
      row_identifiers = sprintf("drug_exposure_id=%s", drug_backup$drug_exposure_id),
      fix_summary = "Restored original drug_exposure_end_date values."
    )
  }
}

get_issue_sql <- function(issue_code) {
  sql_map <- list(
    fatal_duplicate_measurement_pk = "
      SELECT measurement_id, COUNT(*) AS duplicate_count
      FROM main.measurement
      GROUP BY measurement_id
      HAVING COUNT(*) > 1
      ORDER BY measurement_id
    ",
    fatal_observation_period_overlap = "
      SELECT
        op1.person_id,
        op1.observation_period_id AS first_observation_period_id,
        op2.observation_period_id AS second_observation_period_id,
        op1.observation_period_start_date AS first_start_date,
        op1.observation_period_end_date AS first_end_date,
        op2.observation_period_start_date AS second_start_date,
        op2.observation_period_end_date AS second_end_date
      FROM main.observation_period op1
      INNER JOIN main.observation_period op2
        ON op1.person_id = op2.person_id
       AND op1.observation_period_id < op2.observation_period_id
       AND op1.observation_period_start_date <= op2.observation_period_end_date
       AND op2.observation_period_start_date <= op1.observation_period_end_date
      ORDER BY op1.person_id
    ",
    convention_procedure_person_completeness = "
      SELECT p.person_id
      FROM main.person p
      LEFT JOIN main.procedure_occurrence po
        ON p.person_id = po.person_id
      WHERE po.person_id IS NULL
      ORDER BY p.person_id
    ",
    convention_condition_era_completeness = "
      SELECT DISTINCT co.person_id
      FROM main.condition_occurrence co
      LEFT JOIN main.condition_era ce
        ON co.person_id = ce.person_id
      WHERE ce.person_id IS NULL
      ORDER BY co.person_id
    ",
    characterization_observation_within_visit_dates = "
      SELECT
        o.observation_id,
        o.visit_occurrence_id,
        o.observation_date,
        v.visit_start_date,
        v.visit_end_date
      FROM main.observation o
      INNER JOIN main.visit_occurrence v
        ON o.visit_occurrence_id = v.visit_occurrence_id
      WHERE o.observation_date < (v.visit_start_date - 7)
         OR o.observation_date > (v.visit_end_date + 7)
      ORDER BY o.observation_id
    ",
    characterization_drug_start_before_end = "
      SELECT
        drug_exposure_id,
        drug_exposure_start_date,
        drug_exposure_end_date
      FROM main.drug_exposure
      WHERE drug_exposure_end_date < drug_exposure_start_date
      ORDER BY drug_exposure_id
    "
  )

  sql_map[[issue_code]]
}

write_tutorial_log <- function(db_path, log_path, title) {
  connection <- connect_duckdb(db_path, read_only = TRUE)
  on.exit(disconnect_duckdb(connection), add = TRUE)

  issue_rows <- DBI::dbGetQuery(
    connection,
    "SELECT * FROM main.tutorial_issue_registry ORDER BY severity, issue_code, row_identifier"
  )
  fix_rows <- DBI::dbGetQuery(
    connection,
    "SELECT * FROM main.tutorial_fix_registry ORDER BY applied_timestamp, issue_code, row_identifier"
  )

  lines <- c(
    title,
    paste("Database:", normalizePath(db_path, winslash = "/", mustWork = TRUE)),
    paste("Generated:", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
    "",
    "[Issues]"
  )

  if (nrow(issue_rows) == 0) {
    lines <- c(lines, "None")
  } else {
    issue_lines <- apply(issue_rows, 1, function(row) {
      paste(
        row[["severity"]],
        row[["issue_code"]],
        row[["row_identifier"]],
        "-",
        row[["mutation_summary"]]
      )
    })
    lines <- c(lines, issue_lines)
  }

  lines <- c(lines, "", "[Fixes]")

  if (nrow(fix_rows) == 0) {
    lines <- c(lines, "None")
  } else {
    fix_lines <- apply(fix_rows, 1, function(row) {
      paste(
        row[["stage"]],
        row[["issue_code"]],
        row[["row_identifier"]],
        "-",
        row[["fix_summary"]]
      )
    })
    lines <- c(lines, fix_lines)
  }

  write_lines_file(lines, log_path)
}
