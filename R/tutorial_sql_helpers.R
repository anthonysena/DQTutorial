if (file.exists("R/dqd_helpers.R")) {
  source("R/dqd_helpers.R")
}

tutorial_cdm_schema <- "cdm"
tutorial_raw_schema <- "synthea_native"
tutorial_meta_schema <- "main"

tutorial_issue_catalog <- data.frame(
  issue_code = c(
    "fatal_visit_end_date_required",
    "fatal_observation_period_overlap",
    "convention_measurement_fk_domain",
    "convention_drug_is_standard_valid_concept",
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
    "isRequired",
    "measureObservationPeriodOverlap",
    "fkDomain",
    "isStandardValidConcept",
    "withinVisitDates",
    "plausibleStartBeforeEnd"
  ),
  table_name = c(
    "VISIT_OCCURRENCE",
    "OBSERVATION_PERIOD",
    "MEASUREMENT",
    "DRUG_EXPOSURE",
    "OBSERVATION",
    "DRUG_EXPOSURE"
  ),
  field_name = c(
    "VISIT_END_DATE",
    NA,
    "MEASUREMENT_CONCEPT_ID",
    "DRUG_CONCEPT_ID",
    "OBSERVATION_DATE",
    "DRUG_EXPOSURE_END_DATE"
  ),
  stringsAsFactors = FALSE
)

table_id <- function(name, schema = tutorial_meta_schema) {
  DBI::Id(schema = schema, table = name)
}

qualified_name <- function(name, schema) {
  paste(schema, name, sep = ".")
}

get_tutorial_check_metadata <- function(cdm_version = "5.4") {
  checks <- DataQualityDashboard::listDqChecks(cdmVersion = cdm_version)
  descriptions <- as.data.frame(checks$checkDescriptions, stringsAsFactors = FALSE)
  merge(
    tutorial_issue_catalog,
    descriptions,
    by.x = c("target_check", "severity"),
    by.y = c("checkName", "severity"),
    all.x = TRUE,
    sort = FALSE
  )
}

get_dqd_template_path <- function(sql_file, dbms = "sql_server") {
  system.file(file.path("sql", dbms, sql_file), package = "DataQualityDashboard")
}

read_dqd_template <- function(sql_file, dbms = "sql_server") {
  template_path <- get_dqd_template_path(sql_file, dbms = dbms)
  if (!nzchar(template_path) || !file.exists(template_path)) {
    stop("Could not locate DQD SQL template for: ", sql_file)
  }

  paste(readLines(template_path, warn = FALSE), collapse = "\n")
}

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

  DBI::dbExecute(connection, "
    CREATE OR REPLACE TABLE main.tutorial_issue_lineage (
      issue_code VARCHAR,
      cdm_table_name VARCHAR,
      cdm_row_id VARCHAR,
      raw_table_name VARCHAR,
      raw_patient VARCHAR,
      raw_encounter VARCHAR,
      raw_start VARCHAR,
      raw_code VARCHAR,
      raw_description VARCHAR,
      note VARCHAR
    )
  ")
}

append_table <- function(connection, name, data, schema = tutorial_meta_schema) {
  if (nrow(data) == 0) {
    return(invisible(NULL))
  }

  exists <- DBI::dbGetQuery(
    connection,
    sprintf(
      "
      SELECT COUNT(*) AS n
      FROM information_schema.tables
      WHERE table_schema = %s
        AND table_name = %s
      ",
      sql_literal(schema),
      sql_literal(name)
    )
  )$n[1] > 0

  if (exists) {
    insert_rows_sql(connection, schema, name, data)
  } else {
    DBI::dbWriteTable(connection, table_id(name, schema = schema), data, overwrite = TRUE)
  }
}

append_issue_registry <- function(connection,
                                  issue_code,
                                  row_identifiers,
                                  mutation_summary) {
  issue_meta <- tutorial_issue_catalog[tutorial_issue_catalog[["issue_code"]] == issue_code, , drop = FALSE]
  registry <- data.frame(
    issue_code = issue_code,
    severity = issue_meta[["severity"]],
    target_check = issue_meta[["target_check"]],
    table_name = issue_meta[["table_name"]],
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

append_issue_lineage <- function(connection,
                                 issue_code,
                                 cdm_table_name,
                                 cdm_row_ids,
                                 raw_table_name,
                                 raw_patient,
                                 raw_encounter,
                                 raw_start,
                                 raw_code,
                                 raw_description,
                                 note) {
  lineage <- data.frame(
    issue_code = issue_code,
    cdm_table_name = cdm_table_name,
    cdm_row_id = as.character(cdm_row_ids),
    raw_table_name = raw_table_name,
    raw_patient = raw_patient,
    raw_encounter = raw_encounter,
    raw_start = raw_start,
    raw_code = raw_code,
    raw_description = raw_description,
    note = note,
    stringsAsFactors = FALSE
  )

  append_table(connection, "tutorial_issue_lineage", lineage)
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

insert_rows_sql <- function(connection, schema, table_name, data) {
  if (nrow(data) == 0) {
    return(invisible(NULL))
  }

  columns <- names(data)
  value_rows <- apply(data, 1, function(row) {
    paste(vapply(columns, function(column) sql_literal(row[[column]]), character(1)), collapse = ", ")
  })

  DBI::dbExecute(
    connection,
    sprintf(
      "INSERT INTO %s.%s (%s) VALUES %s",
      schema,
      table_name,
      paste(columns, collapse = ", "),
      paste(sprintf("(%s)", value_rows), collapse = ", ")
    )
  )
}

register_raw_fk_domain_issue <- function(connection,
                                         cdm_schema = tutorial_cdm_schema,
                                         raw_schema = tutorial_raw_schema) {
  query <- sprintf(
    "
    WITH raw_rows AS (
      SELECT
        ROW_NUMBER() OVER (ORDER BY DATE, PATIENT, ENCOUNTER, CODE) AS raw_row_number,
        DATE AS START,
        PATIENT,
        ENCOUNTER,
        CODE,
        DESCRIPTION
      FROM %s.observations
      WHERE DESCRIPTION LIKE '%%[tutorial fkDomain issue]%%'
    )
    SELECT
      measured_rows.measurement_id,
      pe.person_source_value,
      raw_rows.ENCOUNTER,
      raw_rows.START,
      raw_rows.CODE,
      raw_rows.DESCRIPTION
    FROM raw_rows
    INNER JOIN %s.person pe
      ON pe.person_source_value = raw_rows.PATIENT
    INNER JOIN %s.visit_occurrence vo
      ON vo.person_id = pe.person_id
     AND vo.visit_source_value = raw_rows.ENCOUNTER
    INNER JOIN (
      SELECT
        measurement_id,
        person_id,
        visit_occurrence_id,
        ROW_NUMBER() OVER (
          PARTITION BY person_id, visit_occurrence_id
          ORDER BY measurement_id
        ) AS rn
      FROM %s.measurement
    ) measured_rows
      ON measured_rows.person_id = pe.person_id
     AND measured_rows.visit_occurrence_id = vo.visit_occurrence_id
     AND measured_rows.rn = 1
    ORDER BY measured_rows.measurement_id
    ",
    raw_schema,
    cdm_schema,
    cdm_schema,
    cdm_schema
  )

  rows <- DBI::dbGetQuery(connection, query)
  if (nrow(rows) == 0) {
    stop("Could not locate any visit-linked measurement rows for the tutorial fkDomain issue.")
  }

  measurement_rows <- DBI::dbGetQuery(
    connection,
    sprintf(
      "SELECT * FROM %s.measurement WHERE measurement_id IN (%s)",
      cdm_schema,
      paste(rows$measurement_id, collapse = ", ")
    )
  )
  measurement_rows$issue_code <- "convention_measurement_fk_domain"
  append_table(connection, "tutorial_backup_measurement_fk_domain", measurement_rows)

  replacement_code <- rows$CODE[1]
  replacement_concept_id <- get_candidate_scalar(
    connection,
    sprintf(
      "
      SELECT concept_id
      FROM %s.concept
      WHERE concept_code = %s
        AND domain_id <> 'Measurement'
      LIMIT 1
      ",
      cdm_schema,
      sql_literal(replacement_code)
    ),
    "concept_id"
  )

  DBI::dbExecute(
    connection,
    sprintf(
      "UPDATE %s.measurement SET measurement_concept_id = %s WHERE measurement_id IN (%s)",
      cdm_schema,
      replacement_concept_id,
      paste(rows$measurement_id, collapse = ", ")
    )
  )

  append_issue_registry(
    connection = connection,
    issue_code = "convention_measurement_fk_domain",
    row_identifiers = sprintf("measurement_id=%s", rows$measurement_id),
    mutation_summary = sprintf(
      paste(
        "Marked raw observations were traced to the same visit and the selected",
        "measurement rows were adjusted to concept_id %s derived from raw code %s",
        "after ETL."
      ),
      replacement_concept_id,
      replacement_code
    )
  )

  append_issue_lineage(
    connection = connection,
    issue_code = "convention_measurement_fk_domain",
    cdm_table_name = "MEASUREMENT",
    cdm_row_ids = rows$measurement_id,
    raw_table_name = "observations",
    raw_patient = rows$person_source_value,
    raw_encounter = rows$ENCOUNTER,
    raw_start = rows$START,
    raw_code = rows$CODE,
    raw_description = rows$DESCRIPTION,
    note = sprintf(
      paste(
        "The raw observation marker identifies the visit; the tutorial then",
        "adjusts one measurement row in that visit to concept_id %s derived",
        "from raw code %s."
      ),
      replacement_concept_id,
      replacement_code
    )
  )
}

inject_visit_end_date_required_issue <- function(connection, cdm_schema = tutorial_cdm_schema) {
  candidate <- DBI::dbGetQuery(
    connection,
    sprintf(
      "
      SELECT visit_occurrence_id, visit_end_date
      FROM %s.visit_occurrence
      WHERE visit_end_date IS NOT NULL
      ORDER BY visit_occurrence_id
      LIMIT 1
      ",
      cdm_schema
    )
  )

  if (nrow(candidate) != 1) {
    stop("Could not find an eligible visit_occurrence row for the required-field fatal issue.")
  }

  backup <- candidate
  backup$issue_code <- "fatal_visit_end_date_required"
  append_table(connection, "tutorial_backup_visit_end_date_required", backup)

  DBI::dbExecute(
    connection,
    sprintf(
      "ALTER TABLE %s.visit_occurrence ALTER COLUMN visit_end_date DROP NOT NULL",
      cdm_schema
    )
  )
  DBI::dbExecute(
    connection,
    sprintf(
      "UPDATE %s.visit_occurrence SET visit_end_date = NULL WHERE visit_occurrence_id = %s",
      cdm_schema,
      candidate$visit_occurrence_id[1]
    )
  )

  append_issue_registry(
    connection = connection,
    issue_code = "fatal_visit_end_date_required",
    row_identifiers = sprintf("visit_occurrence_id=%s", candidate$visit_occurrence_id[1]),
    mutation_summary = "Dropped NOT NULL on VISIT_OCCURRENCE.VISIT_END_DATE and set one required value to NULL."
  )
}

inject_observation_period_overlap <- function(connection, cdm_schema = tutorial_cdm_schema) {
  candidate <- DBI::dbGetQuery(
    connection,
    sprintf(
      "
      WITH one_period_person AS (
        SELECT person_id
        FROM %s.observation_period
        GROUP BY person_id
        HAVING COUNT(*) = 1
        ORDER BY person_id
        LIMIT 1
      )
      SELECT *
      FROM %s.observation_period
      WHERE person_id IN (SELECT person_id FROM one_period_person)
      ",
      cdm_schema,
      cdm_schema
    )
  )

  if (nrow(candidate) != 1) {
    stop("Could not find an eligible observation_period row for overlap injection.")
  }

  new_row <- candidate
  new_row$observation_period_id <- get_candidate_scalar(
    connection,
    sprintf(
      "SELECT COALESCE(MAX(observation_period_id), 0) + 1 AS next_id FROM %s.observation_period",
      cdm_schema
    ),
    "next_id"
  )
  append_table(connection, "tutorial_backup_observation_period_overlap", new_row)
  insert_rows_sql(connection, cdm_schema, "observation_period", new_row)

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

inject_drug_standard_valid_issue <- function(connection,
                                             cdm_schema = tutorial_cdm_schema,
                                             raw_schema = tutorial_raw_schema) {
  rows <- DBI::dbGetQuery(
    connection,
    sprintf(
      "
      WITH raw_rows AS (
        SELECT
          START,
          PATIENT,
          ENCOUNTER,
          CODE,
          DESCRIPTION
        FROM %s.medications
        WHERE DESCRIPTION LIKE '%%[tutorial standard validity issue]%%'
      )
      SELECT
        de.drug_exposure_id,
        pe.person_source_value,
        raw_rows.ENCOUNTER,
        raw_rows.START,
        raw_rows.CODE,
        raw_rows.DESCRIPTION
      FROM raw_rows
      INNER JOIN %s.person pe
        ON pe.person_source_value = raw_rows.PATIENT
      INNER JOIN %s.drug_exposure de
        ON de.person_id = pe.person_id
       AND de.drug_source_value = raw_rows.CODE
       AND de.drug_exposure_start_date = CAST(raw_rows.START AS DATE)
      INNER JOIN %s.visit_occurrence vo
        ON de.visit_occurrence_id = vo.visit_occurrence_id
       AND vo.visit_source_value = raw_rows.ENCOUNTER
      ORDER BY de.drug_exposure_id
      ",
      raw_schema,
      cdm_schema,
      cdm_schema,
      cdm_schema
    )
  )

  if (nrow(rows) == 0) {
    stop("Could not locate any ETL rows for the tutorial isStandardValidConcept issue.")
  }

  replacement_concept_id <- get_candidate_scalar(
    connection,
    sprintf(
      "
      SELECT concept_id
      FROM %s.concept
      WHERE domain_id = 'Drug'
        AND standard_concept = 'C'
        AND invalid_reason IS NULL
      ORDER BY concept_id
      LIMIT 1
      ",
      cdm_schema
    ),
    "concept_id"
  )

  backup <- DBI::dbGetQuery(
    connection,
    sprintf(
      "SELECT * FROM %s.drug_exposure WHERE drug_exposure_id IN (%s)",
      cdm_schema,
      paste(rows$drug_exposure_id, collapse = ", ")
    )
  )
  backup$issue_code <- "convention_drug_is_standard_valid_concept"
  append_table(connection, "tutorial_backup_drug_standard_valid", backup)

  DBI::dbExecute(
    connection,
    sprintf(
      "UPDATE %s.drug_exposure SET drug_concept_id = %s WHERE drug_exposure_id IN (%s)",
      cdm_schema,
      replacement_concept_id,
      paste(rows$drug_exposure_id, collapse = ", ")
    )
  )

  append_issue_registry(
    connection = connection,
    issue_code = "convention_drug_is_standard_valid_concept",
    row_identifiers = sprintf("drug_exposure_id=%s", rows$drug_exposure_id),
    mutation_summary = sprintf(
      "Adjusted selected drug_exposure rows to non-standard concept_id %s after ETL.",
      replacement_concept_id
    )
  )

  append_issue_lineage(
    connection = connection,
    issue_code = "convention_drug_is_standard_valid_concept",
    cdm_table_name = "DRUG_EXPOSURE",
    cdm_row_ids = rows$drug_exposure_id,
    raw_table_name = "medications",
    raw_patient = rows$person_source_value,
    raw_encounter = rows$ENCOUNTER,
    raw_start = rows$START,
    raw_code = rows$CODE,
    raw_description = rows$DESCRIPTION,
    note = sprintf(
      "Post-ETL tutorial adjustment replaced drug_concept_id with concept_id %s to guarantee isStandardValidConcept failure.",
      replacement_concept_id
    )
  )
}

inject_observation_within_visit_dates <- function(connection, cdm_schema = tutorial_cdm_schema) {
  rows <- DBI::dbGetQuery(
    connection,
    sprintf(
      "
      SELECT
        o.observation_id,
        o.visit_occurrence_id,
        o.observation_date,
        v.visit_end_date
      FROM %s.observation o
      INNER JOIN %s.visit_occurrence v
        ON o.visit_occurrence_id = v.visit_occurrence_id
      WHERE o.visit_occurrence_id IS NOT NULL
      ORDER BY o.observation_id
      LIMIT 10
      ",
      cdm_schema,
      cdm_schema
    )
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
        "UPDATE %s.observation SET observation_date = %s WHERE observation_id = %s",
        cdm_schema,
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

inject_drug_start_before_end <- function(connection, cdm_schema = tutorial_cdm_schema) {
  rows <- DBI::dbGetQuery(
    connection,
    sprintf(
      "
      SELECT
        drug_exposure_id,
        drug_exposure_start_date,
        drug_exposure_end_date
      FROM %s.drug_exposure
      WHERE drug_exposure_end_date IS NOT NULL
      ORDER BY drug_exposure_id
      LIMIT 10
      ",
      cdm_schema
    )
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
        "UPDATE %s.drug_exposure SET drug_exposure_end_date = %s WHERE drug_exposure_id = %s",
        cdm_schema,
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

inject_tutorial_issues <- function(connection,
                                   cdm_schema = tutorial_cdm_schema,
                                   raw_schema = tutorial_raw_schema) {
  initialize_tutorial_tables(connection)
  register_raw_fk_domain_issue(connection, cdm_schema = cdm_schema, raw_schema = raw_schema)
  inject_drug_standard_valid_issue(connection, cdm_schema = cdm_schema, raw_schema = raw_schema)
  inject_visit_end_date_required_issue(connection, cdm_schema = cdm_schema)
  inject_observation_period_overlap(connection, cdm_schema = cdm_schema)
  inject_observation_within_visit_dates(connection, cdm_schema = cdm_schema)
  inject_drug_start_before_end(connection, cdm_schema = cdm_schema)
}

apply_fatal_fixes <- function(connection,
                              stage = "fatal",
                              cdm_schema = tutorial_cdm_schema) {
  visit_backup <- DBI::dbReadTable(connection, table_id("tutorial_backup_visit_end_date_required"))
  if (nrow(visit_backup) > 0) {
    DBI::dbExecute(
      connection,
      sprintf(
        "UPDATE %s.visit_occurrence SET visit_end_date = %s WHERE visit_occurrence_id = %s",
        cdm_schema,
        sql_literal(as.Date(visit_backup$visit_end_date[1])),
        visit_backup$visit_occurrence_id[1]
      )
    )
    DBI::dbExecute(
      connection,
      sprintf(
        "ALTER TABLE %s.visit_occurrence ALTER COLUMN visit_end_date SET NOT NULL",
        cdm_schema
      )
    )
    append_fix_registry(
      connection = connection,
      stage = stage,
      issue_code = "fatal_visit_end_date_required",
      table_name = "VISIT_OCCURRENCE",
      row_identifiers = sprintf("visit_occurrence_id=%s", visit_backup$visit_occurrence_id[1]),
      fix_summary = "Restored the missing visit_end_date value and reinstated the NOT NULL constraint."
    )
  }

  overlap_backup <- DBI::dbReadTable(connection, table_id("tutorial_backup_observation_period_overlap"))
  if (nrow(overlap_backup) > 0) {
    DBI::dbExecute(
      connection,
      sprintf(
        "DELETE FROM %s.observation_period WHERE observation_period_id = %s",
        cdm_schema,
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

apply_convention_fixes <- function(connection,
                                   stage = "convention",
                                   cdm_schema = tutorial_cdm_schema) {
  measurement_backup <- DBI::dbReadTable(connection, table_id("tutorial_backup_measurement_fk_domain"))
  if (nrow(measurement_backup) > 0) {
    for (row_index in seq_len(nrow(measurement_backup))) {
      DBI::dbExecute(
        connection,
        sprintf(
          "UPDATE %s.measurement SET measurement_concept_id = %s WHERE measurement_id = %s",
          cdm_schema,
          measurement_backup$measurement_concept_id[row_index],
          measurement_backup$measurement_id[row_index]
        )
      )
    }
    append_fix_registry(
      connection = connection,
      stage = stage,
      issue_code = "convention_measurement_fk_domain",
      table_name = "MEASUREMENT",
      row_identifiers = sprintf(
        "measurement_id=%s",
        measurement_backup$measurement_id
      ),
      fix_summary = "Restored the original measurement_concept_id values for the tutorial measurement rows."
    )
  }

  drug_backup <- DBI::dbReadTable(connection, table_id("tutorial_backup_drug_standard_valid"))
  if (nrow(drug_backup) > 0) {
    for (row_index in seq_len(nrow(drug_backup))) {
      DBI::dbExecute(
        connection,
        sprintf(
          "UPDATE %s.drug_exposure SET drug_concept_id = %s WHERE drug_exposure_id = %s",
          cdm_schema,
          drug_backup$drug_concept_id[row_index],
          drug_backup$drug_exposure_id[row_index]
        )
      )
    }
    append_fix_registry(
      connection = connection,
      stage = stage,
      issue_code = "convention_drug_is_standard_valid_concept",
      table_name = "DRUG_EXPOSURE",
      row_identifiers = sprintf("drug_exposure_id=%s", drug_backup$drug_exposure_id),
      fix_summary = "Restored the original drug_concept_id values for the tutorial drug rows."
    )
  }
}

apply_characterization_fixes <- function(connection,
                                         stage = "characterization",
                                         cdm_schema = tutorial_cdm_schema) {
  observation_backup <- DBI::dbReadTable(connection, table_id("tutorial_backup_observation_dates"))
  if (nrow(observation_backup) > 0) {
    for (row_index in seq_len(nrow(observation_backup))) {
      DBI::dbExecute(
        connection,
        sprintf(
          "UPDATE %s.observation SET observation_date = %s WHERE observation_id = %s",
          cdm_schema,
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
          "UPDATE %s.drug_exposure SET drug_exposure_end_date = %s WHERE drug_exposure_id = %s",
          cdm_schema,
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

get_issue_sql <- function(issue_code,
                          cdm_schema = tutorial_cdm_schema,
                          raw_schema = tutorial_raw_schema,
                          meta_schema = tutorial_meta_schema) {
  sql_map <- list(
    fatal_visit_end_date_required = sprintf(
      "
      SELECT
        visit_occurrence_id,
        person_id,
        visit_start_date,
        visit_end_date
      FROM %s.visit_occurrence
      WHERE visit_end_date IS NULL
      ORDER BY visit_occurrence_id
      ",
      cdm_schema
    ),
    fatal_observation_period_overlap = sprintf(
      "
      SELECT
        op1.person_id,
        op1.observation_period_id AS first_observation_period_id,
        op2.observation_period_id AS second_observation_period_id,
        op1.observation_period_start_date AS first_start_date,
        op1.observation_period_end_date AS first_end_date,
        op2.observation_period_start_date AS second_start_date,
        op2.observation_period_end_date AS second_end_date
      FROM %s.observation_period op1
      INNER JOIN %s.observation_period op2
        ON op1.person_id = op2.person_id
       AND op1.observation_period_id < op2.observation_period_id
       AND op1.observation_period_start_date <= op2.observation_period_end_date
       AND op2.observation_period_start_date <= op1.observation_period_end_date
      ORDER BY op1.person_id
      ",
      cdm_schema,
      cdm_schema
    ),
    convention_measurement_fk_domain = sprintf(
      "
      SELECT
        me.measurement_id,
        me.measurement_date,
        pe.person_source_value,
        me.measurement_source_value,
        me.measurement_concept_id,
        co.concept_name,
        co.domain_id,
        li.raw_encounter,
        li.raw_start,
        li.raw_description
      FROM %s.measurement me
      INNER JOIN %s.person pe
        ON me.person_id = pe.person_id
      INNER JOIN %s.concept co
        ON me.measurement_concept_id = co.concept_id
      LEFT JOIN %s.tutorial_issue_lineage li
        ON li.issue_code = 'convention_measurement_fk_domain'
       AND li.cdm_row_id = CAST(me.measurement_id AS VARCHAR)
      WHERE me.measurement_id IN (
        SELECT CAST(cdm_row_id AS BIGINT)
        FROM %s.tutorial_issue_lineage
        WHERE issue_code = 'convention_measurement_fk_domain'
      )
        AND co.domain_id <> 'Measurement'
      ORDER BY me.measurement_id
      ",
      cdm_schema,
      cdm_schema,
      cdm_schema,
      meta_schema,
      meta_schema
    ),
    convention_drug_is_standard_valid_concept = sprintf(
      "
      SELECT
        de.drug_exposure_id,
        de.drug_exposure_start_date,
        pe.person_source_value,
        de.drug_source_value,
        de.drug_concept_id,
        co.concept_name,
        co.standard_concept,
        co.invalid_reason,
        li.raw_encounter,
        li.raw_start,
        li.raw_description,
        li.note
      FROM %s.drug_exposure de
      INNER JOIN %s.person pe
        ON de.person_id = pe.person_id
      INNER JOIN %s.concept co
        ON de.drug_concept_id = co.concept_id
      LEFT JOIN %s.tutorial_issue_lineage li
        ON li.issue_code = 'convention_drug_is_standard_valid_concept'
       AND li.cdm_row_id = CAST(de.drug_exposure_id AS VARCHAR)
      WHERE de.drug_exposure_id IN (
        SELECT CAST(cdm_row_id AS BIGINT)
        FROM %s.tutorial_issue_lineage
        WHERE issue_code = 'convention_drug_is_standard_valid_concept'
      )
        AND (
          co.standard_concept <> 'S'
          OR co.standard_concept IS NULL
          OR co.invalid_reason IS NOT NULL
        )
      ORDER BY de.drug_exposure_id
      ",
      cdm_schema,
      cdm_schema,
      cdm_schema,
      meta_schema,
      meta_schema
    ),
    characterization_observation_within_visit_dates = sprintf(
      "
      SELECT
        o.observation_id,
        o.visit_occurrence_id,
        o.observation_date,
        v.visit_start_date,
        v.visit_end_date
      FROM %s.observation o
      INNER JOIN %s.visit_occurrence v
        ON o.visit_occurrence_id = v.visit_occurrence_id
      WHERE o.observation_date < (v.visit_start_date - 7)
         OR o.observation_date > (v.visit_end_date + 7)
      ORDER BY o.observation_id
      ",
      cdm_schema,
      cdm_schema
    ),
    characterization_drug_start_before_end = sprintf(
      "
      SELECT
        drug_exposure_id,
        drug_exposure_start_date,
        drug_exposure_end_date
      FROM %s.drug_exposure
      WHERE drug_exposure_end_date < drug_exposure_start_date
      ORDER BY drug_exposure_id
      ",
      cdm_schema
    )
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
