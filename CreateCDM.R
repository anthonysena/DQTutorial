library(Eunomia)

normalizeCsvDates <- function(dataPath) {
  csvFiles <- list.files(dataPath, pattern = "\\.csv$", full.names = TRUE)

  for (csvFile in csvFiles) {
    tableData <- readr::read_csv(
      file = csvFile,
      col_types = readr::cols(.default = readr::col_character()),
      show_col_types = FALSE
    )

    dateColumns <- names(tableData)[grepl("date", names(tableData), ignore.case = TRUE)]
    fileChanged <- FALSE

    for (columnName in dateColumns) {
      values <- tableData[[columnName]]
      ymdRows <- !is.na(values) & nzchar(values) & grepl("^\\d{8}$", values)

      if (!any(ymdRows)) {
        next
      }

      parsedDates <- as.Date(values[ymdRows], format = "%Y%m%d")
      if (any(is.na(parsedDates))) {
        stop("Failed to parse date values in ", basename(csvFile), "$", columnName)
      }

      tableData[[columnName]][ymdRows] <- format(parsedDates, "%Y-%m-%d")
      fileChanged <- TRUE
    }

    if (fileChanged) {
      readr::write_csv(tableData, csvFile, na = "")
    }
  }
}

normalizeCsvDates("data/syntheaCDM")

Eunomia::loadDataFiles(
  dataPath = "data/syntheaCDM",
  dbPath = "data/syntheaCDM.duckdb",
  inputFormat = "csv",
  cdmVersion = "5.4",
  cdmDatabaseSchema = "main",
  dbms = "duckdb",
  verbose = TRUE,
  overwrite = TRUE
)
