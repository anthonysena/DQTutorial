# DQ Tutorial for OHDSI

This repository is a hands-on tutorial project for OHDSI users who want to learn how to use the DataQualityDashboard (DQD) against an OMOP CDM. It includes a clean synthetic Synthea OMOP CDM, scripts to generate intentionally broken tutorial snapshots, and a staged Quarto tutorial that walks through finding and fixing issues by DQD severity.

## Repository Layout

- [CreateCDM.R](C:/git/anthonysena/DQTutorial/CreateCDM.R): rebuilds the clean Synthea OMOP CDM from the CSV source in `data/syntheaCDM/`.
- [InjectTutorialIssues.R](C:/git/anthonysena/DQTutorial/InjectTutorialIssues.R): copies the clean DuckDB and injects the six curated tutorial issues.
- [PrepareTutorialSnapshots.R](C:/git/anthonysena/DQTutorial/PrepareTutorialSnapshots.R): creates the broken and stage-fixed tutorial DuckDB snapshots.
- [RunDQD.R](C:/git/anthonysena/DQTutorial/RunDQD.R): executes DQD for a chosen DuckDB and severity set and writes deterministic JSON output.
- [LaunchDqdDashboards.R](C:/git/anthonysena/DQTutorial/LaunchDqdDashboards.R): opens one or more saved DQD JSON result files in the dashboard viewer.
- [R/dqd_helpers.R](C:/git/anthonysena/DQTutorial/R/dqd_helpers.R): shared helper functions for argument parsing, DuckDB connections, and DQD execution.
- [R/tutorial_sql_helpers.R](C:/git/anthonysena/DQTutorial/R/tutorial_sql_helpers.R): helper functions for injecting, diagnosing, and fixing the staged tutorial issues.
- [tutorials/dqd-tutorial.qmd](C:/git/anthonysena/DQTutorial/tutorials/dqd-tutorial.qmd): the interactive Quarto tutorial.
- `data/syntheaCDM/`: source CSV files used to build the clean OMOP CDM.
- `dqd_output/`: output location for logs, DQD JSON files, and Quarto working files.

## Prerequisites

- R with the project dependencies restored through `renv`
- Quarto, if you want to render and run the tutorial document
- Enough local disk space to create several DuckDB snapshots

## Environment Setup

From the repository root:

```r
renv::restore()
```

If you want to render the Quarto tutorial, confirm Quarto is installed:

```powershell
quarto check
```

## Build the Clean Synthea CDM

The clean baseline CDM is stored in:

- `data/syntheaCDM.duckdb`

To rebuild it from the Synthea CSV extracts:

```powershell
Rscript CreateCDM.R
```

This uses the CSV files in `data/syntheaCDM/`, normalizes date formats, and reloads the data into DuckDB.

## Generate Tutorial CDM Snapshots

The tutorial keeps the clean baseline immutable and creates prepared snapshots for each stage of the workshop.

To build all snapshots:

```powershell
Rscript PrepareTutorialSnapshots.R
```

This produces:

- `data/syntheaCDM.duckdb`
- `data/syntheaCDM_tutorial_broken.duckdb`
- `data/syntheaCDM_tutorial_after_fatal.duckdb`
- `data/syntheaCDM_tutorial_after_convention.duckdb`
- `data/syntheaCDM_tutorial_after_characterization.duckdb`

The injected issue set is:

- `fatal`: duplicate `MEASUREMENT.MEASUREMENT_ID`
- `fatal`: overlapping `OBSERVATION_PERIOD` rows for one person
- `convention`: missing `PROCEDURE_OCCURRENCE` coverage for selected persons
- `convention`: missing `CONDITION_ERA` rows for one person with condition history
- `characterization`: `OBSERVATION.OBSERVATION_DATE` outside visit window
- `characterization`: `DRUG_EXPOSURE_END_DATE` before `DRUG_EXPOSURE_START_DATE`

The broken snapshot includes backup tables and registry tables used by the tutorial:

- `main.tutorial_issue_registry`
- `main.tutorial_fix_registry`

If you only want the broken tutorial database:

```powershell
Rscript InjectTutorialIssues.R
```

## Run DQD Outside Quarto

`RunDQD.R` accepts named `key=value` arguments.

### Clean CDM

```powershell
Rscript RunDQD.R dbPath=data/syntheaCDM.duckdb cdmSourceName=Synthea checkSeverity=fatal,convention,characterization outputJsonPath=dqd_output/results/synthea-clean-dqd.json
```

### Broken tutorial snapshot by stage

Fatal:

```powershell
Rscript RunDQD.R dbPath=data/syntheaCDM_tutorial_broken.duckdb cdmSourceName=SyntheaTutorial checkSeverity=fatal outputJsonPath=dqd_output/results/synthea-tutorial-fatal.json
```

Convention:

```powershell
Rscript RunDQD.R dbPath=data/syntheaCDM_tutorial_after_fatal.duckdb cdmSourceName=SyntheaTutorial checkSeverity=convention outputJsonPath=dqd_output/results/synthea-tutorial-convention.json
```

Characterization:

```powershell
Rscript RunDQD.R dbPath=data/syntheaCDM_tutorial_after_convention.duckdb cdmSourceName=SyntheaTutorial checkSeverity=characterization outputJsonPath=dqd_output/results/synthea-tutorial-characterization.json
```

The intended stable JSON artifacts are:

- `dqd_output/results/synthea-clean-dqd.json`
- `dqd_output/results/synthea-tutorial-fatal.json`
- `dqd_output/results/synthea-tutorial-convention.json`
- `dqd_output/results/synthea-tutorial-characterization.json`

To launch the dashboard viewer for these files:

```powershell
Rscript LaunchDqdDashboards.R
```

## Interactive Tutorial in Quarto

The staged hands-on tutorial lives in [tutorials/dqd-tutorial.qmd](C:/git/anthonysena/DQTutorial/tutorials/dqd-tutorial.qmd).

The tutorial flow is:

1. Start from the prepared broken tutorial snapshot.
2. Run DQD for `fatal`.
3. Identify the failing rows with focused SQL.
4. Apply the fatal fixes and rerun DQD.
5. Reset to the prepared post-fatal snapshot.
6. Run DQD for `convention`.
7. Identify, fix, and rerun.
8. Reset to the prepared post-convention snapshot.
9. Run DQD for `characterization`.
10. Identify, fix, and rerun.

Render the tutorial with:

```powershell
quarto render tutorials/dqd-tutorial.qmd
```

You can also open the `.qmd` file in an IDE and run the R chunks interactively.

## Expected Artifacts

After a full workflow run, expect these core files:

- `data/syntheaCDM.duckdb`
- `data/syntheaCDM_tutorial_broken.duckdb`
- `data/syntheaCDM_tutorial_after_fatal.duckdb`
- `data/syntheaCDM_tutorial_after_convention.duckdb`
- `data/syntheaCDM_tutorial_after_characterization.duckdb`
- `dqd_output/results/synthea-clean-dqd.json`
- `dqd_output/results/synthea-tutorial-fatal.json`
- `dqd_output/results/synthea-tutorial-convention.json`
- `dqd_output/results/synthea-tutorial-characterization.json`
- `dqd_output/tutorial_issue_log.txt`
- `dqd_output/tutorial_after_fatal_log.txt`
- `dqd_output/tutorial_after_convention_log.txt`
- `dqd_output/tutorial_after_characterization_log.txt`

## Troubleshooting / Known Caveats

- The previous version of the repo tried to export results from the DuckDB `dqddashboard_results` table. In this project, the direct JSON output from `executeDqChecks()` is the source of truth because the table-based export path was producing empty output.
- The vocabulary tables in the current DuckDB are empty, so the curated tutorial issues intentionally avoid vocabulary-dependent failures.
- The clean Synthea CDM may still have ambient DQD findings unrelated to the injected tutorial defects. The tutorial focuses on the staged injected deltas.
- The Quarto tutorial works from prepared snapshots so each severity stage starts from a deterministic database state.
