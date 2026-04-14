# DQ Tutorial for OHDSI

This repository is a hands-on tutorial project for OHDSI users who want to learn how to use the DataQualityDashboard (DQD) against an OMOP CDM. The current workflow centers on a committed broken raw Synthea export and a single tutorial DuckDB that contains both the transformed OMOP CDM and the staged raw source tables.

## Repository Layout

- [CreateCDM.R](C:/git/anthonysena/DQTutorial/CreateCDM.R): rebuilds the clean Synthea OMOP CDM from `data/syntheaCDM/`.
- [PrepareRawIssueSet.R](C:/git/anthonysena/DQTutorial/PrepareRawIssueSet.R): recreates `data/syntheaRawWithDqIssues/` from `data/syntheaRaw/` with deterministic tutorial mutations.
- [BuildTutorialCdmWithDq.R](C:/git/anthonysena/DQTutorial/BuildTutorialCdmWithDq.R): builds `data/syntheaCDMWithDq.duckdb` from `data/syntheaRawWithDqIssues/` and injects the non-raw tutorial issues.
- [RunDQD.R](C:/git/anthonysena/DQTutorial/RunDQD.R): executes DQD for a chosen DuckDB and schema.
- [LaunchDqdDashboards.R](C:/git/anthonysena/DQTutorial/LaunchDqdDashboards.R): opens one or more saved DQD JSON result files in the dashboard viewer.
- [R/dqd_helpers.R](C:/git/anthonysena/DQTutorial/R/dqd_helpers.R): shared helper functions for argument parsing, DuckDB connections, and DQD execution.
- [R/tutorial_sql_helpers.R](C:/git/anthonysena/DQTutorial/R/tutorial_sql_helpers.R): helper functions for registering, diagnosing, and fixing the curated tutorial issues in the single tutorial DuckDB.
- [tutorials/dqd-tutorial.qmd](C:/git/anthonysena/DQTutorial/tutorials/dqd-tutorial.qmd): the interactive Quarto tutorial.
- `data/syntheaRaw/`: clean raw Synthea CSV files.
- `data/syntheaRawWithDqIssues/`: committed raw Synthea CSV files with tutorial data quality mutations.
- `data/syntheaCDM.duckdb`: clean DuckDB artifact used as the historical “clean” baseline.
- `data/syntheaCDMWithDq.duckdb`: the main tutorial DuckDB with both `cdm` and `synthea_native` schemas.

## Prerequisites

- R with the project dependencies restored through `renv`
- Quarto, if you want to render the tutorial
- Enough local disk space to create DuckDB artifacts

## Environment Setup

From the repository root:

```r
renv::restore()
```

If you want to render the Quarto tutorial:

```powershell
quarto check
```

## Clean Baseline

The clean ETL artifact remains:

- `data/syntheaCDM.duckdb`

The historical ETL entrypoint used to build the clean database is still:

```powershell
Rscript etl/etl-synthea/run.R
```

That script is preserved as the clean-build artifact shown to students.

## Regenerate the Broken Raw Input Set

The committed tutorial raw inputs live under:

- `data/syntheaRawWithDqIssues/`

To recreate them from the clean raw Synthea export:

```powershell
Rscript PrepareRawIssueSet.R
```

This script copies `data/syntheaRaw/` and then applies deterministic tutorial mutations:

- `observations.csv`: selected rows are changed to a clearly marked source code used to trace the tutorial `fkDomain` issue from raw staging rows back to the affected measurement visit
- `medications.csv`: selected rows are marked for the tutorial `isStandardValidConcept` flow

## Build the Tutorial DuckDB

The main tutorial database is:

- `data/syntheaCDMWithDq.duckdb`

Build it with:

```powershell
Rscript BuildTutorialCdmWithDq.R
```

This database contains:

- `cdm`: OMOP CDM tables evaluated by DQD
- `synthea_native`: staged raw Synthea tables loaded by the ETL
- `main`: tutorial metadata tables used to track curated issue lineage and local fixes

The curated issue set is:

- `fatal`: missing required `VISIT_OCCURRENCE.VISIT_END_DATE`
- `fatal`: overlapping `OBSERVATION_PERIOD` rows for one person
- `convention`: `fkDomain` on `MEASUREMENT.MEASUREMENT_CONCEPT_ID`, anchored to marked raw `observations.csv` rows and recorded in the lineage metadata
- `convention`: `isStandardValidConcept` on `DRUG_EXPOSURE.DRUG_CONCEPT_ID`, tied to marked raw medication rows and guaranteed with a narrow post-ETL adjustment
- `characterization`: `OBSERVATION.OBSERVATION_DATE` outside visit window
- `characterization`: `DRUG_EXPOSURE_END_DATE` before `DRUG_EXPOSURE_START_DATE`

## Run DQD Outside Quarto

`RunDQD.R` accepts named `key=value` arguments.

### Clean baseline

```powershell
Rscript RunDQD.R dbPath=data/syntheaCDM.duckdb cdmDatabaseSchema=cdm resultsDatabaseSchema=main cdmSourceName=Synthea checkSeverity=fatal,convention,characterization outputJsonPath=dqd_output/results/synthea-clean-dqd.json
```

### Single tutorial DuckDB

```powershell
Rscript RunDQD.R dbPath=data/syntheaCDMWithDq.duckdb cdmDatabaseSchema=cdm resultsDatabaseSchema=main cdmSourceName=SyntheaTutorial checkSeverity=fatal,convention,characterization outputJsonPath=dqd_output/results/synthea-with-dq-dqd.json
```

To launch the dashboard viewer for saved results:

```powershell
Rscript LaunchDqdDashboards.R
```

## Interactive Tutorial in Quarto

The hands-on tutorial lives in [tutorials/dqd-tutorial.qmd](C:/git/anthonysena/DQTutorial/tutorials/dqd-tutorial.qmd).

Render it with:

```powershell
quarto render tutorials/dqd-tutorial.qmd
```

The tutorial now works from a temporary copy of `data/syntheaCDMWithDq.duckdb` instead of relying on committed stage snapshots. Students investigate issues by querying both `cdm` and `synthea_native` in the same database file.

## Expected Artifacts

After the main workflow runs, expect these core files:

- `data/syntheaRawWithDqIssues/`
- `data/syntheaCDM.duckdb`
- `data/syntheaCDMWithDq.duckdb`
- `dqd_output/results/synthea-clean-dqd.json`
- `dqd_output/results/synthea-with-dq-dqd.json`
- `dqd_output/tutorial_cdm_with_dq_log.txt`

## Legacy Scripts

These scripts remain in the repo for reference, but they are no longer the primary student workflow:

- [PrepareTutorialSnapshots.R](C:/git/anthonysena/DQTutorial/PrepareTutorialSnapshots.R)
- [InjectTutorialIssues.R](C:/git/anthonysena/DQTutorial/InjectTutorialIssues.R)

The tutorial no longer depends on `data/syntheaCDM_tutorial_*.duckdb`.

## Troubleshooting / Known Caveats

- The `fkDomain` and `isStandardValidConcept` convention examples are both traceable from committed raw rows in `synthea_native`, but each uses a narrow post-ETL concept adjustment so the final failing CDM row is deterministic for the tutorial.
- DQD should be run against the `cdm` schema, not `main`, for the single tutorial DuckDB.
- The tutorial works best after regenerating `data/syntheaRawWithDqIssues/` and `data/syntheaCDMWithDq.duckdb` so the committed artifacts and local build outputs stay aligned.
