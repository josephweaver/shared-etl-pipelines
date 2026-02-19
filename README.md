# Shared ETL Pipelines

This repository contains shared pipeline and script assets used by Research ETL execution environments.

## Layout

- `pipelines/`: shared pipeline YAML files
- `scripts/`: shared helper scripts referenced by pipelines/plugins

## Initial import

Imported from `research-etl` on 2026-02-19.

## Future project routing

If/when non-shared projects are added, use top-level folders such as:

- `pipelines_<project_id>/`
- `scripts_<project_id>/`

or separate repos per project.
