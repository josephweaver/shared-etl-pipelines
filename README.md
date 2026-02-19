# Shared ETL Pipelines

This repository contains pipeline and script assets used by Research ETL execution environments.

## Layout

- `projects/shared/pipelines/`: shared pipeline YAML files
- `projects/shared/scripts/`: shared helper scripts referenced by pipelines/plugins

## Initial import

Imported from `research-etl` on 2026-02-19.

## Project routing convention

For now, all assets are under `projects/shared/`.
Future project-specific assets can be added as:

- `projects/<project_id>/pipelines/`
- `projects/<project_id>/scripts/`

Execution environments can then map `project_id -> projects/<project_id>/...`.
