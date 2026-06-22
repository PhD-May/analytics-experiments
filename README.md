# analytics-experiments

Repository for experiment **analysis artifacts** (tables, notebooks, plots) kept separate from the `mec-streaming-framework` codebase.

## Layout

| Path | Role |
|------|------|
| `scripts/materialize_experiment_tables.py` | Reads a clone’s `mec-streaming-framework/runs/` (`aggregate.json` + optional `metrics/episodes/*.csv`) and writes **CSV tables** into `outputs/experiment_tables/`. |
| `outputs/experiment_tables/` | Generated `01_…`–`04_…` CSVs + `manifest.json` (see `outputs/README.md`). |
| `notebooks/explore_experiment_outputs.ipynb` | Four sections; each reads one of the materialized CSVs. |
| `baseline_comparison/` | Legacy hand-maintained `baseline_aggregates.csv` + notes (optional). |

Raw run outputs live under **`mec-streaming-framework/runs/`** (usually gitignored there).

## Quick start

1. In **mec-streaming-framework**, aggregate repetitions per experiment (see `runner/README.md`, INFRA-3.3):

   ```bash
   python -m runner aggregate-repetitions baseline_full_mec_c1
   ```

2. In **this repo** (sibling checkout of `mec-streaming-framework` recommended):

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   python scripts/materialize_experiment_tables.py \
     --runs-dir ../mec-streaming-framework/runs \
     --output-dir outputs/experiment_tables
   ```

3. Explore plots:

   ```bash
   jupyter lab notebooks/explore_experiment_outputs.ipynb
   ```

   Or use Docker:

   ```bash
   docker compose up
   ```

   Open the printed URL (token). Notebooks and `outputs/` are mounted under `/home/jovyan/work/`.

## Docker

- `docker-compose.yml` uses `jupyter/scipy-notebook` and mounts `./notebooks` and `./outputs`.
- To materialize **inside** the container, uncomment the optional `../mec-streaming-framework/runs:/data/runs:ro` volume and run the `materialize_experiment_tables.py` command with `--runs-dir /data/runs`.
