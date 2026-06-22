# Generated tables (`experiment_tables/`)

CSV files here are produced by the framework-side clone path + **analytics script**:

```bash
# from analytics-experiments repo root (sibling to mec-streaming-framework)
pip install -r requirements.txt
python scripts/materialize_experiment_tables.py \
  --runs-dir ../mec-streaming-framework/runs \
  --output-dir outputs/experiment_tables
```

| File | Purpose |
|------|---------|
| `01_qoe_cost_by_cache_and_clients.csv` | One row per experiment; aggregate metrics (`mean`/`std`/`n`/`ic95_*`) from each `runs/<id>/aggregate.json`. |
| `02_reward_components_by_cache_and_clients.csv` | Long format: `reward_terms` keys, mean and std **across runs** (repetitions). |
| `03_qoe_cost_vs_clients.csv` | Long format for `qoe_avg`, `total_cost_term`, `total_transmission_cost_raw`, `total_bytes_cloud` vs `clients`. |
| `04_baseline_bitrate_evolution_by_step.csv` | Mean `selected_bitrate_kbps` per `step` (mean over clients per run, then mean/std across runs); only experiments matching `--bitrate-experiment-regex` (default `^baseline_`). |
| `manifest.json` | Generation metadata and warnings. |

**Prerequisite:** for each experiment, run aggregation in the framework repo:

`python -m runner aggregate-repetitions <experiment_id>`

The aggregator includes only `summary_global.json` files with `experiences_count > 0`
(see `repetition_run_ids` in `aggregate.json`). Tables `02` and `04` use those same run
folders when `aggregate.json` lists `repetition_run_ids`.

See [runner README (aggregate-repetitions)](../../mec-streaming-framework/runner/README.md) in the sibling `mec-streaming-framework` clone (INFRA-3.3).

Notebooks under `../notebooks/` read these CSVs only (no direct access to `runs/` required once materialized).

## Metrics for cloud vs MEC comparison

After the **cloud miss delay** model update (emulator + export), use:

| Metric in `01_...csv` | Meaning |
|-----------------------|---------|
| **`total_transmission_cost_raw_mean`** | Sum per run of `reward_terms.cost_raw` (= `delay_factor × j × bytes`). **Primary cost chart** for cloud vs MEC. |
| **`total_bytes_cloud_mean`** | Bytes fetched from cloud path (volume). |
| **`total_cost_term_mean`** | Sum of normalized `reward_terms.cost` (0..1 per step). Penalizes cloud more after the fix, but can saturate at 1.0. |
| **`qoe_avg_mean`** | Mean QoE per experience; should diverge after player applies 10× delivery on cache miss. |

Legacy runs (before re-export) may lack `total_transmission_cost_raw` in `summary_global.json`; re-run baselines and `export-summaries`, then `aggregate-repetitions` + materialize.

## Re-run after emulator / export changes

```bash
cd mec-streaming-framework
source .venv/bin/activate
COMPOSE_BUILD=1 ./experiments/start_infra.sh   # if Docker images changed
./experiments/run_all_baselines.sh

for exp in baseline_full_mec_c1 baseline_full_mec_c2 baseline_full_mec_c5 \
           baseline_cloud_only_c1 baseline_cloud_only_c2 baseline_cloud_only_c5; do
  python -m runner aggregate-repetitions "$exp"
done

cd ../analytics-experiments && source .venv/bin/activate
python scripts/materialize_experiment_tables.py \
  --runs-dir ../mec-streaming-framework/runs \
  --output-dir outputs/experiment_tables
```
