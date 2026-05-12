# Baseline Comparison (full_mec vs cloud_only)

This folder consolidates the six aggregated baseline runs:

- `baseline_full_mec_c1`, `baseline_full_mec_c2`, `baseline_full_mec_c5`
- `baseline_cloud_only_c1`, `baseline_cloud_only_c2`, `baseline_cloud_only_c5`

Source files used:

- `runs/<experiment_id>/aggregate.json`

Consolidated table:

- `baseline_aggregates.csv`

## Quick Read

- For `c1`, `cloud_only` is worse than `full_mec` in cloud traffic and cost, and has higher rebuffer.
- For `c2` and `c5`, aggregate means are effectively equal between `full_mec` and `cloud_only` for QoE/latency/rebuffer/bytes/cost in the current run set.
- `cache_commits_mean` differs by construction (`full_mec` publishes bootstrap actions; `cloud_only` does not).

## Pairwise deltas (cloud_only - full_mec)

| clients | delta qoe_avg | delta latency_ms_p95 | delta total_rebuffer_ms | delta total_bytes_cloud | delta total_cost_term |
|---:|---:|---:|---:|---:|---:|
| 1 | -0.0021481765 | 0.0 | +2758.9997923471 | +25147975.33333333 | +5.1917065156 |
| 2 | +0.0000000000 | 0.0 | +0.0000000000 | +0.0000000000 | +0.0000000000 |
| 5 | +0.0000000000 | 0.0 | +0.0000000000 | +0.0000000000 | +0.0000000000 |

## Notes

- `repetitions_found=6` for all experiments in this snapshot.
- Some metrics in each `aggregate.json` were computed from subsets (`n` per metric can differ from `repetitions_found`), so always check metric-specific `n` when doing statistical claims.
