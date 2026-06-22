#!/usr/bin/env python3
"""
Materialize CSV tables from mec-streaming-framework runs/ for analytics notebooks.

Reads aggregate.json per experiment and optional episode CSVs under each run.
Writes 01–04 CSV files + manifest.json into --output-dir.

Usage (from analytics-experiments repo root):
  python scripts/materialize_experiment_tables.py \\
    --runs-dir ../mec-streaming-framework/runs \\
    --output-dir outputs/experiment_tables
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import pandas as pd
except ImportError as e:  # pragma: no cover
    print("pandas is required: pip install -r requirements.txt", file=sys.stderr)
    raise SystemExit(1) from e

CANONICAL_MODES = frozenset({"full_mec", "cloud_only", "optimized"})

F01 = "01_qoe_cost_by_cache_and_clients.csv"
F02 = "02_reward_components_by_cache_and_clients.csv"
F03 = "03_qoe_cost_vs_clients.csv"
F04 = "04_baseline_bitrate_evolution_by_step.csv"
MANIFEST = "manifest.json"


def _default_runs_dir() -> Path:
    root = Path(__file__).resolve().parents[1]
    return (root / ".." / "mec-streaming-framework" / "runs").resolve()


def _parse_clients_from_id(experiment_id: str) -> int | None:
    m = re.search(r"_c(\d+)$", experiment_id.strip())
    if m:
        return int(m.group(1))
    return None


def _cache_mode_from_experiment_id(experiment_id: str) -> str | None:
    e = experiment_id.lower()
    if "full_mec" in e or "_full_mec_" in e or e.endswith("full_mec"):
        return "full_mec"
    if "cloud_only" in e:
        return "cloud_only"
    if "optimized" in e:
        return "optimized"
    return None


def _load_env_cache_mode(exp_dir: Path) -> str | None:
    """First run dir under exp_dir with configs/env.json -> CACHE_MODE."""
    if not exp_dir.is_dir():
        return None
    for child in sorted(exp_dir.iterdir()):
        if not child.is_dir():
            continue
        env_path = child / "configs" / "env.json"
        if not env_path.is_file():
            continue
        try:
            env = json.loads(env_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if isinstance(env, dict):
            cm = env.get("CACHE_MODE")
            if cm is not None:
                s = str(cm).strip().lower()
                if s in CANONICAL_MODES:
                    return s
    return None


def _load_clients_from_env(exp_dir: Path) -> int | None:
    for child in sorted(exp_dir.iterdir()):
        if not child.is_dir():
            continue
        env_path = child / "configs" / "env.json"
        if not env_path.is_file():
            continue
        try:
            env = json.loads(env_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if not isinstance(env, dict):
            continue
        v = env.get("CLIENTS_PER_NODE")
        if v is not None and str(v).strip() != "":
            try:
                return int(float(v))
            except (TypeError, ValueError):
                pass
    return None


def resolve_cache_mode(experiment_id: str, exp_dir: Path) -> str:
    cm = _cache_mode_from_experiment_id(experiment_id)
    if cm:
        return cm
    cm2 = _load_env_cache_mode(exp_dir)
    if cm2:
        return cm2
    print(f"[warn] cache_mode unknown for experiment_id={experiment_id}", file=sys.stderr)
    return "unknown"


def resolve_clients(experiment_id: str, exp_dir: Path) -> int | None:
    c = _load_clients_from_env(exp_dir)
    if c is not None:
        return c
    return _parse_clients_from_id(experiment_id)


def included_run_id_names(exp_dir: Path, agg: dict[str, Any] | None = None) -> set[str] | None:
    """
    When aggregate.json lists repetition_run_ids, restrict episode CSV scans to those runs.
    """
    if agg is None:
        agg_path = exp_dir / "aggregate.json"
        if not agg_path.is_file():
            return None
        try:
            agg = json.loads(agg_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None
    if not isinstance(agg, dict):
        return None
    ids = agg.get("repetition_run_ids") or agg.get("repetitions_included")
    if not isinstance(ids, list) or not ids:
        return None
    out = {str(x).strip() for x in ids if str(x).strip()}
    return out or None


def included_run_ids_from_aggregate(agg: dict[str, Any]) -> set[str] | None:
    """
    Prefer repetition_run_ids from aggregate.json so CSV tables (02/04) match 01/03.
    """
    for key in ("repetition_run_ids", "repetitions_included"):
        raw = agg.get(key)
        if isinstance(raw, list) and raw:
            return {str(x).strip() for x in raw if x is not None and str(x).strip()}
    return None


def iter_run_dirs(exp_dir: Path, *, run_ids: set[str] | None = None) -> list[Path]:
    children = [c for c in sorted(exp_dir.iterdir()) if c.is_dir()]
    if run_ids is None:
        return children
    return [c for c in children if c.name in run_ids]


def included_run_ids_from_aggregate(exp_dir: Path) -> set[str] | None:
    """Prefer repetition_run_ids from aggregate.json so 02/04 match 01/03."""
    agg_path = exp_dir / "aggregate.json"
    if not agg_path.is_file():
        return None
    try:
        agg = json.loads(agg_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(agg, dict):
        return None
    ids = agg.get("repetition_run_ids") or agg.get("repetitions_included")
    if not isinstance(ids, list):
        return None
    out = {str(x).strip() for x in ids if x is not None and str(x).strip()}
    return out or None


def _iter_run_dirs(exp_dir: Path, *, run_ids: set[str] | None) -> list[Path]:
    children: list[Path] = []
    for child in sorted(exp_dir.iterdir()):
        if not child.is_dir():
            continue
        if run_ids is not None and child.name not in run_ids:
            continue
        children.append(child)
    return children


def discover_experiments(runs_dir: Path, filter_ids: set[str] | None) -> list[Path]:
    out: list[Path] = []
    if not runs_dir.is_dir():
        return out
    for p in sorted(runs_dir.iterdir()):
        if not p.is_dir():
            continue
        if not (p / "aggregate.json").is_file():
            continue
        eid = p.name
        if filter_ids is not None and eid not in filter_ids:
            continue
        out.append(p)
    return out


def flatten_aggregate_row(
    experiment_id: str,
    cache_mode: str,
    clients: int | None,
    agg: dict[str, Any],
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "experiment_id": experiment_id,
        "cache_mode": cache_mode,
        "clients": clients if clients is not None else "",
        "repetitions_found": agg.get("repetitions_found", ""),
    }
    metrics = agg.get("metrics")
    if not isinstance(metrics, dict):
        return row
    for key, stats in metrics.items():
        if not isinstance(stats, dict):
            continue
        for sub in ("mean", "std", "n", "ic95_low", "ic95_high"):
            v = stats.get(sub)
            if v is None or (isinstance(v, float) and math.isnan(v)):
                row[f"{key}_{sub}"] = ""
            else:
                row[f"{key}_{sub}"] = v
    return row


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        if fieldnames:
            with path.open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                w.writeheader()
        return 0
    if fieldnames is None:
        fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
    return len(rows)


def build_file_01_fieldnames(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return [
            "experiment_id",
            "cache_mode",
            "clients",
            "repetitions_found",
            "qoe_avg_mean",
            "qoe_avg_std",
            "qoe_avg_n",
            "total_cost_term_mean",
            "total_cost_term_std",
            "total_cost_term_n",
            "reward_avg_mean",
            "reward_avg_std",
            "reward_avg_n",
        ]
    ordered = ["experiment_id", "cache_mode", "clients", "repetitions_found"]
    rest: set[str] = set()
    for r in rows:
        for k in r:
            if k not in ordered:
                rest.add(k)
    metric_keys = sorted(rest, key=lambda x: (x.rsplit("_", 1)[0], x))
    return ordered + metric_keys


def build_file_03_from_01(rows01: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows01:
        eid = r.get("experiment_id", "")
        cm = r.get("cache_mode", "")
        clients = r.get("clients", "")
        for metric in ("qoe_avg", "total_cost_term", "total_transmission_cost_raw", "total_bytes_cloud"):
            mean = r.get(f"{metric}_mean", "")
            std = r.get(f"{metric}_std", "")
            n = r.get(f"{metric}_n", "")
            out.append(
                {
                    "experiment_id": eid,
                    "cache_mode": cm,
                    "clients": clients,
                    "metric": metric,
                    "mean": mean,
                    "std": std,
                    "n": n,
                }
            )
    return out


def per_run_reward_component_means(
    exp_dir: Path,
    *,
    run_ids: set[str] | None = None,
) -> tuple[list[dict[str, float]], list[str]]:
    """
    For each run under exp_dir, mean of each numeric key in reward_terms_json across all episode rows.
    Returns (list of per-run dicts, sorted union of component keys).
    """
    per_run: list[dict[str, float]] = []
    all_keys: set[str] = set()
    for child in _iter_run_dirs(exp_dir, run_ids=run_ids):
        ep_dir = child / "metrics" / "episodes"
        if not ep_dir.is_dir():
            continue
        sums: dict[str, float] = defaultdict(float)
        counts: dict[str, int] = defaultdict(int)
        any_row = False
        for csv_path in sorted(ep_dir.glob("episode_*.csv")):
            try:
                df = pd.read_csv(csv_path, usecols=["reward_terms_json"], dtype=str)
            except (ValueError, OSError):
                continue
            if "reward_terms_json" not in df.columns:
                continue
            for cell in df["reward_terms_json"].dropna():
                any_row = True
                try:
                    terms = json.loads(str(cell))
                except json.JSONDecodeError:
                    continue
                if not isinstance(terms, dict):
                    continue
                for k, v in terms.items():
                    if isinstance(v, bool):
                        continue
                    try:
                        fv = float(v)
                    except (TypeError, ValueError):
                        continue
                    if math.isnan(fv):
                        continue
                    sums[k] += fv
                    counts[k] += 1
                    all_keys.add(str(k))
        if any_row and counts:
            means = {k: sums[k] / counts[k] for k in sums}
            per_run.append(means)
    return per_run, sorted(all_keys)


def build_file_02_rows(
    experiment_id: str,
    cache_mode: str,
    clients: int | None,
    exp_dir: Path,
    *,
    run_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    per_run, keys = per_run_reward_component_means(exp_dir, run_ids=run_ids)
    if not per_run or not keys:
        return []
    rows: list[dict[str, Any]] = []
    for comp in keys:
        vals = [pr[comp] for pr in per_run if comp in pr]
        if not vals:
            continue
        m = sum(vals) / len(vals)
        if len(vals) > 1:
            var = sum((x - m) ** 2 for x in vals) / (len(vals) - 1)
            sd = math.sqrt(var)
        else:
            sd = 0.0
        rows.append(
            {
                "experiment_id": experiment_id,
                "cache_mode": cache_mode,
                "clients": clients if clients is not None else "",
                "component": comp,
                "mean": m,
                "std": sd,
                "n": len(vals),
            }
        )
    return rows


def per_run_bitrate_by_step(
    exp_dir: Path,
    *,
    run_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Per run: list of dicts episode_id, step, bitrate_mean (over clients for that step).
    """
    series_per_run: list[dict[tuple[str, int], float]] = []
    for child in _iter_run_dirs(exp_dir, run_ids=run_ids):
        ep_dir = child / "metrics" / "episodes"
        if not ep_dir.is_dir():
            continue
        step_sums: dict[tuple[str, int], float] = defaultdict(float)
        step_counts: dict[tuple[str, int], int] = defaultdict(int)
        for csv_path in sorted(ep_dir.glob("episode_*.csv")):
            try:
                stem = csv_path.stem  # episode_1
                ep_id = stem.replace("episode_", "") if stem.startswith("episode_") else stem
                df = pd.read_csv(csv_path, usecols=["step", "action_json"], dtype=str)
            except (ValueError, OSError):
                continue
            if "step" not in df.columns or "action_json" not in df.columns:
                continue
            for _, row in df.iterrows():
                try:
                    step = int(row["step"])
                except (TypeError, ValueError):
                    continue
                try:
                    action = json.loads(str(row["action_json"]))
                except json.JSONDecodeError:
                    continue
                if not isinstance(action, dict):
                    continue
                br = action.get("selected_bitrate_kbps")
                if br is None:
                    continue
                try:
                    fv = float(br)
                except (TypeError, ValueError):
                    continue
                key = (str(ep_id), step)
                step_sums[key] += fv
                step_counts[key] += 1
        if not step_counts:
            continue
        run_means: dict[tuple[str, int], float] = {}
        for k, s in step_sums.items():
            run_means[k] = s / max(1, step_counts[k])
        series_per_run.append(run_means)
    return series_per_run


def build_file_04_rows(
    experiment_id: str,
    cache_mode: str,
    clients: int | None,
    exp_dir: Path,
    *,
    run_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    series_list = per_run_bitrate_by_step(exp_dir, run_ids=run_ids)
    if not series_list:
        return []
    all_keys: set[tuple[str, int]] = set()
    for s in series_list:
        all_keys.update(s.keys())
    rows: list[dict[str, Any]] = []
    for ep_id, step in sorted(all_keys, key=lambda x: (x[0], x[1])):
        vals = [s.get((ep_id, step)) for s in series_list if (ep_id, step) in s]
        vals = [v for v in vals if v is not None]
        if not vals:
            continue
        m = sum(vals) / len(vals)
        if len(vals) > 1:
            var = sum((x - m) ** 2 for x in vals) / (len(vals) - 1)
            sd = math.sqrt(var)
        else:
            sd = 0.0
        rows.append(
            {
                "experiment_id": experiment_id,
                "cache_mode": cache_mode,
                "clients": clients if clients is not None else "",
                "episode_id": ep_id,
                "step": step,
                "bitrate_kbps_mean": m,
                "bitrate_kbps_std": sd,
                "n_runs": len(vals),
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize experiment tables from framework runs/.")
    parser.add_argument(
        "--runs-dir",
        type=Path,
        default=_default_runs_dir(),
        help="Path to mec-streaming-framework/runs (default: ../mec-streaming-framework/runs from repo root)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to write CSVs and manifest (e.g. outputs/experiment_tables)",
    )
    parser.add_argument(
        "--experiment-id",
        action="append",
        dest="experiment_ids",
        default=None,
        help="Restrict to one or more experiment_id (repeatable)",
    )
    parser.add_argument(
        "--bitrate-experiment-regex",
        default=r"^baseline_",
        help="Only experiments matching this regex contribute to file 04 (default: ^baseline_)",
    )
    args = parser.parse_args()
    runs_dir: Path = args.runs_dir.resolve()
    out_dir: Path = args.output_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    filt: set[str] | None = None
    if args.experiment_ids:
        filt = set(args.experiment_ids)

    exp_paths = discover_experiments(runs_dir, filt)
    warnings: list[str] = []
    if not exp_paths:
        warnings.append(f"No experiments with aggregate.json under {runs_dir}")

    bitrate_pattern = re.compile(args.bitrate_experiment_regex)

    rows01: list[dict[str, Any]] = []
    rows02: list[dict[str, Any]] = []
    rows04: list[dict[str, Any]] = []

    for exp_dir in exp_paths:
        experiment_id = exp_dir.name
        agg_path = exp_dir / "aggregate.json"
        try:
            agg = json.loads(agg_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            warnings.append(f"{experiment_id}: failed to read aggregate.json: {e}")
            continue
        if not isinstance(agg, dict):
            warnings.append(f"{experiment_id}: aggregate.json not an object")
            continue

        cache_mode = resolve_cache_mode(experiment_id, exp_dir)
        clients = resolve_clients(experiment_id, exp_dir)
        included_run_ids = included_run_ids_from_aggregate(exp_dir)

        row = flatten_aggregate_row(experiment_id, cache_mode, clients, agg)
        rows01.append(row)

        r2 = build_file_02_rows(
            experiment_id, cache_mode, clients, exp_dir, run_ids=included_run_ids
        )
        if not r2:
            warnings.append(f"{experiment_id}: no episode reward_terms data for 02")
        rows02.extend(r2)

        if included_run_ids is not None:
            csv_runs = sum(
                1
                for rid in included_run_ids
                if (exp_dir / rid / "metrics" / "episodes").is_dir()
                and any((exp_dir / rid / "metrics" / "episodes").glob("episode_*.csv"))
            )
            if csv_runs != len(included_run_ids):
                warnings.append(
                    f"{experiment_id}: episode CSVs found for {csv_runs}/{len(included_run_ids)} "
                    "included repetition_run_ids (02/04 may be partial)"
                )

        if bitrate_pattern.search(experiment_id):
            r4 = build_file_04_rows(
                experiment_id, cache_mode, clients, exp_dir, run_ids=included_run_ids
            )
            if not r4:
                warnings.append(f"{experiment_id}: no episode bitrate series for 04")
            rows04.extend(r4)

    rows03 = build_file_03_from_01(rows01)

    f01_fields = build_file_01_fieldnames(rows01)
    n01 = len(rows01)
    p01 = out_dir / F01
    write_csv(p01, rows01, f01_fields)

    f03_fields = ["experiment_id", "cache_mode", "clients", "metric", "mean", "std", "n"]
    p03 = out_dir / F03
    write_csv(p03, rows03, f03_fields)

    p02 = out_dir / F02
    write_csv(p02, rows02, ["experiment_id", "cache_mode", "clients", "component", "mean", "std", "n"])

    p04 = out_dir / F04
    write_csv(
        p04,
        rows04,
        [
            "experiment_id",
            "cache_mode",
            "clients",
            "episode_id",
            "step",
            "bitrate_kbps_mean",
            "bitrate_kbps_std",
            "n_runs",
        ],
    )

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runs_dir": str(runs_dir),
        "output_dir": str(out_dir),
        "files": [
            {"name": F01, "rows": n01, "description": "QoE/cost and all aggregate metrics per experiment"},
            {"name": F03, "rows": len(rows03), "description": "Long-form qoe_avg and total_cost_term vs clients"},
            {"name": F02, "rows": len(rows02), "description": "Reward term component means with std across runs"},
            {"name": F04, "rows": len(rows04), "description": "Mean bitrate by step; std across runs (baseline regex)"},
        ],
        "warnings": warnings,
    }
    (out_dir / MANIFEST).write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote {p01} ({n01} rows), {out_dir / F02} ({len(rows02)}), {p03} ({len(rows03)}), {p04} ({len(rows04)})")
    if warnings:
        print(f"{len(warnings)} warning(s); see {out_dir / MANIFEST}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
