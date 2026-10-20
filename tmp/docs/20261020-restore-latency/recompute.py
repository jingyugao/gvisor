"""Recompute report CSV files from preserved, non-secret evidence.

Run in this directory: uv run --no-project python recompute.py
Uses only Python's standard library. No network requests or sandbox operations.
The 70 individual final-ablation samples are unavailable: only differences of
preserved aggregates can be checked. All 40 CoCell observations are present.
"""
import csv
import hashlib
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent
provenance = json.loads((ROOT / "provenance.json").read_text())
for filename, expected in provenance["inputSha256"].items():
    actual = hashlib.sha256((ROOT / filename).read_bytes()).hexdigest()
    assert actual == expected, f"Input checksum mismatch: {filename}"
aggregates = json.loads((ROOT / "recorded-final-aggregates.json").read_text())
paired = json.loads((ROOT / "recorded-final-paired.json").read_text())
with (ROOT / "cocell-samples.csv").open() as stream:
    historical = [
        {
            "round": int(row["round"]),
            "strategy": row["strategy"],
            "first_provider_request_ms": float(row["first_provider_request_ms"]),
            "turn_completed_ms": float(row["turn_completed_ms"]),
        }
        for row in csv.DictReader(stream)
    ]

labels = {
    "cold-root-eager": ("无预创建池 / root / eager", "无池 · root · eager"),
    "warm-root-eager": ("预创建池 / root / eager", "池 · root · eager"),
    "warm-root-lazy": ("预创建池 / root / lazy", "池 · root · lazy"),
    "warm-noroot-eager": ("预创建池 / no-root / eager", "池 · no-root · eager"),
    "warm-noroot-lazy": ("预创建池 / no-root / lazy（默认组合）", "池 · no-root · lazy"),
    "warm-noroot-lazy-prefetch": ("默认组合 + 并行整文件预读", "＋并行整文件预读"),
    "warm-noroot-lazy-cache-hot": ("默认组合 + 提前全量预热", "＋提前全量预热"),
}
rows = []
for mode, stats in aggregates.items():
    health = stats["triggerToHealthMs"]
    rows.append({
        "mode": mode, "configuration": labels[mode][0], "shortLabel": labels[mode][1],
        "n": stats["n"],
        "operationMeanMs": stats["triggerToOperationMs"]["meanMs"],
        "healthMeanMs": health["meanMs"],
        "stateMeanMs": stats["triggerToStateMs"]["meanMs"],
        "healthMedianMs": health["medianMs"],
        "healthMinMs": health["minMs"], "healthMaxMs": health["maxMs"],
        "healthRangeMs": f'{health["minMs"]:.2f}–{health["maxMs"]:.2f}',
        "advancePrefetchMeanMs": stats.get("advancePrefetchMeanMs"),
        "prefetchPlusHealthMeanMs": stats.get("prefetchPlusHealthMeanMs"),
        "prefetchPlusStateMeanMs": stats.get("prefetchPlusStateMeanMs"),
    })
by_mode = {row["mode"]: row for row in rows}
steps = []
specs = [
    ("加入预创建池", "cold-root-eager", "warm-root-eager"),
    ("再关闭内部后台页扫描（lazy）", "warm-root-eager", "warm-root-lazy"),
    ("再使用 no-root / 延迟绑定 checkpoint", "warm-root-lazy", "warm-noroot-lazy"),
    ("可选：再加并行整文件预读", "warm-noroot-lazy", "warm-noroot-lazy-prefetch"),
]
for index, (name, before, after) in enumerate(specs):
    old, new = by_mode[before], by_mode[after]
    saved = old["healthMeanMs"] - new["healthMeanMs"]
    retained = paired[index]["metrics"]
    assert abs(saved - retained["triggerToHealthMs"]["meanSavedMs"]) < 1e-7
    steps.append({
        "step": name, "beforeMode": before, "afterMode": after,
        "beforeHealthMs": old["healthMeanMs"], "afterHealthMs": new["healthMeanMs"],
        "healthSavedMs": saved,
        "operationSavedMs": old["operationMeanMs"] - new["operationMeanMs"],
        "stateSavedMs": old["stateMeanMs"] - new["stateMeanMs"],
        "healthReductionPercent": saved / old["healthMeanMs"] * 100,
        "fasterRounds": f'{retained["triggerToHealthMs"]["wins"]}/10',
        "pairedMedianHealthSavedMs": retained["triggerToHealthMs"]["medianSavedMs"],
    })

history_labels = {
    "master": "原版内部后台加载",
    "lazy-only": "仅按需加载",
    "lazy-full-512": "按需加载 + 整文件预读",
    "lazy-hot-512": "按需加载 + hotset 预读",
}
hist_rows = []
for strategy, label in history_labels.items():
    vals = [row["first_provider_request_ms"] for row in historical if row["strategy"] == strategy]
    assert len(vals) == 10
    hist_rows.append({
        "strategy": strategy, "configuration": label, "n": len(vals),
        "medianDispatchMs": statistics.median(vals), "meanDispatchMs": statistics.mean(vals),
        "minDispatchMs": min(vals), "maxDispatchMs": max(vals),
    })
hist_by_mode = {row["strategy"]: row for row in hist_rows}
hist_pairs = []
for before, after in [("master", "lazy-only"), ("lazy-only", "lazy-full-512"),
                      ("lazy-only", "lazy-hot-512"), ("master", "lazy-hot-512")]:
    old = {row["round"]: row["first_provider_request_ms"] for row in historical if row["strategy"] == before}
    diffs = [old[row["round"]] - row["first_provider_request_ms"]
             for row in historical if row["strategy"] == after]
    hist_pairs.append({
        "before": before, "after": after, "n": len(diffs),
        "differenceOfMediansMs": hist_by_mode[before]["medianDispatchMs"] - hist_by_mode[after]["medianDispatchMs"],
        "pairedMedianSavedMs": statistics.median(diffs), "pairedMeanSavedMs": statistics.mean(diffs),
        "fasterRounds": sum(value > 0 for value in diffs),
    })

for filename, values in [("latency-summary.csv", rows), ("step-effects.csv", steps),
                          ("cocell-summary.csv", hist_rows), ("cocell-paired.csv", hist_pairs)]:
    with (ROOT / filename).open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(values[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(values)
assert sum(row["n"] for row in rows) == 70
assert len(historical) == 40
assert len({(row["round"], row["strategy"]) for row in historical}) == 40
assert abs(sum(row["healthSavedMs"] for row in steps[:3]) -
           (by_mode["cold-root-eager"]["healthMeanMs"] - by_mode["warm-noroot-lazy"]["healthMeanMs"])) < 1e-7
for index, (_, before, after) in enumerate(specs):
    for field, original_field in [("operationMeanMs", "triggerToOperationMs"),
                                  ("healthMeanMs", "triggerToHealthMs"),
                                  ("stateMeanMs", "triggerToStateMs")]:
        delta = by_mode[before][field] - by_mode[after][field]
        assert abs(delta - paired[index]["metrics"][original_field]["meanSavedMs"]) < 1e-7
print("Verified 7 retained aggregate groups and recomputed 40 original CoCell timing observations.")
print("Rewrote 4 derived CSV files; cocell-samples.csv is an unchanged input.")
