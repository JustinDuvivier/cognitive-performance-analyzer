import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import psycopg2

from config.config import DB_CONFIG, PROJECT_ROOT

SCORE_COLS = ["sequence_memory_score", "reaction_time_ms", "verbal_memory_words"]

FACTOR_COLS = [
    "pressure_hpa", "pressure_change_24h", "temperature", "humidity",
    "hour_of_day", "day_of_week", "weekend", "pm25", "aqi", "co", "no",
    "no2", "o3", "so2", "pm10", "nh3", "sleep_hours", "phone_usage",
    "steps", "screen_time_minutes", "active_energy_kcal", "calories_intake",
    "protein_g", "carbs_g", "fat_g",
]


def _ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_from_db(limit: Optional[int] = None) -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
        SELECT m.*, p.name AS person_name, p.location_name
        FROM measurements m
        LEFT JOIN persons p ON m.person_id = p.person_id
        ORDER BY m.timestamp
    """
    if limit:
        query += " LIMIT %s"
        df = pd.read_sql(query, conn, params=(limit,))
    else:
        df = pd.read_sql(query, conn)
    conn.close()
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df


def compute_stats(df: pd.DataFrame, top_n: int = 10) -> Dict:
    present_scores = [c for c in SCORE_COLS if c in df.columns]
    present_factors = [c for c in FACTOR_COLS if c in df.columns]
    corr_pairs: List[Dict] = []
    corr_matrix = None

    if present_scores and present_factors:
        df_copy = df.copy()
        for col in present_scores + present_factors:
            if col in df_copy.columns and df_copy[col].dtype == "bool":
                df_copy[col] = df_copy[col].astype(int)
        sub = df_copy[present_scores + present_factors].select_dtypes(include="number")
        if not sub.empty:
            corr = sub.corr()
            avail_scores = [c for c in present_scores if c in corr.index]
            avail_factors = [c for c in present_factors if c in corr.columns]
            if avail_scores and avail_factors:
                corr_matrix = corr.loc[avail_scores, avail_factors].fillna(0)
                for s in avail_scores:
                    for f in avail_factors:
                        val = corr.loc[s, f]
                        if pd.notna(val):
                            corr_pairs.append({"pair": (s, f), "corr": float(abs(val))})
                corr_pairs = sorted(corr_pairs, key=lambda x: x["corr"], reverse=True)[:top_n]

    return {
        "row_count": len(df),
        "column_count": len(df.columns),
        "top_corr_pairs": corr_pairs,
        "missing_fraction": df.isna().mean().to_dict(),
        "corr_matrix": corr_matrix.to_dict() if corr_matrix is not None else None,
    }


def plot_correlation(stats: Dict, out_path: Path) -> Optional[Path]:
    corr_dict = stats.get("corr_matrix")
    if not corr_dict:
        return None
    import matplotlib.pyplot as plt
    corr = pd.DataFrame(corr_dict).fillna(0)
    if corr.empty:
        return None
    
    fig, ax = plt.subplots(figsize=(max(12, corr.shape[1] * 1.2), max(8, corr.shape[0] * 1.5)))
    im = ax.imshow(corr, cmap="coolwarm", vmin=-1, vmax=1, aspect='auto')
    cbar = plt.colorbar(im, fraction=0.046, pad=0.04)
    cbar.set_label("Correlation", fontsize=12)
    
    ax.set_xticks(range(len(corr.columns)))
    ax.set_yticks(range(len(corr.index)))
    ax.set_xticklabels(corr.columns, rotation=45, ha="right", fontsize=11)
    ax.set_yticklabels(corr.index, fontsize=12)
    
    for i in range(len(corr.index)):
        for j in range(len(corr.columns)):
            val = corr.iloc[i, j]
            if pd.isna(val):
                val = 0
            color = "white" if abs(val) > 0.5 else "black"
            ax.text(j, i, f"{val:.2f}", ha="center", va="center", fontsize=10, fontweight="bold", color=color)
    
    ax.set_title("Correlation: Cognitive Scores vs Factors", fontsize=16, fontweight="bold", pad=15)
    plt.tight_layout()
    plt.savefig(out_path, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')
    plt.close()
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int)
    parser.add_argument("--output-dir", default="reports/insights")
    parser.add_argument("--top-n", type=int, default=10)
    args = parser.parse_args()

    output_dir = PROJECT_ROOT / args.output_dir
    _ensure_output_dir(output_dir)

    df = load_from_db(limit=args.limit)
    stats = compute_stats(df, top_n=args.top_n)

    plot_path = plot_correlation(stats, output_dir / "correlation_heatmap.png")

    payload = {"summary": {"rows": stats["row_count"]}, "top_correlations": stats["top_corr_pairs"], "plots": [str(plot_path)] if plot_path else []}
    (output_dir / "ai_prompt_payload.json").write_text(json.dumps(payload, indent=2))

    print(f"Done. Payload: {output_dir / 'ai_prompt_payload.json'}")
    if plot_path:
        print(f"Plot: {plot_path}")


if __name__ == "__main__":
    main()
