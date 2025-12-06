import json
import os
from pathlib import Path

from openai import OpenAI

from config.config import PROJECT_ROOT


def load_payload(path: Path) -> dict:
    return json.loads(path.read_text())


def build_prompt(payload: dict) -> str:
    top_corr = json.dumps(payload.get("top_correlations", []), indent=2)
    plots = payload.get("plots", [])
    plot_note = plots[0] if plots else "N/A"
    rows = payload.get("summary", {}).get("rows")
    cols = payload.get("summary", {}).get("columns")
    scores = payload.get("score_columns_used", [])
    factors = payload.get("factor_columns_used", [])

    return f"""
You are a data analyst. Provide a concise summary of correlations between cognitive scores and factors.

Rows in dataset: {rows}, Columns: {cols}
Scores considered: {scores}
Factors considered: {factors}

Top correlations (abs value):
{top_corr}

Heatmap location: {plot_note}

Guidelines:
- Use only the provided correlations; do not invent data.
- Highlight strongest positive/negative relationships and note if sample size is small.
- Keep it short and bullet-style.
"""


def main():
    payload_path = PROJECT_ROOT / "reports/insights/ai_prompt_payload.json"
    if not payload_path.exists():
        raise FileNotFoundError(f"Payload not found at {payload_path}. Run generate_insights.py first.")

    payload = load_payload(payload_path)
    prompt = build_prompt(payload)

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set. Add it to your environment or .env file.")

    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
    )
    print(resp.choices[0].message.content)


if __name__ == "__main__":
    main()
