import json
import os
import textwrap
from datetime import datetime

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from openai import OpenAI

from config.config import PROJECT_ROOT

REPORTS_DIR = PROJECT_ROOT / "reports" / "insights"


def get_ai_summary(payload: dict) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return "[AI summary unavailable - OPENAI_API_KEY not set]"

    top_corr = json.dumps(payload.get("top_correlations", []), indent=2)
    rows = payload.get("summary", {}).get("rows", "?")

    prompt = f"""
You are a data analyst creating a professional report. Summarize correlations between cognitive scores and environmental/behavioral factors.

Dataset: {rows} measurements
Top correlations (absolute values):
{top_corr}

Format your response EXACTLY like this:
KEY FINDINGS
• [First key finding - one clear sentence]
• [Second key finding - one clear sentence]
• [Third key finding - one clear sentence]

STRONGEST CORRELATIONS
• [Factor]: [correlation value] with [score] - [brief interpretation]
• [Factor]: [correlation value] with [score] - [brief interpretation]

LIMITATIONS
• [Note about sample size or data quality if relevant]

Keep each bullet point to one line. Be concise and professional.
"""

    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
    )
    return resp.choices[0].message.content


def wrap_text(text: str, width: int = 90) -> str:
    lines = text.split('\n')
    wrapped = []
    for line in lines:
        if line.strip().startswith(('KEY', 'STRONGEST', 'LIMITATIONS', 'NOTABLE', 'RECOMMENDATIONS')):
            wrapped.append('\n' + line)
        elif len(line) > width:
            wrapped.extend(textwrap.wrap(line, width=width))
        else:
            wrapped.append(line)
    return '\n'.join(wrapped)


def create_pdf_report():
    payload_path = REPORTS_DIR / "ai_prompt_payload.json"
    heatmap_path = REPORTS_DIR / "correlation_heatmap.png"
    pdf_path = REPORTS_DIR / "brain_fog_report.pdf"

    if not payload_path.exists():
        raise FileNotFoundError(f"Run generate_insights.py first. Missing: {payload_path}")

    payload = json.loads(payload_path.read_text())
    print("Generating AI summary...")
    ai_summary = get_ai_summary(payload)
    print("AI summary complete.")

    date_str = datetime.now().strftime("%B %d, %Y")
    rows = payload.get("summary", {}).get("rows", "?")

    with PdfPages(pdf_path) as pdf:
        # Page 1: Full-size heatmap
        if heatmap_path.exists():
            fig1 = plt.figure(figsize=(11, 8.5))
            fig1.patch.set_facecolor('white')
            
            fig1.text(0.5, 0.97, "Brain Fog Analysis Report", ha="center", fontsize=20, fontweight="bold", color="#2c3e50")
            fig1.text(0.5, 0.94, f"Generated: {date_str}  •  Total Measurements: {rows}", ha="center", fontsize=10, color="#7f8c8d")
            
            ax1 = fig1.add_axes([0.02, 0.10, 0.96, 0.82])
            img = plt.imread(str(heatmap_path))
            ax1.imshow(img, interpolation='lanczos')
            ax1.axis("off")
            
            legend_text = (
                "Color Guide:   "
                "Red = Positive correlation (X ↑ → Y ↑)   •   "
                "Blue = Negative correlation (X ↑ → Y ↓)   •   "
                "White = Weak/no correlation"
            )
            fig1.text(0.5, 0.04, legend_text, ha="center", fontsize=9, color="#34495e", style="italic")
            
            pdf.savefig(fig1, facecolor='white', dpi=300)
            plt.close(fig1)

        # Page 2: AI Analysis
        fig2 = plt.figure(figsize=(11, 8.5))
        fig2.patch.set_facecolor('#f8f9fa')
        
        fig2.text(0.5, 0.95, "AI Analysis Summary", ha="center", fontsize=22, fontweight="bold", color="#2c3e50")
        fig2.text(0.5, 0.90, "Automated insights generated from correlation data", ha="center", fontsize=10, color="#7f8c8d")
        
        ax_line = fig2.add_axes([0.1, 0.87, 0.8, 0.001])
        ax_line.axhline(y=0, color='#bdc3c7', linewidth=2)
        ax_line.axis('off')
        
        ax_text = fig2.add_axes([0.08, 0.08, 0.84, 0.77])
        ax_text.axis("off")
        
        formatted_text = wrap_text(ai_summary, width=95)
        ax_text.text(
            0, 1, formatted_text,
            fontsize=11,
            va="top",
            ha="left",
            transform=ax_text.transAxes,
            family="sans-serif",
            linespacing=1.5,
            color="#2c3e50"
        )
        
        fig2.text(0.5, 0.03, "This report was generated automatically using AI-powered analysis.", 
                  ha="center", fontsize=8, style="italic", color="#95a5a6")
        
        pdf.savefig(fig2, facecolor=fig2.get_facecolor())
        plt.close(fig2)

    print(f"\nReport saved: {pdf_path}")
    return pdf_path


if __name__ == "__main__":
    create_pdf_report()
