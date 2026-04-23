from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any


def write_benchmark_report(benchmark: dict[str, Any], out_dir: Path) -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    strategies = [item for item in benchmark["strategies"] if item.get("status") != "ERROR"]

    json_path = out_dir / "benchmark_results.json"
    json_path.write_text(json.dumps(benchmark, ensure_ascii=False, indent=2), encoding="utf-8")

    summary_svg = out_dir / "summary_returns.svg"
    summary_svg.write_text(_render_return_bar_chart(strategies), encoding="utf-8")

    risk_svg = out_dir / "risk_return.svg"
    risk_svg.write_text(_render_risk_return_scatter(strategies), encoding="utf-8")

    equity_dir = out_dir / "equity_curves"
    equity_dir.mkdir(exist_ok=True)
    for strategy in strategies:
        equity_svg = equity_dir / f"{strategy['name']}.svg"
        equity_svg.write_text(_render_equity_curve_svg(strategy), encoding="utf-8")

    html_path = out_dir / "report.html"
    html_path.write_text(_render_html_report(benchmark), encoding="utf-8")

    return {
        "json": str(json_path),
        "summary_svg": str(summary_svg),
        "risk_svg": str(risk_svg),
        "html": str(html_path),
        "equity_dir": str(equity_dir),
    }


def write_walkforward_report(report: dict[str, Any], out_dir: Path) -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "walkforward_results.json"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    folds_svg = out_dir / "fold_returns.svg"
    folds_svg.write_text(_render_walkforward_fold_chart(report), encoding="utf-8")

    html_path = out_dir / "report.html"
    html_path.write_text(_render_walkforward_html(report), encoding="utf-8")

    return {
        "json": str(json_path),
        "folds_svg": str(folds_svg),
        "html": str(html_path),
    }


def _render_return_bar_chart(strategies: list[dict[str, Any]]) -> str:
    ranked = sorted(strategies, key=lambda item: item["metrics"]["total_return_pct"], reverse=True)
    width = 1400
    row_height = 28
    top_padding = 60
    left_margin = 260
    right_margin = 60
    bottom_padding = 40
    chart_width = width - left_margin - right_margin
    height = top_padding + bottom_padding + (len(ranked) * row_height)
    values = [item["metrics"]["total_return_pct"] for item in ranked]
    min_value = min(values + [0.0])
    max_value = max(values + [0.0])
    spread = max(max_value - min_value, 1.0)
    zero_x = left_margin + ((0 - min_value) / spread) * chart_width

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<style>text{font-family:Menlo,Consolas,monospace;fill:#102a43} .small{font-size:12px} .label{font-size:13px} .title{font-size:22px;font-weight:700} .axis{stroke:#bcccdc;stroke-width:1}</style>',
        f'<rect width="{width}" height="{height}" fill="#f8fbff"/>',
        '<text x="28" y="34" class="title">All Built-in Strategies: Total Return</text>',
        '<text x="28" y="54" class="small">Sorted by total return %, green is positive, red is negative.</text>',
        f'<line x1="{zero_x:.2f}" y1="{top_padding - 8}" x2="{zero_x:.2f}" y2="{height - bottom_padding + 8}" class="axis"/>',
    ]

    for index, strategy in enumerate(ranked):
        y = top_padding + (index * row_height)
        value = strategy["metrics"]["total_return_pct"]
        value_x = left_margin + ((value - min_value) / spread) * chart_width
        x = min(value_x, zero_x)
        bar_width = abs(value_x - zero_x)
        color = "#2d6a4f" if value >= 0 else "#c1121f"
        parts.append(f'<text x="20" y="{y + 18}" class="label">{html.escape(strategy["name"])}</text>')
        parts.append(f'<rect x="{x:.2f}" y="{y + 6}" width="{bar_width:.2f}" height="14" rx="3" fill="{color}" opacity="0.88"/>')
        label_x = value_x + 8 if value >= 0 else value_x - 68
        parts.append(f'<text x="{label_x:.2f}" y="{y + 18}" class="small">{value:.2f}%</text>')

    parts.append("</svg>")
    return "".join(parts)


def _render_risk_return_scatter(strategies: list[dict[str, Any]]) -> str:
    width = 980
    height = 720
    left = 90
    right = 40
    top = 60
    bottom = 70
    chart_width = width - left - right
    chart_height = height - top - bottom
    x_values = [item["metrics"]["max_drawdown_pct"] for item in strategies]
    y_values = [item["metrics"]["total_return_pct"] for item in strategies]
    max_x = max(x_values + [1.0]) * 1.15
    min_y = min(y_values + [0.0])
    max_y = max(y_values + [0.0])
    y_span = max(max_y - min_y, 1.0)

    def x_map(value: float) -> float:
        return left + (value / max_x) * chart_width

    def y_map(value: float) -> float:
        return top + ((max_y - value) / y_span) * chart_height

    zero_y = y_map(0.0)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<style>text{font-family:Menlo,Consolas,monospace;fill:#102a43} .small{font-size:12px} .title{font-size:22px;font-weight:700} .axis{stroke:#9fb3c8;stroke-width:1} .grid{stroke:#d9e2ec;stroke-width:1}</style>',
        f'<rect width="{width}" height="{height}" fill="#f8fbff"/>',
        '<text x="24" y="34" class="title">Risk vs Return</text>',
        '<text x="24" y="54" class="small">X = max drawdown %, Y = total return %.</text>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{height - bottom}" class="axis"/>',
        f'<line x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}" class="axis"/>',
        f'<line x1="{left}" y1="{zero_y:.2f}" x2="{width - right}" y2="{zero_y:.2f}" class="grid"/>',
    ]

    for step in range(1, 6):
        x = left + chart_width * step / 5
        label = max_x * step / 5
        parts.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{height - bottom}" class="grid"/>')
        parts.append(f'<text x="{x - 8:.2f}" y="{height - bottom + 20}" class="small">{label:.1f}</text>')

    for step in range(6):
        value = min_y + (y_span * step / 5)
        y = y_map(value)
        parts.append(f'<line x1="{left}" y1="{y:.2f}" x2="{width - right}" y2="{y:.2f}" class="grid"/>')
        parts.append(f'<text x="16" y="{y + 4:.2f}" class="small">{value:.1f}</text>')

    for strategy in strategies:
        x = x_map(strategy["metrics"]["max_drawdown_pct"])
        y = y_map(strategy["metrics"]["total_return_pct"])
        color = "#2d6a4f" if strategy["metrics"]["total_return_pct"] >= 0 else "#c1121f"
        radius = max(4, min(12, 3 + strategy["metrics"]["trade_count"] / 4))
        parts.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="{radius:.2f}" fill="{color}" opacity="0.72"/>')
        parts.append(f'<text x="{x + 8:.2f}" y="{y - 8:.2f}" class="small">{html.escape(strategy["name"])}</text>')

    parts.append(f'<text x="{width / 2 - 80:.2f}" y="{height - 18}" class="small">Max Drawdown %</text>')
    parts.append(f'<text transform="translate(20 {height / 2:.2f}) rotate(-90)" class="small">Total Return %</text>')
    parts.append("</svg>")
    return "".join(parts)


def _render_equity_curve_svg(strategy: dict[str, Any]) -> str:
    curve = strategy.get("equity_curve", [])
    width = 1200
    height = 420
    left = 70
    right = 30
    top = 48
    bottom = 40
    chart_width = width - left - right
    chart_height = height - top - bottom
    equities = [item["equity"] for item in curve]
    min_equity = min(equities) if equities else 0.0
    max_equity = max(equities) if equities else 1.0
    span = max(max_equity - min_equity, 1.0)

    def x_map(index: int) -> float:
        if len(curve) <= 1:
            return left
        return left + (index / (len(curve) - 1)) * chart_width

    def y_map(value: float) -> float:
        return top + ((max_equity - value) / span) * chart_height

    line_points = " ".join(f"{x_map(index):.2f},{y_map(point['equity']):.2f}" for index, point in enumerate(curve))
    final_return = strategy["metrics"]["total_return_pct"]
    color = "#2d6a4f" if final_return >= 0 else "#c1121f"

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<style>text{font-family:Menlo,Consolas,monospace;fill:#102a43} .small{font-size:12px} .title{font-size:20px;font-weight:700} .axis{stroke:#bcccdc;stroke-width:1} .grid{stroke:#e6eef6;stroke-width:1}</style>',
        f'<rect width="{width}" height="{height}" fill="#ffffff"/>',
        f'<text x="20" y="28" class="title">{html.escape(strategy["title"])}</text>',
        f'<text x="20" y="44" class="small">{html.escape(strategy["name"])} | return {final_return:.2f}% | drawdown {strategy["metrics"]["max_drawdown_pct"]:.2f}% | trades {strategy["metrics"]["trade_count"]}</text>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{height - bottom}" class="axis"/>',
        f'<line x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}" class="axis"/>',
    ]

    for step in range(5):
        value = min_equity + (span * step / 4)
        y = y_map(value)
        parts.append(f'<line x1="{left}" y1="{y:.2f}" x2="{width - right}" y2="{y:.2f}" class="grid"/>')
        parts.append(f'<text x="8" y="{y + 4:.2f}" class="small">{value:.0f}</text>')

    if line_points:
        parts.append(f'<polyline fill="none" stroke="{color}" stroke-width="2.5" points="{line_points}"/>')

    parts.append("</svg>")
    return "".join(parts)


def _render_html_report(benchmark: dict[str, Any]) -> str:
    strategies = [item for item in benchmark["strategies"] if item.get("status") != "ERROR"]
    failed = [item for item in benchmark["strategies"] if item.get("status") == "ERROR"]

    rows = []
    for strategy in sorted(strategies, key=lambda item: item["metrics"]["total_return_pct"], reverse=True):
        metrics = strategy["metrics"]
        chart_path = f"equity_curves/{strategy['name']}.svg"
        rows.append(
            f"""
            <section class="card">
              <div class="head">
                <div>
                  <h2>{html.escape(strategy['title'])}</h2>
                  <p class="meta">{html.escape(strategy['name'])} | {html.escape(strategy['category'])}</p>
                </div>
                <div class="pill {'pos' if metrics['total_return_pct'] >= 0 else 'neg'}">{metrics['total_return_pct']:.2f}%</div>
              </div>
              <div class="stats">
                <div><strong>Profit Factor</strong><span>{metrics['profit_factor']}</span></div>
                <div><strong>Max Drawdown</strong><span>{metrics['max_drawdown_pct']:.2f}%</span></div>
                <div><strong>Sharpe</strong><span>{metrics['sharpe']}</span></div>
                <div><strong>Trades</strong><span>{metrics['trade_count']}</span></div>
                <div><strong>Fees</strong><span>{metrics['fees_paid']:.2f}</span></div>
                <div><strong>Exposure</strong><span>{metrics['exposure_pct']:.2f}%</span></div>
              </div>
              <img src="{html.escape(chart_path)}" alt="{html.escape(strategy['name'])} equity curve" />
              <p class="desc">{html.escape(strategy['description'])}</p>
            </section>
            """
        )

    failed_html = ""
    if failed:
        items = "".join(f"<li>{html.escape(item['name'])}: {html.escape(item['error'])}</li>" for item in failed)
        failed_html = f"<section class='errors'><h2>Failures</h2><ul>{items}</ul></section>"

    return f"""
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8" />
      <title>BinanceTrade Benchmark Report</title>
      <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background: #f4f7fb; color: #102a43; }}
        main {{ max-width: 1320px; margin: 0 auto; padding: 32px 24px 64px; }}
        h1 {{ margin: 0 0 8px; font-size: 34px; }}
        p.lead {{ margin: 0 0 18px; color: #486581; }}
        .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 28px; }}
        .grid img {{ width: 100%; border: 1px solid #d9e2ec; border-radius: 14px; background: white; }}
        .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 18px; }}
        .card {{ background: white; border: 1px solid #d9e2ec; border-radius: 18px; padding: 18px; box-shadow: 0 8px 30px rgba(15, 23, 42, 0.04); }}
        .head {{ display: flex; justify-content: space-between; gap: 16px; align-items: baseline; }}
        .head h2 {{ margin: 0; font-size: 20px; }}
        .meta {{ margin: 4px 0 0; color: #7b8794; font-size: 13px; }}
        .pill {{ padding: 6px 10px; border-radius: 999px; font-weight: 700; }}
        .pill.pos {{ background: #d8f3dc; color: #1b4332; }}
        .pill.neg {{ background: #ffe3e3; color: #9d0208; }}
        .stats {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin: 14px 0 14px; }}
        .stats div {{ background: #f8fbff; border-radius: 12px; padding: 10px; }}
        .stats strong {{ display: block; font-size: 12px; color: #7b8794; margin-bottom: 4px; }}
        .stats span {{ font-size: 16px; font-weight: 600; }}
        .card img {{ width: 100%; border: 1px solid #e6eef6; border-radius: 12px; background: white; }}
        .desc {{ color: #486581; line-height: 1.45; }}
        .errors {{ margin-top: 28px; background: #fff5f5; border: 1px solid #fed7d7; border-radius: 16px; padding: 18px; }}
      </style>
    </head>
    <body>
      <main>
        <h1>Benchmark Report</h1>
        <p class="lead">{html.escape(benchmark['benchmark']['market_type'])} | {html.escape(benchmark['benchmark']['symbol'])} | {html.escape(benchmark['benchmark']['interval'])} | bars={benchmark['benchmark']['bars']} | strategies={benchmark['benchmark']['strategy_count']}</p>
        <div class="grid">
          <img src="summary_returns.svg" alt="summary returns" />
          <img src="risk_return.svg" alt="risk return" />
        </div>
        <section class="cards">
          {''.join(rows)}
        </section>
        {failed_html}
      </main>
    </body>
    </html>
    """


def _render_walkforward_fold_chart(report: dict[str, Any]) -> str:
    folds = report["folds"]
    width = 1280
    row_height = 30
    top_padding = 70
    left_margin = 170
    right_margin = 50
    bottom_padding = 50
    chart_width = width - left_margin - right_margin
    height = top_padding + bottom_padding + (len(folds) * row_height)
    values = [fold["test_metrics"]["total_return_pct"] for fold in folds]
    min_value = min(values + [0.0])
    max_value = max(values + [0.0])
    spread = max(max_value - min_value, 1.0)
    zero_x = left_margin + ((0 - min_value) / spread) * chart_width

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<style>text{font-family:Menlo,Consolas,monospace;fill:#102a43} .small{font-size:12px} .label{font-size:13px} .title{font-size:22px;font-weight:700} .axis{stroke:#bcccdc;stroke-width:1}</style>',
        f'<rect width="{width}" height="{height}" fill="#f8fbff"/>',
        '<text x="28" y="34" class="title">Walk-Forward Test Returns By Fold</text>',
        '<text x="28" y="54" class="small">Each bar is the out-of-sample test return for one fold.</text>',
        f'<line x1="{zero_x:.2f}" y1="{top_padding - 8}" x2="{zero_x:.2f}" y2="{height - bottom_padding + 8}" class="axis"/>',
    ]

    for index, fold in enumerate(folds):
        y = top_padding + (index * row_height)
        value = fold["test_metrics"]["total_return_pct"]
        value_x = left_margin + ((value - min_value) / spread) * chart_width
        x = min(value_x, zero_x)
        bar_width = abs(value_x - zero_x)
        color = "#2d6a4f" if value >= 0 else "#c1121f"
        parts.append(f'<text x="22" y="{y + 18}" class="label">Fold {fold["fold"]}</text>')
        parts.append(f'<rect x="{x:.2f}" y="{y + 6}" width="{bar_width:.2f}" height="14" rx="3" fill="{color}" opacity="0.88"/>')
        parts.append(f'<text x="{value_x + 8 if value >= 0 else value_x - 70:.2f}" y="{y + 18}" class="small">{value:.2f}%</text>')

    parts.append("</svg>")
    return "".join(parts)


def _render_walkforward_html(report: dict[str, Any]) -> str:
    fold_rows = "".join(
        f"""
        <tr>
          <td>{fold['fold']}</td>
          <td>{fold['train_metrics']['total_return_pct']:.2f}%</td>
          <td>{fold['test_metrics']['total_return_pct']:.2f}%</td>
          <td>{fold['test_metrics']['max_drawdown_pct']:.2f}%</td>
          <td>{fold['test_metrics']['profit_factor']}</td>
          <td>{fold['test_metrics']['trade_count']}</td>
        </tr>
        """
        for fold in report["folds"]
    )
    summary = report["summary"]
    return f"""
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8" />
      <title>Walk-Forward Report</title>
      <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background: #f4f7fb; color: #102a43; }}
        main {{ max-width: 1200px; margin: 0 auto; padding: 32px 24px 64px; }}
        h1 {{ margin: 0 0 8px; font-size: 34px; }}
        p.lead {{ margin: 0 0 18px; color: #486581; }}
        .stats {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; margin-bottom: 24px; }}
        .stats div {{ background: white; border: 1px solid #d9e2ec; border-radius: 16px; padding: 14px; }}
        .stats strong {{ display:block; color:#7b8794; font-size:12px; margin-bottom:6px; }}
        .stats span {{ font-size:18px; font-weight:700; }}
        img {{ width: 100%; border: 1px solid #d9e2ec; border-radius: 14px; background: white; margin-bottom: 24px; }}
        table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 16px; overflow: hidden; }}
        th, td {{ padding: 12px 14px; border-bottom: 1px solid #e6eef6; text-align: left; }}
        th {{ background: #f8fbff; }}
      </style>
    </head>
    <body>
      <main>
        <h1>Walk-Forward Report</h1>
        <p class="lead">{html.escape(report['strategy_name'])} | {html.escape(report['symbol'])} | {html.escape(report['interval'])} | folds={report['fold_count']}</p>
        <div class="stats">
          <div><strong>Average Train Return</strong><span>{summary['avg_train_return_pct']:.2f}%</span></div>
          <div><strong>Average Test Return</strong><span>{summary['avg_test_return_pct']:.2f}%</span></div>
          <div><strong>Positive Test Folds</strong><span>{summary['positive_test_fold_pct']:.2f}%</span></div>
          <div><strong>Best Test Fold</strong><span>{summary['best_test_return_pct']:.2f}%</span></div>
          <div><strong>Worst Test Fold</strong><span>{summary['worst_test_return_pct']:.2f}%</span></div>
          <div><strong>Average Test Drawdown</strong><span>{summary['avg_test_drawdown_pct']:.2f}%</span></div>
        </div>
        <img src="fold_returns.svg" alt="walk-forward fold returns" />
        <table>
          <thead>
            <tr>
              <th>Fold</th>
              <th>Train Return</th>
              <th>Test Return</th>
              <th>Test Drawdown</th>
              <th>Test Profit Factor</th>
              <th>Test Trades</th>
            </tr>
          </thead>
          <tbody>{fold_rows}</tbody>
        </table>
      </main>
    </body>
    </html>
    """
