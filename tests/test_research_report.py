import json
from pathlib import Path

from binance_trade.research_report import write_benchmark_report


def test_write_benchmark_report_generates_json_svg_and_html(tmp_path: Path) -> None:
    benchmark = {
        "benchmark": {
            "market_type": "spot",
            "symbol": "BTCUSDT",
            "interval": "15m",
            "bars": 100,
            "strategy_count": 1,
            "completed_count": 1,
            "failed_count": 0,
            "assumptions": {
                "initial_capital": 10000.0,
                "fee_bps": 10.0,
                "slippage_bps": 2.0,
                "leverage": 1.0,
                "position_fraction": 1.0,
            },
        },
        "top_strategies": [],
        "worst_strategies": [],
        "strategies": [
            {
                "name": "ema_crossover",
                "title": "EMA Crossover",
                "category": "trend",
                "description": "Fast EMA crosses slow EMA.",
                "status": "OK",
                "metrics": {
                    "total_return_pct": 3.5,
                    "max_drawdown_pct": 2.1,
                    "profit_factor": 1.3,
                    "trade_count": 8,
                    "fees_paid": 42.0,
                    "exposure_pct": 51.0,
                    "sharpe": 1.1,
                },
                "equity_curve": [
                    {"time": 1, "equity": 10000.0},
                    {"time": 2, "equity": 10100.0},
                    {"time": 3, "equity": 10350.0},
                ],
            }
        ],
    }

    artifacts = write_benchmark_report(benchmark, tmp_path)

    assert Path(artifacts["json"]).exists()
    assert Path(artifacts["summary_svg"]).exists()
    assert Path(artifacts["risk_svg"]).exists()
    assert Path(artifacts["html"]).exists()
    assert (tmp_path / "equity_curves" / "ema_crossover.svg").exists()
    assert "<svg" in Path(artifacts["summary_svg"]).read_text(encoding="utf-8")
    assert "<html" in Path(artifacts["html"]).read_text(encoding="utf-8")
    payload = json.loads(Path(artifacts["json"]).read_text(encoding="utf-8"))
    assert payload["benchmark"]["market_type"] == "spot"
