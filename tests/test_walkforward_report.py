import json
from pathlib import Path

from binance_trade.research_report import write_walkforward_report


def test_write_walkforward_report_generates_json_svg_and_html(tmp_path: Path) -> None:
    report = {
        "strategy_name": "EmaCrossoverStrategy",
        "symbol": "BTCUSDT",
        "interval": "15m",
        "market_type": "spot",
        "fold_count": 2,
        "summary": {
            "avg_train_return_pct": 1.5,
            "avg_test_return_pct": 0.8,
            "positive_test_fold_pct": 50.0,
            "best_test_return_pct": 1.2,
            "worst_test_return_pct": -0.4,
            "avg_test_drawdown_pct": 1.1,
        },
        "folds": [
            {"fold": 1, "train_metrics": {"total_return_pct": 2.0}, "test_metrics": {"total_return_pct": 1.2, "max_drawdown_pct": 0.8, "profit_factor": 1.4, "trade_count": 3}},
            {"fold": 2, "train_metrics": {"total_return_pct": 1.0}, "test_metrics": {"total_return_pct": -0.4, "max_drawdown_pct": 1.4, "profit_factor": 0.8, "trade_count": 2}},
        ],
    }
    artifacts = write_walkforward_report(report, tmp_path)

    assert Path(artifacts["json"]).exists()
    assert Path(artifacts["folds_svg"]).exists()
    assert Path(artifacts["html"]).exists()
    assert "<svg" in Path(artifacts["folds_svg"]).read_text(encoding="utf-8")
    assert "<html" in Path(artifacts["html"]).read_text(encoding="utf-8")
    payload = json.loads(Path(artifacts["json"]).read_text(encoding="utf-8"))
    assert payload["strategy_name"] == "EmaCrossoverStrategy"
