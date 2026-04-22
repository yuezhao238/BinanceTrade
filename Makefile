install:
	uv pip install -e ".[dev]"

test:
	uv run pytest

doctor:
	uv run binance-trade doctor
