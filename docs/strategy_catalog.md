# Built-in Strategy Catalog

These are built-in strategy templates integrated into the runtime. They are intended to be valid, executable, and backtestable rulesets. They are not guarantees of profitability.

| Name | Category | Core rule |
| --- | --- | --- |
| `sma_crossover` | Trend | Fast SMA crosses slow SMA. |
| `ema_crossover` | Trend | Fast EMA crosses slow EMA. |
| `macd_signal` | Trend | MACD line crosses signal line. |
| `macd_zero` | Trend | MACD line crosses zero line. |
| `rsi_mean_reversion` | Mean reversion | RSI exits oversold or overbought zone. |
| `rsi_regime` | Trend | RSI enters bullish or bearish regime. |
| `stoch_rsi` | Momentum | StochRSI K crosses D in extreme zone. |
| `slow_stochastic` | Momentum | Stochastic K crosses D in extreme zone. |
| `bollinger_mean_reversion` | Mean reversion | Price re-enters Bollinger envelope after stretch. |
| `bollinger_squeeze_breakout` | Volatility | Low bandwidth followed by band breakout. |
| `cci_breakout` | Momentum | CCI breaks above `+100` or below `-100`. |
| `cci_mean_reversion` | Mean reversion | CCI reverts back from extreme zone. |
| `obv_confirmation` | Volume | Price trend confirmed by OBV trend. |
| `cmf_breakout` | Volume | CMF crosses zero with price breakout confirmation. |
| `mfi_mean_reversion` | Volume | MFI exits oversold or overbought zone. |
| `atr_breakout` | Volatility | Price clears breakout channel by ATR buffer. |
| `dmi_adx_trend` | Trend | `+DI/-DI` crossover with ADX trend-strength filter. |
| `aroon_trend` | Trend | Aroon Up/Down crossover with trend threshold. |
| `keltner_breakout` | Volatility | Price breaks outside Keltner bands. |
| `ichimoku_trend` | Trend | Tenkan/Kijun crossover with cloud confirmation. |
| `williams_r` | Momentum | Williams `%R` exits oversold or overbought zone. |

## Source Families

- Fidelity Technical Indicator Guide overview: [overview](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/overview)
- SMA: [sma](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/sma)
- EMA: [ema](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/ema)
- MACD: [macd](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/macd)
- RSI: [rsi](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/rsi)
- StochRSI: [stochrsi](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/stochrsi)
- Fast Stochastic: [fast-stochastic](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/fast-stochastic)
- Bollinger Bands: [bollinger-bands](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/bollinger-bands)
- CCI: [cci](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/cci)
- OBV: [obv](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/obv)
- CMF: [cmf](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/cmf)
- MFI: [mfi](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/mfi)
- ATR: [atr](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/atr)
- DMI: [DMI](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/DMI)
- Aroon: [aroon-indicator](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/aroon-indicator)
- Keltner Bands: [keltner-bands](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/keltner-bands)
- Ichimoku Cloud: [ichimoku-cloud](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/ichimoku-cloud)
- Williams %R: [williams-r](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/williams-r)
- TA-Lib indicator references: [RSI](https://ta-lib.github.io/ta-doc/indicator/RSI.htm), [MACD](https://ta-lib.github.io/ta-doc/indicator/MACD.htm), [BBANDS](https://ta-lib.github.io/ta-doc/indicator/BBANDS.htm), [CCI](https://ta-lib.github.io/ta-doc/indicator/CCI.htm), [ATR](https://ta-lib.github.io/ta-doc/indicator/ATR.htm), [ADX](https://ta-lib.github.io/ta-doc/indicator/ADX.htm), [AROON](https://ta-lib.github.io/ta-doc/indicator/AROON.htm), [MFI](https://ta-lib.github.io/ta-doc/indicator/MFI.htm), [OBV](https://ta-lib.github.io/ta-doc/indicator/OBV.htm), [WILLR](https://ta-lib.github.io/ta-doc/indicator/WILLR.htm)
- Brock, Lakonishok, and LeBaron on moving-average and trading-range rules: [JSTOR](https://www.jstor.org/stable/2328994)
