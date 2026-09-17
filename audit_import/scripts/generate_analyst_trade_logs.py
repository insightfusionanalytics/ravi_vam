"""
Generate analyst-grade Excel trade logs for every strategy × ETF combo
that PASSES the qualification gates.

Each Excel file has 3 sheets:
  1. Summary — all metrics at a glance
  2. Trade_Log — every trade with OHLCV at entry/exit, costs, reasoning
  3. OHLCV_Data — full daily data with indicators + ENTRY/EXIT markers

Usage:
    cd _engine/code
    python scripts/generate_analyst_trade_logs.py
"""

import sys
from pathlib import Path

import pandas as pd
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

# Import strategy classes from sweep script
from scripts.ravi_strategy_sweep import (
    COMMISSION,
    INITIAL_CAPITAL,
    SLIPPAGE,
    EMA_Momentum,
    IBS_Reversion,
    RSI_Bounce,
    SMA_RSI_Combo,
    build_strategy_variants,
)

from src.data.loader import load_csv
from src.engine.backtester import Backtester, StrategyBase
from src.indicators.registry import IndicatorRegistry

DATA_DIR = Path(__file__).parent.parent / "data" / "databento" / "equities"
OUTPUT_DIR = Path(__file__).parent.parent / "clients" / "ravi_vam" / "analyst_trade_logs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ETFS = ["SPY", "QQQ", "TQQQ", "SOXL", "TLT", "GLD", "UPRO", "SHY"]


def get_entry_exit_logic(strategy: StrategyBase, bar_index: int, data: pd.DataFrame) -> str:
    """Generate human-readable entry/exit logic with actual indicator values."""
    IndicatorRegistry()
    close = data["close"].iloc[bar_index]

    if isinstance(strategy, EMA_Momentum):
        fast_val = strategy.fast_ema.iloc[bar_index] if hasattr(strategy, "fast_ema") else None
        slow_val = strategy.slow_ema.iloc[bar_index] if hasattr(strategy, "slow_ema") else None
        adx_val = strategy.adx.iloc[bar_index] if hasattr(strategy, "adx") else None
        if fast_val is not None and slow_val is not None:
            cross = "above" if fast_val > slow_val else "below"
            return f"EMA({strategy.fast_period})={fast_val:.2f} crossed {cross} EMA({strategy.slow_period})={slow_val:.2f} | ADX(14)={adx_val:.1f} > {strategy.adx_threshold}"
    elif isinstance(strategy, SMA_RSI_Combo):
        sma_val = strategy.sma.iloc[bar_index] if hasattr(strategy, "sma") else None
        rsi_val = strategy.rsi.iloc[bar_index] if hasattr(strategy, "rsi") else None
        if sma_val is not None and rsi_val is not None:
            return f"Close={close:.2f} vs SMA({strategy.sma_period})={sma_val:.2f} | RSI({strategy.rsi_period})={rsi_val:.1f}"
    elif isinstance(strategy, IBS_Reversion):
        ibs_val = strategy.ibs.iloc[bar_index] if hasattr(strategy, "ibs") else None
        if ibs_val is not None:
            return f"IBS={ibs_val:.4f} (threshold={strategy.entry_threshold})"
    elif isinstance(strategy, RSI_Bounce):
        rsi_val = strategy.rsi.iloc[bar_index] if hasattr(strategy, "rsi") else None
        if rsi_val is not None:
            return f"RSI({strategy.period})={rsi_val:.1f} (entry<{strategy.entry_level}, exit>{strategy.exit_level})"

    return f"Close={close:.2f}"


def generate_trade_log_excel(
    strategy_name: str,
    strategy: StrategyBase,
    etf: str,
    data: pd.DataFrame,
):
    """Run backtest and generate detailed Excel trade log."""
    # Run backtest
    engine = Backtester(
        data=data,
        initial_capital=INITIAL_CAPITAL,
        commission_pct=COMMISSION,
        slippage_pct=SLIPPAGE,
    )
    result = engine.run(strategy)

    if not result.trades:
        return None

    metrics = result.metrics

    # ── Sheet 1: Summary ──
    summary_data = {
        "Metric": [
            "Strategy",
            "ETF",
            "Period",
            "Initial Capital",
            "Final Capital",
            "Total Return %",
            "CAGR %",
            "Sharpe Ratio",
            "Sortino Ratio",
            "Calmar Ratio",
            "Max Drawdown %",
            "Win Rate %",
            "Profit Factor",
            "Total Trades",
            "Winning Trades",
            "Losing Trades",
            "Avg Win ($)",
            "Avg Loss ($)",
            "Avg Trade Return %",
            "Avg Bars Held",
            "Commission %",
            "Slippage %",
        ],
        "Value": [
            strategy_name,
            etf,
            f"{data.index[0].date()} to {data.index[-1].date()}",
            f"${INITIAL_CAPITAL:,.2f}",
            f"${metrics['final_capital']:,.2f}",
            f"{metrics['total_return_pct']:.2f}%",
            f"{metrics['annual_return_pct']:.2f}%",
            f"{metrics['sharpe']:.3f}",
            f"{metrics['sortino']:.3f}",
            f"{metrics['calmar']:.3f}",
            f"{metrics['max_drawdown_pct']:.2f}%",
            f"{metrics['win_rate']:.1f}%",
            f"{metrics['profit_factor']:.2f}",
            metrics["total_trades"],
            metrics["winning_trades"],
            metrics["losing_trades"],
            f"${metrics['avg_win']:,.2f}",
            f"${metrics['avg_loss']:,.2f}",
            f"{metrics['total_return_pct'] / max(metrics['total_trades'], 1):.2f}%",
            f"{metrics['avg_bars_held']:.1f}",
            f"{COMMISSION * 100:.2f}%",
            f"{SLIPPAGE * 100:.3f}%",
        ],
    }
    df_summary = pd.DataFrame(summary_data)

    # ── Sheet 2: Trade Log ──
    trade_rows = []
    capital = INITIAL_CAPITAL
    for i, trade in enumerate(result.trades, 1):
        entry_idx = data.index.get_loc(trade.entry_time) if trade.entry_time in data.index else None
        data.index.get_loc(trade.exit_time) if trade.exit_time in data.index else None

        entry_bar = data.loc[trade.entry_time] if trade.entry_time in data.index else None
        exit_bar = data.loc[trade.exit_time] if trade.exit_time in data.index else None

        # Cost breakdown
        entry_cost = trade.entry_price * trade.quantity * (COMMISSION + SLIPPAGE)
        exit_cost = trade.exit_price * trade.quantity * (COMMISSION + SLIPPAGE)
        gross_pnl = (trade.exit_price - trade.entry_price) * trade.quantity
        total_costs = entry_cost + exit_cost
        net_pnl = trade.pnl  # backtester already computes net

        # Entry/exit logic
        entry_logic = (
            get_entry_exit_logic(strategy, entry_idx, data) if entry_idx is not None else ""
        )
        exit_logic = (
            f"Exit: {trade.exit_reason}" if hasattr(trade, "exit_reason") else "Signal exit"
        )

        capital_before = capital
        capital += net_pnl
        pnl_pct = (net_pnl / capital_before) * 100 if capital_before > 0 else 0

        row = {
            "Trade_#": i,
            "Direction": "LONG",
            "Entry_Date": (
                str(trade.entry_time.date())
                if hasattr(trade.entry_time, "date")
                else str(trade.entry_time)
            ),
            "Exit_Date": (
                str(trade.exit_time.date())
                if hasattr(trade.exit_time, "date")
                else str(trade.exit_time)
            ),
            "Bars_Held": trade.bars_held,
            "Entry_Price": round(trade.entry_price, 2),
            "Exit_Price": round(trade.exit_price, 2),
            "Quantity": trade.quantity,
        }

        # Add OHLCV at entry
        if entry_bar is not None:
            row["Entry_Open"] = round(entry_bar.get("open", 0), 2)
            row["Entry_High"] = round(entry_bar.get("high", 0), 2)
            row["Entry_Low"] = round(entry_bar.get("low", 0), 2)
            row["Entry_Close"] = round(entry_bar.get("close", 0), 2)
            row["Entry_Volume"] = int(entry_bar.get("volume", 0))

        # Add OHLCV at exit
        if exit_bar is not None:
            row["Exit_Open"] = round(exit_bar.get("open", 0), 2)
            row["Exit_High"] = round(exit_bar.get("high", 0), 2)
            row["Exit_Low"] = round(exit_bar.get("low", 0), 2)
            row["Exit_Close"] = round(exit_bar.get("close", 0), 2)
            row["Exit_Volume"] = int(exit_bar.get("volume", 0))

        row["Gross_PnL"] = round(gross_pnl, 2)
        row["Commission_Entry"] = round(entry_cost / 2, 2)
        row["Commission_Exit"] = round(exit_cost / 2, 2)
        row["Slippage_Entry_bps"] = SLIPPAGE * 10000
        row["Slippage_Exit_bps"] = SLIPPAGE * 10000
        row["Total_Costs"] = round(total_costs, 2)
        row["Net_PnL"] = round(net_pnl, 2)
        row["PnL_Pct"] = round(pnl_pct, 2)
        row["Capital_Before"] = round(capital_before, 2)
        row["Capital_After"] = round(capital, 2)
        row["Win_Loss"] = "WIN" if net_pnl > 0 else "LOSS"
        row["Entry_Logic"] = entry_logic
        row["Exit_Logic"] = exit_logic

        trade_rows.append(row)

    df_trades = pd.DataFrame(trade_rows)

    # ── Sheet 3: OHLCV Data with indicators and markers ──
    df_ohlcv = data[["open", "high", "low", "close", "volume"]].copy()

    # Add indicators based on strategy type
    registry = IndicatorRegistry()
    if isinstance(strategy, EMA_Momentum):
        df_ohlcv[f"EMA_{strategy.fast_period}"] = registry.compute(
            "EMA", data, period=strategy.fast_period
        )
        df_ohlcv[f"EMA_{strategy.slow_period}"] = registry.compute(
            "EMA", data, period=strategy.slow_period
        )
        adx_data = registry.compute("ADX", data, period=14)
        df_ohlcv["ADX_14"] = adx_data["adx"] if isinstance(adx_data, pd.DataFrame) else adx_data
    elif isinstance(strategy, SMA_RSI_Combo):
        df_ohlcv[f"SMA_{strategy.sma_period}"] = registry.compute(
            "SMA", data, period=strategy.sma_period
        )
        df_ohlcv[f"RSI_{strategy.rsi_period}"] = registry.compute(
            "RSI", data, period=strategy.rsi_period
        )
    elif isinstance(strategy, IBS_Reversion):
        df_ohlcv["IBS"] = (data["close"] - data["low"]) / (data["high"] - data["low"] + 1e-10)
    elif isinstance(strategy, RSI_Bounce):
        df_ohlcv[f"RSI_{strategy.period}"] = registry.compute("RSI", data, period=strategy.period)
        df_ohlcv["ATR_14"] = registry.compute("ATR", data, period=14)

    # Add ENTRY/EXIT markers
    df_ohlcv["Signal"] = ""
    for trade in result.trades:
        if trade.entry_time in df_ohlcv.index:
            df_ohlcv.loc[trade.entry_time, "Signal"] = "ENTRY"
        if trade.exit_time in df_ohlcv.index:
            existing = df_ohlcv.loc[trade.exit_time, "Signal"]
            df_ohlcv.loc[trade.exit_time, "Signal"] = "EXIT" if not existing else f"{existing}/EXIT"

    # Reset index for clean Excel output
    df_ohlcv = df_ohlcv.reset_index()
    df_ohlcv.rename(columns={"index": "Date"}, inplace=True)

    # ── Write Excel ──
    filename = f"{strategy_name}_{etf}.xlsx"
    filepath = OUTPUT_DIR / filename

    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        df_summary.to_excel(writer, sheet_name="Summary", index=False)
        df_trades.to_excel(writer, sheet_name="Trade_Log", index=False)
        df_ohlcv.to_excel(writer, sheet_name="OHLCV_Data", index=False)

    return filepath


def main():
    logger.remove()
    logger.add(sys.stderr, level="WARNING")

    print("=" * 80)
    print("GENERATING ANALYST TRADE LOGS")
    print("=" * 80)

    # Load all ETF data
    etf_data = {}
    for etf in ETFS:
        filepath = DATA_DIR / f"{etf}_daily.csv"
        if filepath.exists():
            etf_data[etf] = load_csv(str(filepath))
            print(f"  Loaded {etf}: {len(etf_data[etf])} bars")

    # Build all strategy variants
    variants = build_strategy_variants()

    # Load passing strategies from sweep results

    passing_path = (
        Path(__file__).parent.parent
        / "clients"
        / "ravi_vam"
        / "results"
        / "strategy_sweep_full.csv"
    )
    sweep_df = pd.read_csv(passing_path)

    # Generate for ALL strategies (not just passing) so analyst can see everything
    generated = 0
    failed = 0
    total = len(sweep_df)

    for idx, row in sweep_df.iterrows():
        strategy_name = row["strategy"]
        etf = row["etf"]

        if etf not in etf_data:
            continue

        # Find matching strategy variant
        matching = [v for v in variants if v[0] == strategy_name]
        if not matching:
            continue

        var_name, strategy = matching[0]
        data = etf_data[etf]

        try:
            filepath = generate_trade_log_excel(strategy_name, strategy, etf, data)
            if filepath:
                generated += 1
                if generated % 50 == 0:
                    print(f"  Progress: {generated} generated ({generated * 100 / total:.0f}%)")
            else:
                failed += 1
        except Exception as e:
            failed += 1
            if failed <= 5:
                print(f"  Failed: {strategy_name} on {etf}: {e}")

    print(f"\n{'=' * 80}")
    print(f"COMPLETE: {generated} Excel files generated, {failed} failed")
    print(f"Output: {OUTPUT_DIR}")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
