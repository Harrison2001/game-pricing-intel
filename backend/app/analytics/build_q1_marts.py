from pathlib import Path
import pandas as pd

# ============================================================
# build_q1_marts.py
# Input:  data/games_clean.csv
# Output: data/marts/q1_*.csv  (Power BI-ready)
# Purpose:
#   Question 1 (Seasonal Pricing):
#     - Avg price by month
#     - Median price by season
#     - Count of releases by month
# ============================================================

# project root (this file likely lives in backend/app/analytics/)
PROJECT_ROOT = Path(__file__).resolve().parents[3]
CLEAN = PROJECT_ROOT / "data" / "games_clean.csv"

# output folder for Power BI-ready datasets
MARTS_DIR = PROJECT_ROOT / "data" / "marts"
OUT_SEASON = MARTS_DIR / "q1_pricing_by_season.csv"
OUT_MONTH = MARTS_DIR / "q1_pricing_by_month.csv"


def month_name(m: int) -> str:
    """Optional helper for nicer labels in Power BI."""
    names = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }
    return names.get(int(m), str(m))


def safe_write_csv(df: pd.DataFrame, out_path: Path) -> None:
    """
    Writes in a Windows-friendly way:
    - writes to a temp file first
    - then replaces the final file
    If the final file is locked (Power BI/VS Code), you still get a timestamped fallback.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    tmp = out_path.with_suffix(".tmp.csv")
    df.to_csv(tmp, index=False)

    try:
        tmp.replace(out_path)
    except PermissionError:
        stamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        fallback = out_path.with_name(f"{out_path.stem}_{stamp}.csv")
        tmp.replace(fallback)
        print(f"⚠️ {out_path.name} locked. Wrote fallback: {fallback.name}")


def main(paid_only: bool = True) -> None:
    print(f"Loading clean data: {CLEAN.resolve()}")

    if not CLEAN.exists():
        raise FileNotFoundError(f"Missing {CLEAN}. Run clean_games.py first.")

    df = pd.read_csv(CLEAN, keep_default_na=False)

    # Ensure numeric
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df["ReleaseMonth"] = pd.to_numeric(df["ReleaseMonth"], errors="coerce")

    # Basic safety filters
    df = df[df["Price"].notna()]
    df = df[df["Price"] >= 0]
    df = df[df["ReleaseSeason"].notna()]
    df = df[df["ReleaseMonth"].notna()]

    # Paid-only filter (your choice)
    if paid_only:
        df = df[df["Price"] > 0].copy()
        print("Mode: PAID games only (Price > 0)")
    else:
        print("Mode: ALL games (includes free)")

    # ------------------------------------------------------------
    # Q1 Dataset 1: Pricing by Season
    # Fields useful for visuals:
    #   ReleaseSeason, releases_count, avg_price, median_price, min_price, max_price
    # ------------------------------------------------------------
    season_order = ["Winter", "Spring", "Summer", "Fall"]

    season_stats = (
        df.groupby("ReleaseSeason")["Price"]
        .agg(
            releases_count="count",
            avg_price="mean",
            median_price="median",
            min_price="min",
            max_price="max",
        )
        .reset_index()
    )

    # Order seasons for nicer charts
    season_stats["__order"] = season_stats["ReleaseSeason"].apply(
        lambda s: season_order.index(s) if s in season_order else 999
    )
    season_stats = season_stats.sort_values("__order").drop(columns="__order")

    # Round for display
    for col in ["avg_price", "median_price", "min_price", "max_price"]:
        season_stats[col] = season_stats[col].round(2)

    # ------------------------------------------------------------
    # Q1 Dataset 2: Pricing by Month
    # Fields useful for visuals:
    #   ReleaseMonth, MonthName, releases_count, avg_price, median_price, min_price, max_price
    # ------------------------------------------------------------
    month_stats = (
        df.groupby("ReleaseMonth")["Price"]
        .agg(
            releases_count="count",
            avg_price="mean",
            median_price="median",
            min_price="min",
            max_price="max",
        )
        .reset_index()
        .sort_values("ReleaseMonth")
    )

    month_stats["ReleaseMonth"] = month_stats["ReleaseMonth"].astype(int)
    month_stats["MonthName"] = month_stats["ReleaseMonth"].apply(month_name)

    for col in ["avg_price", "median_price", "min_price", "max_price"]:
        month_stats[col] = month_stats[col].round(2)

    # ------------------------------------------------------------
    # Write outputs for Power BI
    # ------------------------------------------------------------
    safe_write_csv(season_stats, OUT_SEASON)
    safe_write_csv(month_stats, OUT_MONTH)

    print("\n✅ Wrote Q1 marts:")
    print(" -", OUT_SEASON.resolve())
    print(" -", OUT_MONTH.resolve())

    print("\nPreview (season):")
    print(season_stats.head(10).to_string(index=False))

    print("\nPreview (month):")
    print(month_stats.head(12).to_string(index=False))


if __name__ == "__main__":
    main(paid_only=True)  # change to False if you want free games included
