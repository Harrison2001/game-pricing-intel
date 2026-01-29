from pathlib import Path
import pandas as pd

# ============================================================
# build_q3_marts.py
# Input:  data/games_clean.csv
# Output: data/marts/q3_*.csv  (Power BI-ready)
#
# Question 3 (Market Pricing Segments)
# Business Question:
#   Which types of games (by genre) tend to be cheaper, free, or premium-priced?
#
# Outputs:
#   1) q3_market_segments.csv (row-level)
#      - Name, Price, IsFree, Genres, PriceBand
#
#   2) q3_free_vs_paid.csv (overall % free vs paid)
#
#   3) q3_genre_price_segments.csv (genre-level aggregates)
#      - Genre, games_count, free_pct, paid_pct,
#        avg_price_paid, median_price_paid, pct_premium_paid, ...
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CLEAN = PROJECT_ROOT / "data" / "games_clean.csv"

MARTS_DIR = PROJECT_ROOT / "data" / "marts"
OUT_ROWS = MARTS_DIR / "q3_market_segments.csv"
OUT_FREEPAID = MARTS_DIR / "q3_free_vs_paid.csv"
OUT_GENRE = MARTS_DIR / "q3_genre_price_segments.csv"


def safe_write_csv(df: pd.DataFrame, out_path: Path) -> None:
    """
    Windows-friendly write:
    - write temp
    - replace final
    - if locked, write timestamp fallback
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


def normalize_isfree(series: pd.Series) -> pd.Series:
    """
    IsFree may come out as:
      - True/False bool
      - 'True'/'False' strings
      - 1/0
    Normalize to bool.
    """
    s = series.astype(str).str.lower().str.strip()
    return s.isin(["true", "1", "yes"])


def price_band(price: float) -> str:
    """
    Price bands for PAID games (free handled separately):
      Cheap:    0 < price < 10
      Mid:      10 <= price < 30
      Premium:  30 <= price
    """
    try:
        p = float(price)
    except Exception:
        return "Unknown"
    if p <= 0:
        return "Free"
    if p < 10:
        return "Cheap (<$10)"
    if p < 30:
        return "Mid ($10-$29.99)"
    return "Premium ($30+)"


def explode_genres(df: pd.DataFrame) -> pd.DataFrame:
    """
    Turn "Action, Indie" into multiple rows with a single Genre column.
    """
    d = df.copy()
    d["Genres"] = d["Genres"].astype(str)

    d = d.assign(Genre=d["Genres"].str.split(",")).explode("Genre")
    d["Genre"] = d["Genre"].astype(str).str.strip()

    d = d[(d["Genre"] != "") & (d["Genre"].str.lower() != "unknown")]
    return d


def main(min_games_per_genre: int = 50) -> None:
    print(f"Loading clean data: {CLEAN.resolve()}")

    if not CLEAN.exists():
        raise FileNotFoundError(f"Missing {CLEAN}. Run clean_games.py first.")

    df = pd.read_csv(CLEAN, keep_default_na=False)

    required = {"Name", "Price", "IsFree", "Genres"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for Q3: {sorted(missing)}")

    # Normalize types
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df = df[df["Price"].notna()]
    df = df[df["Price"] >= 0]

    df["IsFree"] = normalize_isfree(df["IsFree"])
    df["Genres"] = df["Genres"].astype(str)

    # Add a price band column (useful for stacked bars / filtering in Power BI)
    df["PriceBand"] = df["Price"].apply(price_band)

    # ------------------------------------------------------------
    # Output 1: Row-level market segments
    # ------------------------------------------------------------
    q3_rows = df[["Name", "Price", "IsFree", "Genres", "PriceBand"]].copy()
    q3_rows["Price"] = q3_rows["Price"].round(2)

    # ------------------------------------------------------------
    # Output 2: Overall free vs paid summary
    # ------------------------------------------------------------
    total = len(df)
    free_count = int(df["IsFree"].sum())
    paid_count = int(total - free_count)

    q3_freepaid = pd.DataFrame(
        [{
            "total_games": total,
            "free_games": free_count,
            "paid_games": paid_count,
            "free_pct": round((free_count / total) * 100, 2) if total else 0.0,
            "paid_pct": round((paid_count / total) * 100, 2) if total else 0.0,
        }]
    )

    # ------------------------------------------------------------
    # Output 3: Genre-level aggregates (segments)
    #   - avg/median price for PAID games
    #   - free vs paid %
    #   - premium % among paid games
    # ------------------------------------------------------------
    g = explode_genres(df)

    # Paid-only subset for price metrics (free prices = 0 will distort avg)
    g_paid = g[g["Price"] > 0].copy()
    g_paid["PaidBand"] = g_paid["Price"].apply(price_band)

    # Count totals by genre (all games)
    genre_counts = g.groupby("Genre").size().rename("games_count").reset_index()

    # Free/Paid % by genre (all games)
    genre_free = (
        g.groupby("Genre")["IsFree"]
        .agg(free_games="sum", games_count_check="count")
        .reset_index()
    )
    genre_free["free_pct"] = (genre_free["free_games"] / genre_free["games_count_check"] * 100).round(2)
    genre_free["paid_pct"] = (100 - genre_free["free_pct"]).round(2)
    genre_free = genre_free.drop(columns=["games_count_check"])

    # Price stats by genre (paid only)
    genre_price = (
        g_paid.groupby("Genre")["Price"]
        .agg(avg_price_paid="mean", median_price_paid="median")
        .reset_index()
    )
    genre_price["avg_price_paid"] = genre_price["avg_price_paid"].round(2)
    genre_price["median_price_paid"] = genre_price["median_price_paid"].round(2)

    # Premium % among PAID games by genre
    premium = (
        g_paid.assign(is_premium=g_paid["Price"] >= 30)
        .groupby("Genre")["is_premium"]
        .agg(premium_paid_games="sum", paid_games="count")
        .reset_index()
    )
    premium["pct_premium_paid"] = (premium["premium_paid_games"] / premium["paid_games"] * 100).round(2)

    # Merge all genre metrics
    q3_genre = genre_counts.merge(genre_free, on="Genre", how="left") \
                           .merge(genre_price, on="Genre", how="left") \
                           .merge(premium[["Genre", "paid_games", "pct_premium_paid"]], on="Genre", how="left")

    # Filter out tiny genres (so charts are not noisy)
    q3_genre = q3_genre[q3_genre["games_count"] >= min_games_per_genre].copy()

    # Sort: biggest genres first
    q3_genre = q3_genre.sort_values("games_count", ascending=False)

    # ------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------
    safe_write_csv(q3_rows, OUT_ROWS)
    safe_write_csv(q3_freepaid, OUT_FREEPAID)
    safe_write_csv(q3_genre, OUT_GENRE)

    print("\n✅ Wrote Q3 marts:")
    print(" -", OUT_ROWS.resolve())
    print(" -", OUT_FREEPAID.resolve())
    print(" -", OUT_GENRE.resolve())

    print("\nPreview (free vs paid):")
    print(q3_freepaid.to_string(index=False))

    print("\nPreview (genre segments):")
    print(q3_genre.head(10).to_string(index=False))


if __name__ == "__main__":
    main(min_games_per_genre=50)
