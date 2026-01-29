from pathlib import Path
import pandas as pd

# ============================================================
# build_q2_marts.py
# Input:  data/games_clean.csv
# Output: data/marts/q2_*.csv  (Power BI-ready)
#
# Question 2 (Reviews vs Price & Features)
# Business Question:
#   What factors (price, genre) are associated with higher review performance?
#
# We ignore Publisher for now.
#
# Outputs:
#   1) q2_reviews_features.csv (row-level for scatter + slicers)
#      - Name, Price, ReviewRatio, TotalReviews, Genres
#
#   2) q2_genre_review_ratio.csv (aggregated for bar charts)
#      - Genre, games_count, avg_review_ratio, median_review_ratio,
#        avg_price(optional), median_price(optional), avg_total_reviews(optional)
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CLEAN = PROJECT_ROOT / "data" / "games_clean.csv"

MARTS_DIR = PROJECT_ROOT / "data" / "marts"
OUT_FEATURES = MARTS_DIR / "q2_reviews_features.csv"
OUT_GENRE = MARTS_DIR / "q2_genre_review_ratio.csv"


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


def explode_genres(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a row like:
      Genres = "Action, Indie, RPG"
    into multiple rows:
      Genre = "Action"
      Genre = "Indie"
      Genre = "RPG"
    """
    d = df.copy()
    d["Genres"] = d["Genres"].astype(str)

    d = d.assign(Genre=d["Genres"].str.split(",")).explode("Genre")
    d["Genre"] = d["Genre"].astype(str).str.strip()

    # remove blanks / junk
    d = d[(d["Genre"] != "") & (d["Genre"].str.lower() != "unknown")]
    return d


def main(min_total_reviews: int = 10) -> None:
    print(f"Loading clean data: {CLEAN.resolve()}")

    if not CLEAN.exists():
        raise FileNotFoundError(f"Missing {CLEAN}. Run clean_games.py first.")

    df = pd.read_csv(CLEAN, keep_default_na=False)

    # Ensure numeric types for math/filters
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df["ReviewRatio"] = pd.to_numeric(df["ReviewRatio"], errors="coerce")

    # TotalReviews should exist (your cleaned schema shows it does)
    if "TotalReviews" in df.columns:
        df["TotalReviews"] = pd.to_numeric(df["TotalReviews"], errors="coerce")
    else:
        # fallback just in case
        df["TotalReviews"] = pd.NA

    # Basic filters: we need ratio + genres
    required = {"Name", "Price", "ReviewRatio", "Genres"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for Q2: {sorted(missing)}")

    d = df.dropna(subset=["Price", "ReviewRatio"]).copy()
    d = d[d["ReviewRatio"].between(0, 1, inclusive="both")]

    # Optional: avoid super-noisy ratios where TotalReviews is tiny/0
    # (ReviewRatio can be NA when TotalReviews == 0)
    if "TotalReviews" in d.columns:
        d = d.dropna(subset=["TotalReviews"])
        d = d[d["TotalReviews"] >= min_total_reviews]

    # ------------------------------------------------------------
    # Output 1: Row-level features table (for scatter plot + slicers)
    # Power BI scatter:
    #   X = Price
    #   Y = ReviewRatio
    #   Size = TotalReviews (nice touch)
    #   Tooltip = Name, Genres
    # ------------------------------------------------------------
    features_cols = ["Name", "Price", "ReviewRatio", "TotalReviews", "Genres"]
    features_cols = [c for c in features_cols if c in d.columns]
    q2_features = d[features_cols].copy()

    # Friendly rounding (optional)
    q2_features["Price"] = q2_features["Price"].round(2)
    q2_features["ReviewRatio"] = q2_features["ReviewRatio"].round(4)

    # ------------------------------------------------------------
    # Output 2: Aggregated by genre
    # ------------------------------------------------------------
    g = d.dropna(subset=["Genres"]).copy()
    g = explode_genres(g)

    q2_genre = (
        g.groupby("Genre")
        .agg(
            games_count=("Genre", "size"),
            avg_review_ratio=("ReviewRatio", "mean"),
            median_review_ratio=("ReviewRatio", "median"),
            avg_price=("Price", "mean"),
            median_price=("Price", "median"),
            avg_total_reviews=("TotalReviews", "mean"),
        )
        .reset_index()
        .sort_values("games_count", ascending=False)
    )

    q2_genre["avg_review_ratio"] = q2_genre["avg_review_ratio"].round(3)
    q2_genre["median_review_ratio"] = q2_genre["median_review_ratio"].round(3)
    q2_genre["avg_price"] = q2_genre["avg_price"].round(2)
    q2_genre["median_price"] = q2_genre["median_price"].round(2)
    q2_genre["avg_total_reviews"] = q2_genre["avg_total_reviews"].round(1)

    # ------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------
    safe_write_csv(q2_features, OUT_FEATURES)
    safe_write_csv(q2_genre, OUT_GENRE)

    print("\n✅ Wrote Q2 marts:")
    print(" -", OUT_FEATURES.resolve())
    print(" -", OUT_GENRE.resolve())

    print("\nPreview (features):")
    print(q2_features.head(5).to_string(index=False))

    print("\nPreview (genre agg):")
    print(q2_genre.head(10).to_string(index=False))


if __name__ == "__main__":
    main(min_total_reviews=10)
