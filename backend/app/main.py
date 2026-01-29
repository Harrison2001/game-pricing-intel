from fastapi import FastAPI, HTTPException
from typing import Optional
from pathlib import Path
import pandas as pd

print("LOADED MAIN:", __file__)

app = FastAPI(title="Game Pricing Intel API")

# ------------------------------------------------------------
# Paths (main.py lives in backend/app/main.py)
# parents[2] -> project root (folder that contains /data)
# ------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = PROJECT_ROOT / "data"
GAMES_CLEAN = DATA_PATH / "games_clean.csv" 


# ------------------------------------------------------------
# Helper: load CSV safely
# ------------------------------------------------------------
def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise HTTPException(status_code=500, detail=f"Missing file: {path}")

    try:
        df = pd.read_csv(path, keep_default_na=False)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read CSV: {e}")

    df.columns = df.columns.astype(str).str.strip()
    return df


# ------------------------------------------------------------
# Basic endpoints
# ------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/data-paths")
def data_paths():
    return {
        "project_root": str(PROJECT_ROOT),
        "data_path": str(DATA_PATH),
        "games_clean": str(GAMES_CLEAN),
        "games_clean_exists": GAMES_CLEAN.exists(),
        "reviews_clean": str(REVIEWS_CLEAN),
        "reviews_clean_exists": REVIEWS_CLEAN.exists(),
    }


# ------------------------------------------------------------
# Simple list endpoint (useful for debugging)
# ------------------------------------------------------------
@app.get("/games")
def games(
    max_price: Optional[float] = None,
    name_contains: Optional[str] = None,
    limit: int = 50
):
    df = load_csv(GAMES_CLEAN)

    if "Price" in df.columns:
        df["Price"] = pd.to_numeric(df["Price"], errors="coerce")

    if max_price is not None and "Price" in df.columns:
        df = df[df["Price"].notna() & (df["Price"] <= max_price)]

    if name_contains and "Name" in df.columns:
        df = df[df["Name"].astype(str).str.contains(name_contains, case=False, na=False)]

    limit = max(1, min(int(limit), 500))
    return df.head(limit).to_dict(orient="records")


# ============================================================
# Q1) Seasonal Pricing
# ============================================================
@app.get("/analytics/pricing/by-month")
def pricing_by_month():
    df = load_csv(GAMES_CLEAN)

    required = {"Price", "ReleaseMonth"}
    if not required.issubset(df.columns):
        raise HTTPException(status_code=500, detail=f"Need columns {sorted(required)}")

    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df["ReleaseMonth"] = pd.to_numeric(df["ReleaseMonth"], errors="coerce")
    df = df.dropna(subset=["Price", "ReleaseMonth"])

    out = (
        df.groupby("ReleaseMonth")["Price"]
        .agg(
            games_count="count",
            avg_price="mean",
            median_price="median",
            min_price="min",
            max_price="max",
        )
        .reset_index()
        .sort_values("ReleaseMonth")
    )

    for col in ["avg_price", "median_price", "min_price", "max_price"]:
        out[col] = out[col].round(2)

    return out.to_dict(orient="records")


@app.get("/analytics/pricing/by-season")
def pricing_by_season():
    df = load_csv(GAMES_CLEAN)

    required = {"Price", "ReleaseSeason"}
    if not required.issubset(df.columns):
        raise HTTPException(status_code=500, detail=f"Need columns {sorted(required)}")

    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df["ReleaseSeason"] = df["ReleaseSeason"].astype(str).str.strip()
    df = df.dropna(subset=["Price", "ReleaseSeason"])
    df = df[df["ReleaseSeason"].isin(["Winter", "Spring", "Summer", "Fall"])]

    season_order = ["Winter", "Spring", "Summer", "Fall"]

    out = (
        df.groupby("ReleaseSeason")["Price"]
        .agg(
            games_count="count",
            avg_price="mean",
            median_price="median",
            min_price="min",
            max_price="max",
        )
        .reset_index()
    )

    out["__order"] = out["ReleaseSeason"].apply(lambda s: season_order.index(s))
    out = out.sort_values("__order").drop(columns="__order")

    for col in ["avg_price", "median_price", "min_price", "max_price"]:
        out[col] = out[col].round(2)

    return out.to_dict(orient="records")


# ============================================================
# Q2) Reviews vs Price & Features
# ============================================================
@app.get("/analytics/reviews/price-vs-ratio")
def price_vs_review_ratio(limit: int = 2000):
    df = load_csv(GAMES_CLEAN)

    required = {"Price", "ReviewRatio"}
    if not required.issubset(df.columns):
        raise HTTPException(status_code=500, detail=f"Need columns {sorted(required)}")

    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df["ReviewRatio"] = pd.to_numeric(df["ReviewRatio"], errors="coerce")
    df = df.dropna(subset=["Price", "ReviewRatio"])

    limit = max(100, min(int(limit), 20000))
    return df[["Name", "Price", "ReviewRatio", "Genres", "Publisher"]].head(limit).to_dict(orient="records")


@app.get("/analytics/reviews/by-genre")
def reviews_by_genre(top_n: int = 25):
    df = load_csv(GAMES_CLEAN)

    required = {"Genres", "ReviewRatio"}
    if not required.issubset(df.columns):
        raise HTTPException(status_code=500, detail=f"Need columns {sorted(required)}")

    df["ReviewRatio"] = pd.to_numeric(df["ReviewRatio"], errors="coerce")
    df["Genres"] = df["Genres"].astype(str)

    df = df.dropna(subset=["Genres", "ReviewRatio"])

    # split "Action, Indie" into multiple rows
    df = df.assign(Genre=df["Genres"].str.split(",")).explode("Genre")
    df["Genre"] = df["Genre"].astype(str).str.strip()
    df = df[df["Genre"] != ""]

    out = (
        df.groupby("Genre")["ReviewRatio"]
        .agg(
            games_count="count",
            avg_review_ratio="mean",
            median_review_ratio="median",
        )
        .reset_index()
        .sort_values("games_count", ascending=False)
    )

    out["avg_review_ratio"] = out["avg_review_ratio"].round(3)
    out["median_review_ratio"] = out["median_review_ratio"].round(3)

    top_n = max(5, min(int(top_n), 100))
    return out.head(top_n).to_dict(orient="records")


@app.get("/analytics/reviews/by-publisher")
def reviews_by_publisher(top_n: int = 25):
    df = load_csv(GAMES_CLEAN)

    required = {"Publisher", "ReviewRatio"}
    if not required.issubset(df.columns):
        raise HTTPException(status_code=500, detail=f"Need columns {sorted(required)}")

    df["ReviewRatio"] = pd.to_numeric(df["ReviewRatio"], errors="coerce")
    df["Publisher"] = df["Publisher"].astype(str).str.strip()
    df = df.dropna(subset=["Publisher", "ReviewRatio"])
    df = df[df["Publisher"] != ""]

    out = (
        df.groupby("Publisher")["ReviewRatio"]
        .agg(
            games_count="count",
            avg_review_ratio="mean",
            median_review_ratio="median",
        )
        .reset_index()
        .sort_values("games_count", ascending=False)
    )

    out["avg_review_ratio"] = out["avg_review_ratio"].round(3)
    out["median_review_ratio"] = out["median_review_ratio"].round(3)

    top_n = max(5, min(int(top_n), 100))
    return out.head(top_n).to_dict(orient="records")


# ============================================================
# Q3) Market Pricing Segments
# ============================================================
@app.get("/analytics/market/free-vs-paid")
def free_vs_paid():
    df = load_csv(GAMES_CLEAN)

    required = {"IsFree"}
    if not required.issubset(df.columns):
        raise HTTPException(status_code=500, detail=f"Need columns {sorted(required)}")

    # IsFree might be True/False strings in CSV depending on pandas version
    isfree = df["IsFree"].astype(str).str.lower().isin(["true", "1", "yes"])
    total = len(df)

    free_count = int(isfree.sum())
    paid_count = int(total - free_count)

    return {
        "total_games": total,
        "free_games": free_count,
        "paid_games": paid_count,
        "free_pct": round(free_count / total * 100, 2) if total else 0.0,
        "paid_pct": round(paid_count / total * 100, 2) if total else 0.0,
    }


@app.get("/analytics/market/avg-price-by-genre")
def avg_price_by_genre(top_n: int = 25):
    df = load_csv(GAMES_CLEAN)

    required = {"Genres", "Price"}
    if not required.issubset(df.columns):
        raise HTTPException(status_code=500, detail=f"Need columns {sorted(required)}")

    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df["Genres"] = df["Genres"].astype(str)

    df = df.dropna(subset=["Genres", "Price"])

    df = df.assign(Genre=df["Genres"].str.split(",")).explode("Genre")
    df["Genre"] = df["Genre"].astype(str).str.strip()
    df = df[df["Genre"] != ""]

    out = (
        df.groupby("Genre")["Price"]
        .agg(
            games_count="count",
            avg_price="mean",
            median_price="median",
        )
        .reset_index()
        .sort_values("games_count", ascending=False)
    )

    out["avg_price"] = out["avg_price"].round(2)
    out["median_price"] = out["median_price"].round(2)

    top_n = max(5, min(int(top_n), 100))
    return out.head(top_n).to_dict(orient="records")
