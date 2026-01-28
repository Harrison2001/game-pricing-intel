from fastapi import FastAPI, HTTPException
from typing import Optional
from pathlib import Path
import pandas as pd

print("LOADED:", __file__)

print("LOADED MAIN:", __file__)
print("PROJECT_ROOT:", Path(__file__).resolve().parents[2]) # -> project root
print("DATA_PATH:", (Path(__file__).resolve().parents[3] / "data" / "games_clean.csv")) #-> data file


app = FastAPI(title="Game Pricing Intel API")

# project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_Path = PROJECT_ROOT / "data"
# main data file
Games_CLEAN = DATA_Path / "games_clean.csv"
REVIEWS_CLEAN = DATA_Path / "games_reviews_clean.csv"

# loads DATA_PATH robustly, with error handling
def load_data() -> pd.DataFrame:
    if not DATA_Path.exists():
        raise HTTPException(
            status_code=500,
            detail=f"Missing file: {DATA_Path}. Run clean_games.py to generate games_clean.csv."
        )

    try:
        df = pd.read_csv(DATA_PATH, engine="python", keep_default_na=False)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read CSV: {e}")

    df.columns = df.columns.astype(str).str.strip()
    return df

# ------------------------------
# Basic endpoints
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/version")
def version():
    return {"version": "0.1.0"}


@app.get("/data-path")
def data_path():
    return {"data_path": str(DATA_PATH), "exists": DATA_PATH.exists()}


@app.get("/games")
def games(
    max_price: Optional[float] = None,
    name_contains: Optional[str] = None,
    limit: int = 50
):
    #------------------------------

    # Load the data and apply filters
    df = load_data()

    NAME_COL = "Name"
    PRICE_COL = "Price"

    if NAME_COL in df.columns:
        df[NAME_COL] = df[NAME_COL].astype(str).str.strip()

    if PRICE_COL in df.columns:
        df[PRICE_COL] = pd.to_numeric(df[PRICE_COL], errors="coerce")

    if max_price is not None and PRICE_COL in df.columns:
        df = df[df[PRICE_COL].notna()]
        df = df[df[PRICE_COL] <= max_price]

    if name_contains and NAME_COL in df.columns:
        df = df[df[NAME_COL].str.contains(name_contains, case=False, na=False)]

    limit = max(1, min(int(limit), 500))
    return df.head(limit).to_dict(orient="records")


# -----------------------------
# Analytics: by owners (BQ #1)
# -----------------------------
@app.get("/analytics/pricing/by-owners")
def pricing_by_owners():
    df = load_data()

    OWNERS_COL = "Estimated owners"
    PRICE_COL = "Price"
    required = {OWNERS_COL, PRICE_COL}

    if not required.issubset(df.columns):
        raise HTTPException(
            status_code=500,
            detail=f"Missing columns. Need {sorted(required)}. Found: {df.columns.tolist()}"
        )

    df[OWNERS_COL] = df[OWNERS_COL].astype(str).str.strip()
    df[PRICE_COL] = pd.to_numeric(df[PRICE_COL], errors="coerce")

    df = df[df[OWNERS_COL] != ""]
    df = df.dropna(subset=[PRICE_COL])

    out = (
        df.groupby(OWNERS_COL)[PRICE_COL]
        .agg(
            games_count="count",
            avg_price="mean",
            median_price="median",
            min_price="min",
            max_price="max",
        )
        .reset_index()
        .rename(columns={OWNERS_COL: "Estimated owners"})
    )

    for col in ["avg_price", "median_price", "min_price", "max_price"]:
        out[col] = out[col].round(2)

    # sort owner buckets by the left number (e.g., "0 - 20000" -> 0)
    def bucket_left(s: str) -> int:
        try:
            left = s.split("-")[0].strip().replace(",", "")
            return int(left)
        except Exception:
            return 10**18

    out["__sort"] = out["Estimated owners"].apply(bucket_left)
    out = out.sort_values(["__sort", "Estimated owners"]).drop(columns="__sort")

    return out.to_dict(orient="records")


# -----------------------------
# Analytics: by month
# -----------------------------
@app.get("/analytics/pricing/by-month")
def pricing_by_month():
    df = load_data()

    PRICE_COL = "Price"
    MONTH_COL = "ReleaseMonth"
    required = {PRICE_COL, MONTH_COL}

    if not required.issubset(df.columns):
        raise HTTPException(
            status_code=500,
            detail=f"Missing columns. Need {sorted(required)}. Found: {df.columns.tolist()}"
        )

    df[PRICE_COL] = pd.to_numeric(df[PRICE_COL], errors="coerce")
    df[MONTH_COL] = pd.to_numeric(df[MONTH_COL], errors="coerce")
    df = df.dropna(subset=[PRICE_COL, MONTH_COL])

    out = (
        df.groupby(MONTH_COL)[PRICE_COL]
        .agg(
            games_count="count",
            avg_price="mean",
            median_price="median",
            min_price="min",
            max_price="max",
        )
        .reset_index()
        .sort_values(MONTH_COL)
    )

    for col in ["avg_price", "median_price", "min_price", "max_price"]:
        out[col] = out[col].round(2)

    return out.to_dict(orient="records")


# -----------------------------
# Analytics: by season
# -----------------------------
@app.get("/analytics/pricing/by-season")
def pricing_by_season():
    df = load_data()

    PRICE_COL = "Price"
    SEASON_COL = "ReleaseSeason"
    required = {PRICE_COL, SEASON_COL}

    if not required.issubset(df.columns):
        raise HTTPException(
            status_code=500,
            detail=f"Missing columns. Need {sorted(required)}. Found: {df.columns.tolist()}"
        )

    df[PRICE_COL] = pd.to_numeric(df[PRICE_COL], errors="coerce")
    df[SEASON_COL] = df[SEASON_COL].astype(str).str.strip()

    df = df.dropna(subset=[PRICE_COL, SEASON_COL])
    df = df[df[SEASON_COL] != "None"]
    df = df[df[SEASON_COL] != ""]

    season_order = ["Winter", "Spring", "Summer", "Fall"]

    out = (
        df.groupby(SEASON_COL)[PRICE_COL]
        .agg(
            games_count="count",
            avg_price="mean",
            median_price="median",
            min_price="min",
            max_price="max",
        )
        .reset_index()
        .rename(columns={SEASON_COL: "ReleaseSeason"})
    )

    out["__order"] = out["ReleaseSeason"].apply(
        lambda s: season_order.index(s) if s in season_order else 999
    )
    out = out.sort_values("__order").drop(columns="__order")

    for col in ["avg_price", "median_price", "min_price", "max_price"]:
        out[col] = out[col].round(2)

    return out.to_dict(orient="records")
