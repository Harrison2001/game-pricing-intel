from fastapi import FastAPI
from typing import Optional
from pathlib import Path
import pandas as pd

app = FastAPI(title="Game Pricing Intel API")

DATA_PATH = Path(__file__).resolve().parents[2] / "data" / "games.csv"
# parents[2] goes: app/ -> backend/ -> project root

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/version")
def version():
    return {"version": "0.1.0"}

@app.get("/games")
def games(
    max_price: Optional[float] = None,
    name_contains: Optional[str] = None,
    limit: int = 50
):
    # Load dataset
    df = pd.read_csv(DATA_PATH)

    # Basic cleaning: make columns easier
    if "name" in df.columns:
        df["name"] = df["name"].astype(str)

    # Filtering
    if max_price is not None and "price" in df.columns:
        df = df[df["price"] <= max_price]

    if name_contains:
        df = df[df["name"].str.contains(name_contains, case=False, na=False)]

    # Limit results so we don't send millions of rows
    df = df.head(limit)

    # Return list of dicts (FastAPI will turn into JSON)
    return df.to_dict(orient="records")

