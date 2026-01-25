from fastapi import FastAPI

app = FastAPI(title="Game Pricing Intel API")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/version")
def version():
    return {"version": "0.1.0"}

from typing import Optional

from typing import Optional
import pandas as pd

@app.get("/games")
def games(max_price: Optional[float] = None, name_contains: Optional[str] = None):
    df = pd.read_csv("data/games.csv")

    # Filter by price
    if max_price is not None:
        df = df[df["price"] <= max_price]

    # Filter by name
    if name_contains is not None:
        df = df[df["name"].str.lower().str.contains(name_contains.lower())]

    return {"games": df.to_dict(orient="records")}
