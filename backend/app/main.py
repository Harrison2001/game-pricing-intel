from fastapi import FastAPI

app = FastAPI(title="Game Pricing Intel API")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/version")
def version():
    return {"version": "0.1.0"}

from typing import Optional

@app.get("/games")
def games(max_price: Optional[float] = None, name_contains: Optional[str] = None):
    game_list = [
        {"id": 1, "name": "Stardew Valley", "price": 14.99},
        {"id": 2, "name": "Hades", "price": 24.99},
        {"id": 3, "name": "Celeste", "price": 19.99}
    ]

    # Filter by price
    if max_price is not None:
        game_list = [g for g in game_list if g["price"] <= max_price]

    # Filter by name (case-insensitive search)
    if name_contains is not None:
        game_list = [
            g for g in game_list
            if name_contains.lower() in g["name"].lower()
        ]

    return {"games": game_list}
