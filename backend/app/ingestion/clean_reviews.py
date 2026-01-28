from pathlib import Path
import pandas as pd

# backend/app/ingestion/clean_reviews.py -> parents[3] = project root
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW = PROJECT_ROOT / "data" / "games.csv"
OUT = PROJECT_ROOT / "data" / "games_reviews_clean.csv"

# loads RAW, cleans review data, writes OUT
def main():
    print(f"Loading: {RAW}")
    df = pd.read_csv(RAW)

    # If Name looks wrong and AppID contains game names in your dataset:
    if "AppID" in df.columns:
        df["Name"] = df["AppID"]

    # fixes column name issues
    print("RAW COLUMNS:", [repr(c) for c in df.columns])
    df.columns = df.columns.astype(str).str.strip()
    print("STRIPPED COLUMNS:", df.columns.tolist())

    # Renames columns if needed
    rename_map = {}
    if "Positive" not in df.columns:
        for alt in ["positive", "Positive reviews", "Positive Reviews"]:
            if alt in df.columns:
                rename_map[alt] = "Positive"
                break
    if "Negative" not in df.columns:
        for alt in ["negative", "Negative reviews", "Negative Reviews"]:
            if alt in df.columns:
                rename_map[alt] = "Negative"
                break
    if rename_map:
        df = df.rename(columns=rename_map)

    needed = ["Name", "Price", "Genres", "Publishers", "Positive", "Negative"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}\nFound: {df.columns.tolist()}")

    df = df[needed].copy()

    # Clean Price
    df["Price"] = (
        df["Price"]
        .astype(str)
        .str.replace("Free to Play", "0", regex=False)
        .str.replace("Free", "0", regex=False)
        .str.replace("$", "", regex=False)
        .str.strip()
    )
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")

    # Positive / Negative -> numeric
    df["Positive"] = pd.to_numeric(df["Positive"], errors="coerce")
    df["Negative"] = pd.to_numeric(df["Negative"], errors="coerce")

    # Drop bad rows + avoid divide by zero
    df = df.dropna(subset=["Price", "Positive", "Negative"])
    df = df[(df["Positive"] + df["Negative"]) > 0]

    # Compute Positive Ratio
    df["PositiveRatio"] = df["Positive"] / (df["Positive"] + df["Negative"])

    # politely clean up text fields
    df["Genres"] = df["Genres"].astype(str).str.strip()
    df["Publishers"] = df["Publishers"].astype(str).str.strip()

    # Save cleaned file
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)

    # print summary
    print(f"\n✅ Wrote: {OUT.resolve()} rows={len(df):,}")
    print(df.head(5).to_string(index=False))
    print("PositiveRatio min/max:", df["PositiveRatio"].min(), df["PositiveRatio"].max())

if __name__ == "__main__":
    main()

