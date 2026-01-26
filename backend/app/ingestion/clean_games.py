from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[3] # ingestion -> app -> backend -> project root 
RAW = ROOT / "data" / "raw" / "games.csv"
OUT = ROOT / "data" / "clean" / "games_clean.csv" 

def main():
    df = pd.read_csv(RAW)

    # Basic cleaning -> keep what we only need for Question 1.
    keep = [c for c in ["name", "release date", "price"] if c in df.columns]
    df = df[keep]

    # Rename columns to be easier to work with
    if "name" in df.columns:
        df["name"] = df["name"].astype(str).str.strip()

    if "price" in df.columns: 
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df = df[df["price"].notna()]  # Remove NaN prices
        df = df[df["price"] >= 0]  # Remove negative prices)              
    
        # clean the release date column
        if "release date" in df.columns:
            df["release date"] = pd.to_datetime(df["release date"], errors="coerce")
            df = df[df["release date"].notna()]  # Remove invalid dates
            df["release_month"] = df["release date"].dt.month
            df["release_year"] = df["release date"].dt.year
    
        # save cleaned file
        df.to_csv(OUT, index=False)
        print(f"✅ Wrote cleaned file: {OUT} rows={len(df):,}")
    
    
    # run the main function if called directly
    if  __name__ == "__main__":
        main()
