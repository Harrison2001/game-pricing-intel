from pathlib import Path
import pandas as pd

# backend/app/ingestion/clean_games.py -> parents[3] = project root
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW = PROJECT_ROOT / "data" / "games.csv"
OUT = PROJECT_ROOT / "data" / "games_clean.csv"


def month_to_season(month: int) -> str:
    if month in (12, 1, 2):
        return "Winter"
    if month in (3, 4, 5):
        return "Spring"
    if month in (6, 7, 8):
        return "Summer"
    return "Fall"


def debug_df(df: pd.DataFrame, label: str) -> None:
    print(f"\n--- {label} ---")
    print("shape:", df.shape)
    print("columns:", df.columns.tolist()[:30])
    print("head:")
    print(df.head(3).to_string(index=False))


def main():
    print(f"RAW path = {RAW.resolve()}")

    # Read full CSV robustly
    df = pd.read_csv(
        RAW,
        sep=",",
        quotechar='"',
        engine="python",
        on_bad_lines="skip"
    )

    # Strip column names
    df.columns = df.columns.astype(str).str.strip()

    # ------------------------------------------------------------
    # FIX SEMANTIC SHIFT (based on your dataset observation)
    #
    # Observed in your output:
    #   df["Name"]          actually contains the Release date
    #   df["Release date"]  actually contains the Estimated owners bucket
    #   df["AppID"]         actually contains the Game name
    #
    # So we remap BEFORE we filter down to needed columns.
    # ------------------------------------------------------------
    if "AppID" in df.columns and "Name" in df.columns and "Release date" in df.columns:
        original_release_date = df["Name"]          # save dates
        original_est_owners = df["Release date"]    # save owners bucket

        df["Name"] = df["AppID"]                    # real game name
        df["Release date"] = original_release_date  # real release date
        df["Estimated owners"] = original_est_owners

    # Keep only what we need
    needed = ["Name", "Release date", "Price"]
    if "Estimated owners" in df.columns:
        needed.append("Estimated owners")

    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing expected columns: {missing}\n"
            f"Found columns (first 50): {df.columns.tolist()[:50]}"
        )

    df = df[needed].copy()

    debug_df(df, "AFTER SEMANTIC FIX + FILTER")

    # Clean Name
    df["Name"] = df["Name"].astype(str).str.strip()

    # Clean Release date
    df["Release date"] = (
        df["Release date"]
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

    # Clean Price
    df["Price"] = (
        df["Price"]
        .astype(str)
        .str.replace("$", "", regex=False)
        .str.replace("Free to Play", "0", regex=False)
        .str.replace("Free", "0", regex=False)
        .str.strip()
    )
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")

    # Clean Owners (optional)
    if "Estimated owners" in df.columns:
        df["Estimated owners"] = df["Estimated owners"].astype(str).str.strip()

    # Parse Release date -> month
    dt = pd.to_datetime(df["Release date"], errors="coerce", format="mixed")
    df["ReleaseMonth"] = dt.dt.month

    # Month -> Season
    df["ReleaseSeason"] = df["ReleaseMonth"].apply(
        lambda m: month_to_season(int(m)) if pd.notna(m) else pd.NA
    )

    # Drop rows missing core fields
    df = df.dropna(subset=["Name", "Price"])

    # Save cleaned file
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)

    print(f"\n✅ Wrote cleaned file: {OUT.resolve()} rows={len(df):,}")
    print("Columns:", df.columns.tolist())

    preview_cols = [c for c in ["Name", "Release date", "Estimated owners", "ReleaseSeason", "Price"] if c in df.columns]
    print(df[preview_cols].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
