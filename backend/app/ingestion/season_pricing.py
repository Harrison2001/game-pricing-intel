from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

# project root
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA = PROJECT_ROOT / "data" / "games_clean.csv"

def main():
    print(f"Loading: {DATA}")
    df = pd.read_csv(DATA)

    # Basic cleanup/safety
    df = df[df["Price"].notna()]
    df = df[df["Price"] >= 0]
    df = df[df["ReleaseSeason"].notna()]
    df = df[df["ReleaseMonth"].notna()]

    # Optional: focus on paid games only (comment out if you want all games)
    df_paid = df[df["Price"] > 0].copy()

    # -------- SEASON AGGS --------
    season_stats = (
        df_paid.groupby("ReleaseSeason")["Price"]
        .agg(avg_price="mean", median_price="median", count="size")
        .reindex(["Winter", "Spring", "Summer", "Fall"])  # consistent order
    )

    print("\nPaid games: price by release season")
    print(season_stats)

    # -------- MONTH AGGS --------
    month_stats = (
        df_paid.groupby("ReleaseMonth")["Price"]
        .agg(avg_price="mean", median_price="median", count="size")
        .sort_index()
    )

    print("\nPaid games: price by release month")
    print(month_stats)

    # -------- PLOTS --------
    # 1) Avg price by season
    plt.figure(figsize=(8, 5))
    season_stats["avg_price"].plot(kind="bar")
    plt.title("Average Price by Release Season (Paid Games Only)")
    plt.xlabel("Season")
    plt.ylabel("Average Price ($)")
    plt.tight_layout()
    out1 = PROJECT_ROOT / "data" / "season_avg_price_paid.png"
    plt.savefig(out1)

    # 2) Median price by season (more robust)
    plt.figure(figsize=(8, 5))
    season_stats["median_price"].plot(kind="bar")
    plt.title("Median Price by Release Season (Paid Games Only)")
    plt.xlabel("Season")
    plt.ylabel("Median Price ($)")
    plt.tight_layout()
    out2 = PROJECT_ROOT / "data" / "season_median_price_paid.png"
    plt.savefig(out2)

    # 3) Avg price by month (line)
    plt.figure(figsize=(9, 5))
    month_stats["avg_price"].plot(kind="line", marker="o")
    plt.title("Average Price by Release Month (Paid Games Only)")
    plt.xlabel("Release Month")
    plt.ylabel("Average Price ($)")
    plt.xticks(range(1, 13))
    plt.tight_layout()
    out3 = PROJECT_ROOT / "data" / "month_avg_price_paid.png"
    plt.savefig(out3)

    # 4) Release volume by month (context)
    plt.figure(figsize=(9, 5))
    month_stats["count"].plot(kind="bar")
    plt.title("Number of Releases by Month (Paid Games Only)")
    plt.xlabel("Release Month")
    plt.ylabel("Number of Games")
    plt.xticks(range(0, 12), range(1, 13))
    plt.tight_layout()
    out4 = PROJECT_ROOT / "data" / "month_release_count_paid.png"
    plt.savefig(out4)

    plt.show()

    print("\nSaved charts:")
    print(out1)
    print(out2)
    print(out3)
    print(out4)

if __name__ == "__main__":
    main()
