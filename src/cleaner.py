import pandas as pd
import re

RAW_CSV = "data/raw.csv"
CLEANED_CSV = "data/cleaned.csv"

def clean_price(x):
    if pd.isna(x) or x == "":
        return None
    x = str(x).replace("$", "").replace(",", ".").strip()
    match = re.search(r"[-+]?\d*\.?\d+", x)
    return float(match.group()) if match else None

def clean_discount(x):
    if pd.isna(x) or x == "":
        return 0
    match = re.search(r"\d+", str(x))
    return int(match.group()) if match else 0

def run_cleaning(in_path=RAW_CSV, out_path=CLEANED_CSV):
    df = pd.read_csv(in_path)
    df["title"] = df["title"].str.strip()
    df = df[df["title"].notna()]

    df["base_price"] = df["base_price"].apply(clean_price)
    df["final_price"] = df["final_price"].apply(clean_price)
    df["discount_percent"] = df["discount"].apply(clean_discount)

    df["base_price"].fillna(df["final_price"], inplace=True)
    df["discount_amount"] = df["base_price"] - df["final_price"]
    df["is_discounted"] = (df["discount_percent"] > 0).astype(int)

    if "discount" in df.columns:
        df = df.drop(columns=["discount"])


    if "product_id" in df.columns:
        df = df.drop_duplicates(subset=["product_id"])
    else:
        df = df.drop_duplicates(subset=["title", "final_price"])

    df = df.reset_index(drop=True)
    df.to_csv(out_path, index=False, float_format="%.2f")
    print(f"[cleaner] saved cleaned data to {out_path} ({len(df)} rows)")
    return df

if __name__ == "__main__":
    run_cleaning()
