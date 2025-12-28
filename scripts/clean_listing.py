import pandas as pd
import re


class ListingDataCleaner:
    """Clean already processed real estate listings data."""

    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.df = None
        self.removed_df = None
        self.n_removed = 0

    def _log(self, message: str):
        """Print message if verbose mode is enabled."""
        if self.verbose:
            print(message)

    def load_data(self, data_path: str) -> "ListingDataCleaner":
        """Load the listings data."""
        self._log(f"📂 Loading data from: {data_path}")
        self.df = pd.read_csv(data_path)
        self._log(
            f"✅ Data loaded successfully: {len(self.df):,} rows, {len(self.df.columns)} columns"
        )
        return self

    def select_columns(self) -> "ListingDataCleaner":
        """Select only relevant columns for analysis."""
        self._log("📋 Selecting relevant columns...")

        selected_cols = [
            "url",
            "title",
            "description",
            "fetch_date",
            "house_type",
            "bathrooms",
            "bedrooms",
            "price",
            "locality",
            "Condition",
            "Furnishing",
            "Property Size",
            "24-hour Electricity",
            "Air Conditioning",
            "Apartment",
            "Balcony",
            "Chandelier",
            "Dining Area",
            "Dishwasher",
            "Hot Water",
            "Kitchen Cabinets",
            "Kitchen Shelf",
            "Microwave",
            "Pop Ceiling",
            "Pre-Paid Meter",
            "Refrigerator",
            "TV",
            "Tiled Floor",
            "Wardrobe",
            "Wi-Fi",
        ]

        # Only keep columns that exist in the dataframe
        available_cols = [col for col in selected_cols if col in self.df.columns]
        missing_cols = [col for col in selected_cols if col not in self.df.columns]

        if missing_cols:
            self._log(f"⚠️ Warning: {len(missing_cols)} columns not found in data")
            if self.verbose:
                self._log(f"   Missing: {', '.join(missing_cols)}")

        initial_cols = len(self.df.columns)
        self.df = self.df[available_cols].copy()
        dropped_cols = initial_cols - len(available_cols)

        self._log(f"✅ Selected {len(available_cols)} columns")
        if dropped_cols > 0:
            self._log(f"   Dropped {dropped_cols} unnecessary columns")

        return self

    def drop_missing_condition(self) -> "ListingDataCleaner":
        """Drop rows with missing Condition values."""
        self._log("🏚️ Removing rows with missing Condition...")

        if "Condition" not in self.df.columns:
            self._log("⚠️ No 'Condition' column found, skipping")
            return self

        initial_rows = len(self.df)
        missing_count = self.df["Condition"].isna().sum()

        self.df = self.df.dropna(subset=["Condition"]).copy()
        rows_dropped = initial_rows - len(self.df)

        if rows_dropped > 0:
            self._log(f"🗑️ Dropped {rows_dropped} rows with missing Condition")
        else:
            self._log("✅ No missing Condition values found")

        return self

    def clean_property_size(self) -> "ListingDataCleaner":
        """Rename Property Size column and drop missing values."""
        self._log("📏 Cleaning property size data...")

        if "Property Size" not in self.df.columns:
            self._log("⚠️ No 'Property Size' column found, skipping")
            return self

        # Rename column
        self.df.rename(columns={"Property Size": "property_size"}, inplace=True)
        self._log("   Renamed 'Property Size' to 'property_size'")

        # Check missing values
        missing_count = self.df["property_size"].isna().sum()
        initial_rows = len(self.df)

        # Drop missing values
        self.df = self.df.dropna(subset=["property_size"]).copy()
        rows_dropped = initial_rows - len(self.df)

        if rows_dropped > 0:
            self._log(f"🗑️ Dropped {rows_dropped} rows with missing property_size")
        else:
            self._log("✅ No missing property_size values found")

        return self

    @staticmethod
    def _build_regex(patterns):
        """Build regex pattern from list of strings."""
        escaped = [re.escape(p) for p in patterns]
        return "|".join(escaped)

    def remove_sale_and_short_term(self) -> "ListingDataCleaner":
        """Remove listings that appear to be for sale or short-term rentals."""
        self._log("🚫 Filtering out sale and short-term rental listings...")

        # Check if required columns exist
        if "title" not in self.df.columns or "description" not in self.df.columns:
            self._log("⚠️ Missing title or description columns, skipping filter")
            return self

        # Define patterns for sale listings
        sale_patterns = [
            "for sale",
            "on sale",
            "sale",
            "sale only",
            "selling",
            "sell",
            "sold",
            "property for sale",
            "house for sale",
            "apartment for sale",
            "buy",
            "buyer",
            "title deed",
            "deed",
            "closing",
            "escrow",
            "transfer",
            "down payment",
            "mortgage",
            "loan",
            "financing",
            "cash only",
            "investment",
            "roi",
            "yield",
            "capital gain",
            "airbnb",
            "air bnb",
            "booking.com",
            "vrbo",
            "short stay",
            "holiday rental",
            "vacation rental",
            "tourist rental",
            "guesthouse",
            "guest house",
        ]

        # Define patterns for short-term/period rentals
        period_patterns = [
            "per night",
            "nightly",
            "per day",
            "daily",
            "by the day",
            "short term",
            "short-term",
            "weekly",
            "per week",
            "weekend",
            "airbnb",
            "air bnb",
            "business short",
            "holidays",
        ]

        sale_regex = self._build_regex(sale_patterns)
        period_regex = self._build_regex(period_patterns)

        # Combine title and description for matching
        text = (
            self.df["title"].fillna("") + " " + self.df["description"].fillna("")
        ).str.lower()

        # Flag rows to remove
        df_temp = self.df.copy()
        df_temp["sale_match"] = text.str.contains(sale_regex, regex=True, na=False)
        df_temp["period_match"] = text.str.contains(period_regex, regex=True, na=False)
        df_temp["remove"] = df_temp["sale_match"] | df_temp["period_match"]

        # Store statistics
        initial_rows = len(self.df)
        self.n_removed = df_temp["remove"].sum()
        self.removed_df = df_temp[df_temp["remove"]].copy()

        # Filter out unwanted listings
        self.df = df_temp[~df_temp["remove"]].copy()

        # Drop the temporary flag columns
        self.df = self.df.drop(columns=["sale_match", "period_match", "remove"])

        self._log(f"   Sale matches: {df_temp['sale_match'].sum()}")
        self._log(f"   Period matches: {df_temp['period_match'].sum()}")
        self._log(f"🗑️ Removed {self.n_removed} sale/short-term listings")
        self._log(f"✅ Remaining listings: {len(self.df):,}")

        return self

    def clean_price(self):
        self.df["price"] = (
            self.df["price"].str.replace("GH₵ ", "").str.replace(",", "").astype(float)
        )

    def clean_all(self) -> pd.DataFrame:
        """Execute full cleaning pipeline."""
        self._log("\n" + "=" * 60)
        self._log("🚀 Starting Listing Data Cleaning Pipeline")
        self._log("=" * 60 + "\n")

        self.clean_price()
        self.select_columns()
        self.drop_missing_condition()
        self.clean_property_size()
        self.remove_sale_and_short_term()

        self._log("\n" + "=" * 60)
        self._log(f"✨ Listing Data Cleaning Complete!")
        self._log(
            f"📊 Final dataset: {len(self.df):,} rows, {len(self.df.columns)} columns"
        )
        if self.n_removed > 0:
            self._log(f"🗑️ Total removed: {self.n_removed:,} rows")
        self._log("=" * 60 + "\n")

        return self.df

    def get_dataframe(self) -> pd.DataFrame:
        """Get the cleaned DataFrame."""
        return self.df

    def get_removed_dataframe(self) -> pd.DataFrame:
        """Get the DataFrame of removed listings."""
        return self.removed_df

    def save_data(self, output_path: str) -> "ListingDataCleaner":
        """Save the cleaned data to CSV."""
        self._log(f"💾 Saving cleaned data to: {output_path}")
        self.df.to_csv(output_path, index=False)
        self._log(f"✅ Data saved successfully: {len(self.df):,} rows")
        return self


def clean_listing_data(
    data_path: str, output_path: str = None, verbose: bool = True
) -> pd.DataFrame:
    """
    Clean listing data in one line.

    Args:
        data_path: Path to input CSV file
        output_path: Optional path to save cleaned data
        verbose: Whether to print progress messages

    Returns:
        Cleaned DataFrame
    """
    cleaner = ListingDataCleaner(verbose=verbose)
    df = cleaner.load_data(data_path).clean_all()

    if output_path:
        cleaner.save_data(output_path)

    return df
