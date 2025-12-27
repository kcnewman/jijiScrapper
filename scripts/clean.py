import pandas as pd
import ast
import pathlib
from datetime import datetime


class DataCleaner:
    """Data cleaning pipeline for real estate listings."""

    def __init__(self, keep_original_columns: bool = True, verbose: bool = True):
        self.keep_original_columns = keep_original_columns
        self.verbose = verbose

    def _log(self, message: str):
        """Print message if verbose mode is enabled."""
        if self.verbose:
            print(message)

    def load_data(self, data_path: str) -> "DataCleaner":
        self._log(f"📂 Loading data from: {data_path}")
        self.df = pd.read_csv(data_path)
        self._log(
            f"✅ Data loaded successfully: {len(self.df):,} rows, {len(self.df.columns)} columns"
        )
        return self

    @staticmethod
    def _get_sub_location(loc):
        """Extract sub-location from location string."""
        parts = str(loc).split(",")
        if len(parts) >= 3:
            return parts[1].strip()
        return parts[0]

    def append_scraping_date(self) -> "DataCleaner":
        """Use fetch_date from data or add current date if missing"""
        self._log("📅 Processing scraping date...")

        if "fetch_date" not in self.df.columns:
            self.df["fetch_date"] = datetime.now()
            self._log(
                f"✅ Added fetch_date column with current date: {datetime.now().strftime('%Y-%m-%d')}"
            )
        else:
            # Convert to datetime if it's a string
            self.df["fetch_date"] = pd.to_datetime(
                self.df["fetch_date"], errors="coerce"
            )
            # Fill any NaT values with current date
            nat_count = self.df["fetch_date"].isna().sum()
            self.df["fetch_date"].fillna(datetime.now(), inplace=True)
            if nat_count > 0:
                self._log(
                    f"✅ Filled {nat_count} missing fetch_date values with current date"
                )
            else:
                self._log("✅ All fetch_date values are valid")

        return self

    def extract_sub_location(self) -> "DataCleaner":
        """Extract sub-location from location column."""
        self._log("📍 Extracting locality from location...")

        self.df["locality"] = self.df["location"].apply(self._get_sub_location)
        unique_localities = self.df["locality"].nunique()
        self._log(f"✅ Extracted {unique_localities} unique localities")

        if not self.keep_original_columns:
            self.df = self.df.drop("location", axis=1)
            self._log("🗑️ Dropped original 'location' column")

        return self

    def fill_missing_house_type(self) -> "DataCleaner":
        """Fill missing house_type values with 'Bedsitter'."""
        self._log("🏠 Filling missing house types...")

        missing_count = self.df["house_type"].isna().sum()
        if missing_count > 0:
            self.df.loc[self.df["house_type"].isna(), "house_type"] = "Bedsitter"
            self._log(
                f"✅ Filled {missing_count} missing house_type values with 'Bedsitter'"
            )
        else:
            self._log("✅ No missing house_type values found")

        return self

    def clean_bathrooms_bedrooms(self) -> "DataCleaner":
        """Extract numeric values from bathrooms and bedrooms columns."""
        self._log("🚿🛏️ Cleaning bathrooms and bedrooms data...")

        initial_rows = len(self.df)
        bathroom_missing = self.df["bathrooms"].isna().sum()
        bedroom_missing = self.df["bedrooms"].isna().sum()

        self._log(f"   Missing bathroom data before cleaning: {bathroom_missing}")
        self._log(f"   Missing bedroom data before cleaning: {bedroom_missing}")

        # Drop rows with missing bathrooms or bedrooms
        self.df.dropna(subset=["bathrooms"], inplace=True)
        self.df.dropna(subset=["bedrooms"], inplace=True)

        rows_dropped = initial_rows - len(self.df)
        if rows_dropped > 0:
            self._log(
                f"🗑️ Dropped {rows_dropped} rows with missing bathroom/bedroom data"
            )

        # Extract numeric values
        self.df["bathrooms"] = (
            self.df["bathrooms"].str.split(" ").str.get(0).str.strip()
        )
        self.df["bedrooms"] = self.df["bedrooms"].str.split(" ").str.get(0).str.strip()

        bathroom_missing_after = self.df["bathrooms"].isna().sum()
        bedroom_missing_after = self.df["bedrooms"].isna().sum()

        self._log(f"   Missing bathroom data after cleaning: {bathroom_missing_after}")
        self._log(f"   Missing bedroom data after cleaning: {bedroom_missing_after}")
        self._log(f"✅ Cleaned bathrooms and bedrooms successfully")

        return self

    def extract_properties(self) -> "DataCleaner":
        """Expand the properties column into separate columns."""
        self._log("🏗️ Extracting property attributes...")

        initial_columns = len(self.df.columns)

        self.df["properties"] = self.df["properties"].apply(ast.literal_eval)
        properties = self.df["properties"].apply(pd.Series)
        self.df = self.df.join(properties)

        if not self.keep_original_columns:
            self.df = self.df.drop("properties", axis=1)
            self._log("🗑️ Dropped original 'properties' column")

        new_columns = len(self.df.columns) - initial_columns
        self._log(f"✅ Properties extracted: added {new_columns} new attributes!")

        return self

    def extract_amenities(self) -> "DataCleaner":
        """Expand amenities column into binary columns for each amenity."""
        self._log("🛋️ Extracting amenities...")

        initial_columns = len(self.df.columns)

        amenities_encoded = self.df["amenities"].str.strip().str.get_dummies(sep=",")
        amenities_encoded.columns = amenities_encoded.columns.str.strip()
        self.df = self.df.join(amenities_encoded)

        if not self.keep_original_columns:
            self.df = self.df.drop("amenities", axis=1)
            self._log("🗑️ Dropped original 'amenities' column")

        new_amenity_columns = len(self.df.columns) - initial_columns
        self._log(
            f"✅ Amenities extracted: added {new_amenity_columns} new attributes!"
        )

        return self

    def extract_facilities(self) -> "DataCleaner":
        """Expand facilities column into binary columns for each facility."""
        if "Facilities" not in self.df.columns:
            self._log("⚠️ No 'Facilities' column found, skipping facility extraction")
            return self

        self._log("🏢 Extracting facilities...")

        initial_columns = len(self.df.columns)

        facilities_encoded = self.df["Facilities"].str.strip().str.get_dummies(sep=",")
        facilities_encoded.columns = facilities_encoded.columns.str.strip()

        # Update existing columns with facility data
        self.df.update(facilities_encoded)

        # Add any new facility columns that don't already exist
        for col in facilities_encoded.columns:
            if col not in self.df.columns:
                self.df[col] = facilities_encoded[col]

        new_facility_columns = len(self.df.columns) - initial_columns
        if new_facility_columns > 0:
            self._log(
                f"✅ Facilities extracted: added {new_facility_columns} new attributes!"
            )
        else:
            self._log(f"✅ Facilities extracted: updated existing attributes")

        return self

    def clean_price(self) -> "DataCleaner":
        """Clean price column by removing currency symbols and converting to float."""
        if "price" not in self.df.columns:
            self._log("⚠️ No 'price' column found, skipping price cleaning")
            return self

        self._log("💰 Cleaning price data...")

        initial_dtype = self.df["price"].dtype
        self.df["price"] = (
            self.df["price"]
            .str.replace("GH₵ ", "", regex=False)
            .str.replace(",", "", regex=False)
            .astype(float)
        )

        self._log(f"✅ Price cleaned: converted from {initial_dtype} to float")
        self._log(
            f"   Price range: GH₵ {self.df['price'].min():,.2f} - GH₵ {self.df['price'].max():,.2f}"
        )

        return self

    def clean_all(self) -> pd.DataFrame:
        """Execute full cleaning pipeline."""
        self._log("\n" + "=" * 60)
        self._log("🚀 Starting Data Cleaning Pipeline")
        self._log("=" * 60 + "\n")

        self.append_scraping_date()
        self.extract_sub_location()
        self.fill_missing_house_type()
        self.clean_bathrooms_bedrooms()
        self.extract_properties()
        self.extract_amenities()
        self.extract_facilities()
        self.clean_price()

        self._log("\n" + "=" * 60)
        self._log(f"✨ Data Cleaning Complete!")
        self._log(
            f"📊 Final dataset: {len(self.df):,} rows, {len(self.df.columns)} columns"
        )
        self._log("=" * 60 + "\n")

        return self.df

    def get_dataframe(self) -> pd.DataFrame:
        """Get the current DataFrame."""
        return self.df


def clean_data(
    data_path: str, keep_original_columns: bool = True, verbose: bool = True
) -> pd.DataFrame:
    """Cleaning function to clean data in one line."""
    cleaner = DataCleaner(keep_original_columns=keep_original_columns, verbose=verbose)
    return cleaner.load_data(data_path).clean_all()


def combine_urls(
    urls_directory: str | None = None, verbose: bool = True
) -> pd.DataFrame | None:
    """Combine multiple URL CSV files into a single deduplicated file."""

    def _log(message: str):
        if verbose:
            print(message)

    _log("\n" + "=" * 60)
    _log("🔗 Starting URL Combination Process")
    _log("=" * 60 + "\n")

    if urls_directory is None:
        urls_dir = pathlib.Path(__file__).resolve().parents[1] / "outputs" / "urls"
    else:
        urls_dir = pathlib.Path(urls_directory)

    if not urls_dir.exists():
        _log(f"❌ URLs directory not found: {urls_dir}")
        return None

    output_file = urls_dir / "combined_urls.csv"

    csv_files = [f for f in urls_dir.glob("*.csv") if f.name != "combined_urls.csv"]

    if not csv_files and not output_file.exists():
        _log(f"⚠️ No URL CSV files found in {urls_dir}")
        return None

    dfs = []

    # Load existing combined file first
    if output_file.exists():
        try:
            existing_df = pd.read_csv(output_file)
            dfs.append(existing_df)
            _log(f"📂 Loaded existing combined file: {len(existing_df):,} URLs")
        except Exception as e:
            _log(f"⚠️ Error reading existing combined file: {e}")

    # Load new CSV files
    _log(f"📂 Found {len(csv_files)} new CSV file(s) to process")
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            dfs.append(df)
            _log(f"   ✅ Loaded {csv_file.name}: {len(df):,} URLs")
        except Exception as e:
            _log(f"   ⚠️ Error reading {csv_file.name}: {e}")

    if not dfs:
        _log("❌ No valid CSV files could be loaded")
        return None

    _log(f"\n🔄 Combining and deduplicating URLs...")
    combined_df = pd.concat(dfs, ignore_index=True)
    initial_count = len(combined_df)

    # Normalize date
    combined_df["fetch_date"] = pd.to_datetime(
        combined_df.get("fetch_date"), errors="coerce"
    )

    # Keep oldest fetch per URL
    combined_df = combined_df.sort_values("fetch_date", ascending=True)
    combined_df = combined_df.drop_duplicates(subset=["url"], keep="first")

    # Sort by newest first for readability
    combined_df = combined_df.sort_values("fetch_date", ascending=False)

    duplicates_removed = initial_count - len(combined_df)
    _log(f"   Removed {duplicates_removed:,} duplicate URLs")

    combined_df.to_csv(output_file, index=False)

    _log(f"\n✅ Updated combined file: {output_file.name}")
    _log(f"📊 Total unique URLs: {len(combined_df):,}")
    _log("=" * 60 + "\n")

    return combined_df
