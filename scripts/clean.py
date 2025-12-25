import pandas as pd
import ast
import pathlib
from datetime import datetime


class DataCleaner:
    """Data cleaning pipeline for real estate listings."""

    def __init__(self, keep_original_columns: bool = True):
        self.keep_original_columns = keep_original_columns

    def load_data(self, data_path: str) -> "DataCleaner":
        self.df = pd.read_csv(data_path)
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
        if "fetch_date" not in self.df.columns:
            self.df["fetch_date"] = datetime.now()
        else:
            # Convert to datetime if it's a string
            self.df["fetch_date"] = pd.to_datetime(
                self.df["fetch_date"], errors="coerce"
            )
            # Fill any NaT values with current date
            self.df["fetch_date"].fillna(datetime.now(), inplace=True)

        return self

    def extract_sub_location(self) -> "DataCleaner":
        """Extract sub-location from location column."""
        self.df["sub_loc"] = self.df["location"].apply(self._get_sub_location)

        if not self.keep_original_columns:
            self.df = self.df.drop("location", axis=1)

        return self

    def fill_missing_house_type(self) -> "DataCleaner":
        """Fill missing house_type values with 'Bedsitter'."""
        self.df.loc[self.df["house_type"].isna(), "house_type"] = "Bedsitter"
        return self

    def clean_bathrooms_bedrooms(self) -> "DataCleaner":
        """Extract numeric values from bathrooms and bedrooms columns."""
        # Drop rows with missing bathrooms
        self.df = self.df.dropna(subset=["bathrooms"])

        # Extract numeric values
        self.df["bathrooms_"] = (
            self.df["bathrooms"].str.split(" ").str.get(0).str.strip()
        )
        self.df["bedrooms_"] = self.df["bedrooms"].str.split(" ").str.get(0).str.strip()

        if not self.keep_original_columns:
            self.df = self.df.drop(["bathrooms", "bedrooms"], axis=1)
            self.df = self.df.rename(
                columns={"bathrooms_": "bathrooms", "bedrooms_": "bedrooms"}
            )

        return self

    def extract_properties(self) -> "DataCleaner":
        """Expand the properties column into separate columns."""

        self.df["properties"] = self.df["properties"].apply(ast.literal_eval)
        properties = self.df["properties"].apply(pd.Series)
        self.df = self.df.join(properties)

        if not self.keep_original_columns:
            self.df = self.df.drop("properties", axis=1)

        return self

    def extract_amenities(self) -> "DataCleaner":
        """Expand amenities column into binary columns for each amenity."""

        amenities_encoded = self.df["amenities"].str.strip().str.get_dummies(sep=",")
        amenities_encoded.columns = amenities_encoded.columns.str.strip()

        self.df = self.df.join(amenities_encoded)

        if not self.keep_original_columns:
            self.df = self.df.drop("amenities", axis=1)

        return self

    def clean_all(self) -> pd.DataFrame:
        """Execute full cleaning pipeline."""
        self.append_scraping_date()
        self.extract_sub_location()
        self.fill_missing_house_type()
        self.clean_bathrooms_bedrooms()
        self.extract_properties()
        self.extract_amenities()

        return self.df

    def get_dataframe(self) -> pd.DataFrame:
        """Get the current DataFrame."""
        return self.df


def clean_data(data_path: str, keep_original_columns: bool = True) -> pd.DataFrame:
    """Cleaninf function to clean data in one line."""
    cleaner = DataCleaner(keep_original_columns=keep_original_columns)
    return cleaner.load_data(data_path).clean_all()


def combine_urls(urls_directory: str | None = None) -> pd.DataFrame | None:
    """Combine all URL CSV files in the directory into a single combined_urls.csv file."""
    if urls_directory is None:
        urls_dir = pathlib.Path(__file__).resolve().parents[1] / "outputs" / "urls"
    else:
        urls_dir = pathlib.Path(urls_directory)

    if not urls_dir.exists():
        print(f"❌ URLs directory not found: {urls_dir}")
        return None

    csv_files = [f for f in urls_dir.glob("*.csv") if f.name != "combined_urls.csv"]

    if not csv_files:
        print(f"⚠️  No URL CSV files found in {urls_dir}")
        return None

    dfs = []
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            dfs.append(df)
        except Exception as e:
            print(f"⚠️  Error reading {csv_file.name}: {e}")

    if not dfs:
        print("❌ No valid CSV files could be loaded")
        return None

    combined_df = pd.concat(dfs, ignore_index=True)

    # Remove duplicates, keeping the oldest fetch_date for each URL
    combined_df["fetch_date"] = pd.to_datetime(
        combined_df["fetch_date"], errors="coerce"
    )
    combined_df = combined_df.sort_values("fetch_date", ascending=True)
    combined_df = combined_df.drop_duplicates(subset=["url"], keep="first")
    combined_df = combined_df.sort_values("fetch_date", ascending=False)

    output_file = urls_dir / "combined_urls.csv"
    combined_df.to_csv(output_file, index=False)

    print(f"\n✅ Combined {len(csv_files)} URL files into: {output_file.name}")
    print(f"   📊 Total unique URLs: {len(combined_df):,}")

    return combined_df
