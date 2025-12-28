import pandas as pd
import ast
import re


class DataCleaner:
    def __init__(self, df, verbose=True, keep_original_columns=False):
        self.df = df.copy()
        self.verbose = verbose
        self.keep_original_columns = keep_original_columns

    def _log(self, msg):
        if self.verbose:
            print(f" {msg}")

    @staticmethod
    def _get_sub_location(loc):
        """Extract sub-location from location string."""
        parts = str(loc).split(",")
        if len(parts) >= 3:
            return parts[1].strip()
        return parts[0].strip()

    def extract_sub_location(self):
        self._log("📍 Extracting locality from location...")
        self.df["locality"] = self.df["location"].apply(self._get_sub_location)
        unique_localities = self.df["locality"].nunique()
        self._log(f"✅ Extracted {unique_localities} unique localities")
        if not self.keep_original_columns:
            self.df = self.df.drop("location", axis=1)
            self._log("🗑️ Dropped original 'location' column")
        return self

    def fill_missing_house_type(self):
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

    def clean_bathrooms_bedrooms(self):
        self._log("🚿🛏️ Cleaning bathrooms and bedrooms data...")
        initial_rows = len(self.df)
        self.df.dropna(subset=["bathrooms", "bedrooms"], inplace=True)
        rows_dropped = initial_rows - len(self.df)
        if rows_dropped > 0:
            self._log(
                f"🗑️ Dropped {rows_dropped} rows with missing bathroom/bedroom data"
            )

        # Extract numeric values (e.g., '2 Bathrooms' -> '2')
        self.df["bathrooms"] = (
            self.df["bathrooms"].str.split(" ").str.get(0).str.strip()
        )
        self.df["bedrooms"] = self.df["bedrooms"].str.split(" ").str.get(0).str.strip()
        self._log("✅ Cleaned bathrooms and bedrooms successfully")
        return self

    def extract_properties(self):
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

    def extract_amenities(self):
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

    def extract_facilities(self):
        if "Facilities" not in self.df.columns:
            self._log("⚠️ No 'Facilities' column found, skipping facility extraction")
            return self

        self._log("🏢 Extracting and merging facilities...")

        # 1. Generate dummies from the Facilities column
        facilities_encoded = self.df["Facilities"].str.strip().str.get_dummies(sep=",")
        facilities_encoded.columns = facilities_encoded.columns.str.strip()

        # 2. Logical Merge: Handle duplicates manually to avoid Reindex errors
        for col in facilities_encoded.columns:
            if col in self.df.columns:
                # If column exists, combine them (1 if either is 1)
                # We use .max(axis=1) or logical OR to handle the merge
                self.df[col] = (
                    self.df[[col]]
                    .join(facilities_encoded[[col]], rsuffix="_new")
                    .max(axis=1)
                )
            else:
                # If brand new, just add it
                self.df[col] = facilities_encoded[col]

        if not self.keep_original_columns:
            self.df = self.df.drop("Facilities", axis=1)
            self._log("🗑️ Dropped original 'Facilities' column")

        self._log("✅ Facilities merged successfully")
        return self

    def clean_price(self):
        self._log("💰 Cleaning price data...")
        self.df["price"] = (
            self.df["price"]
            .str.replace("GH₵ ", "", regex=False)
            .str.replace(",", "", regex=False)
            .astype(float)
        )
        return self

    def remove_sale_and_short_term(self):
        self._log("🚫 Filtering out sale and short-term rental listings...")
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

        combined_patterns = sale_patterns + period_patterns
        regex = "|".join([re.escape(p) for p in combined_patterns])

        text = (
            self.df["title"].fillna("") + " " + self.df["description"].fillna("")
        ).str.lower()

        initial_rows = len(self.df)
        self.df = self.df[~text.str.contains(regex, regex=True, na=False)]
        self._log(f"🗑️ Removed {initial_rows - len(self.df)} non-rental listings")
        return self

    def select_columns(self):
        self._log("📋 Selecting final relevant columns...")
        selected_cols = [
            "url",
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
        available_cols = [col for col in selected_cols if col in self.df.columns]
        self.df = self.df[available_cols].copy()
        self._log(
            f"✅ Final dataset: {len(available_cols)} columns, {len(self.df)} rows"
        )
        return self

    def get_df(self):
        return self.df
