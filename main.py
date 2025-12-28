#!/usr/bin/env python3

import sys
import pathlib
import os
import pandas as pd
from scrapy.utils.log import configure_logging
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scripts.cleaner import DataCleaner

# Configuration
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent
URL_DIR = PROJECT_ROOT / "outputs" / "urls"
DATA_DIR = PROJECT_ROOT / "outputs" / "data"

for path in [URL_DIR, DATA_DIR]:
    path.mkdir(parents=True, exist_ok=True)

# --- UTILITIES ---


def log(msg, success=True):
    print(f" {'✅' if success else '❌'} {msg}")


def get_input(prompt, valid=None, is_num=False):
    while True:
        val = input(f"\n➤ {prompt}: ").strip().lower()
        if not val and not valid:
            return ""
        if valid and val not in valid:
            print(f"   Invalid choice. Choose: {', '.join(valid)}")
            continue
        if is_num:
            try:
                return int(val)
            except:
                continue
        return val


# --- ROBUST CONSOLIDATION ---


def consolidate_data(directory, pattern, target_name, id_col="url"):
    target_path = directory / target_name
    all_files = [f for f in directory.glob(pattern) if f.name != target_name]

    if not all_files and not target_path.exists():
        return None

    dfs = []
    files_to_delete = []

    if target_path.exists() and target_path.stat().st_size > 0:
        try:
            dfs.append(pd.read_csv(target_path))
        except:
            pass

    for f in all_files:
        if f.stat().st_size > 0:
            try:
                df = pd.read_csv(f)
                if not df.empty:
                    dfs.append(df)
                    files_to_delete.append(f)
            except:
                continue

    if not dfs:
        return None

    combined = pd.concat(dfs, ignore_index=True)

    if "fetch_date" in combined.columns and not combined.empty:
        combined["fetch_date"] = pd.to_datetime(combined["fetch_date"], errors="coerce")
        combined["fetch_date"] = combined["fetch_date"].fillna(pd.Timestamp.now())

        combined = combined.sort_values("fetch_date", ascending=True)
        before = len(combined)
        combined = combined.drop_duplicates(subset=[id_col], keep="first")

        combined["fetch_date"] = combined["fetch_date"].dt.strftime("%Y-%m-%d")

        diff = before - len(combined)
        if diff > 0:
            log(f"Merged {directory.name}. Removed {diff} duplicates.")

    combined.to_csv(target_path, index=False)
    log(f"Updated {target_name} (Total: {len(combined)} rows)")

    for f in files_to_delete:
        try:
            os.remove(f)
        except:
            pass

    if files_to_delete:
        log(f"Cleaned up {len(files_to_delete)} temporary files.")

    return combined


# --- SPIDER EXECUTION ---


def run_spider(spider_cls, **kwargs):
    configure_logging({"LOG_ENABLED": False})
    settings = get_project_settings()
    settings.update({"LOG_LEVEL": "ERROR"})

    process = CrawlerProcess(settings)
    process.crawl(spider_cls, **kwargs)
    process.start()


# --- MODES ---
def mode_clean_data():
    raw_file = DATA_DIR / "listings_combined.csv"
    backup_file = DATA_DIR / "backup.csv"
    clean_file = DATA_DIR / "untouched_raw_original.csv"

    if not raw_file.exists():
        return log("No listings_combined.csv found to clean.", False)

    # 1. LOAD & BACKUP
    df = pd.read_csv(raw_file)
    df.to_csv(backup_file, index=False)
    log(f"Safety backup created: {backup_file.name}")

    # 2. RUN FULL CLEANING PIPE

    try:
        cleaner = DataCleaner(df)
        # Execute EVERY step in order
        df_final = (
            cleaner.extract_sub_location()
            .fill_missing_house_type()
            .clean_bathrooms_bedrooms()
            .extract_properties()
            .extract_amenities()
            .extract_facilities()
            .clean_price()
            .remove_sale_and_short_term()
            .select_columns()
            .get_df()
        )

        # 3. SAVE CLEANED VERSION
        df_final.to_csv(clean_file, index=False)
        log(f"Cleaning Success! {len(df_final)} rows saved to listings_combined.csv")
    except Exception as e:
        log(f"Cleaning process failed: {e}", False)


def mode_url_spider():
    url = (
        input("\nBase URL (Default Jiji): ")
        or "https://jiji.com.gh/greater-accra/houses-apartments-for-rent?page={}"
    )
    total = get_input("Total listings to scrape", is_num=True)

    from scrappers.spiders.urlspider import UrlSpider

    run_spider(UrlSpider, baseUrl=url, startPage=1, totalListings=total)
    consolidate_data(URL_DIR, "listingURLS_*.csv", "combined_urls.csv")


def mode_listing_spider(csv_path=None):
    is_resume = csv_path is not None
    if not csv_path:
        files = sorted(
            URL_DIR.glob("*.csv"), key=lambda x: x.stat().st_mtime, reverse=True
        )
        if not files:
            return log("No URL files found.", False)

        print("\n📂 Available URL sources:")
        for i, f in enumerate(files[:10], 1):
            print(f"  [{i}] {f.name}")
        idx = get_input("Select file index", is_num=True)
        csv_path = files[idx - 1]

    from scrappers.spiders.listingspider import ListingSpider

    run_spider(ListingSpider, csv_path=str(csv_path))
    consolidate_data(DATA_DIR, "listings__*.csv", "listings_combined.csv")

    if is_resume and os.path.exists(csv_path):
        os.remove(csv_path)


def mode_resume():
    url_file = URL_DIR / "combined_urls.csv"
    scraped_file = DATA_DIR / "listings_combined.csv"

    consolidate_data(URL_DIR, "listingURLS_*.csv", "combined_urls.csv")

    if not url_file.exists():
        return log("No combined_urls.csv found.", False)

    df_urls = pd.read_csv(url_file)
    scraped_urls = (
        pd.read_csv(scraped_file)["url"].unique() if scraped_file.exists() else []
    )

    remaining_df = df_urls[~df_urls["url"].isin(scraped_urls)]

    if remaining_df.empty:
        return log("All URLs have already been scraped.")

    log(f"Remaining: {len(remaining_df)} / {len(df_urls)}")
    if get_input("Resume now? (y/n)", valid=["y", "n"]) == "y":
        res_path = URL_DIR / "resume_queue.csv"
        remaining_df.to_csv(res_path, index=False)
        mode_listing_spider(csv_path=res_path)


def show_stats():
    url_file = URL_DIR / "combined_urls.csv"
    data_file = DATA_DIR / "listings_combined.csv"

    urls = len(pd.read_csv(url_file)) if url_file.exists() else 0
    scraped = len(pd.read_csv(data_file)) if data_file.exists() else 0

    print(f"\n{'=' * 30}")
    print("📊 CURRENT DATABASE STATS")
    print(f"{'=' * 30}")
    print(f"🔗 Total URLs:     {urls}")
    print(f"🏠 Scraped Items:  {scraped}")
    print(f"⏳ Pending:        {max(0, urls - scraped)}")
    print(f"{'=' * 30}")


# --- MAIN ---


def main():
    menu = {
        "1": ("🔗 URL Spider", mode_url_spider),
        "2": ("📝 Listing Spider", mode_listing_spider),
        "3": ("🔄 Resume Scraper", mode_resume),
        "4": (
            "🧹 Maintenance (Sync & Clean)",
            lambda: [
                consolidate_data(URL_DIR, "listingURLS_*.csv", "combined_urls.csv"),
                consolidate_data(DATA_DIR, "listings__*.csv", "listings_combined.csv"),
            ],
        ),
        "5": ("📊 Show Stats", show_stats),
        "6": ("🧹 Clean & Backup Data", mode_clean_data),
        "7": ("❌ Exit", sys.exit),
    }

    while True:
        print(f"\n{'=' * 40}\n   🕷️  JIJI PROPERTY SCRAPER\n{'=' * 40}")
        for k, v in menu.items():
            print(f"  {k}. {v[0]}")

        choice = get_input("Select", valid=menu.keys())
        try:
            menu[choice][1]()
        except KeyboardInterrupt:
            print("\nStopped by user.")
        except Exception as e:
            log(f"Error: {e}", False)


if __name__ == "__main__":
    main()
