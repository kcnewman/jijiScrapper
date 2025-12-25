#!/usr/bin/env python3

import sys
import os
import pathlib
import csv
import pandas as pd
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scripts.clean import DataCleaner


def print_header():
    print("\n" + "=" * 50)
    print("🕷️ LISTING SCRAPER")
    print("=" * 50 + "\n")


def print_separator():
    print("-" * 50)


def get_choice(prompt, valid_choices):
    while True:
        choice = input(prompt).strip()
        if choice in valid_choices:
            return choice
        print(f"❌ Invalid choice. Please enter one of: {', '.join(valid_choices)}\n")


def get_number(prompt, min_val=1, max_val=None):
    while True:
        try:
            value = int(input(prompt).strip())
            if value < min_val:
                print(f"❌ Please enter a number >= {min_val}\n")
                continue
            if max_val and value > max_val:
                print(f"❌ Please enter a number <= {max_val}\n")
                continue
            return value
        except ValueError:
            print("❌ Please enter a valid number\n")


def get_url(prompt, default=None):
    while True:
        if default:
            url = input(f"{prompt} (default: {default})\nURL: ").strip()
            if not url:
                return default
        else:
            url = input(prompt).strip()

        if url.startswith("http://") or url.startswith("https://"):
            return url
        else:
            print("❌ URL must start with http:// or https://\n")


def list_csv_files(directory, pattern="*.csv"):
    """List all CSV files in a directory"""
    if not directory.exists():
        return []

    files = sorted(
        directory.glob(pattern),
        key=lambda x: x.stat().st_mtime,
        reverse=True,  # Most recent first
    )
    return files


def display_files(files, file_type):
    if not files:
        print(f"   ⚠️  No {file_type} files found")
        return False

    print(f"\n📁 Available {file_type} files:")
    # print("-" * 70)

    for idx, file in enumerate(files, 1):
        size = file.stat().st_size / 1024  # KB
        modified = datetime.fromtimestamp(file.stat().st_mtime)

        # Count rows
        try:
            with open(file, "r", encoding="utf-8") as f:
                row_count = sum(1 for _ in f) - 1  # Subtract header
        except Exception as e:
            print(e)
            row_count = "?"

        print(f"   [{idx}] {file.name}")
        print(
            f"       Size: {size:.1f} KB | Rows: {row_count:,} | Modified: {modified.strftime('%Y-%m-%d %H:%M:%S')}"
        )

    # print("-" * 80)
    return True


def select_file(files, file_type):
    while True:
        try:
            choice = input(
                f"\n➤ Select {file_type} file (1-{len(files)}) or 'q' to quit: "
            ).strip()

            if choice.lower() == "q":
                return None

            idx = int(choice)
            if 1 <= idx <= len(files):
                selected = files[idx - 1]
                print(f"   ✅ Selected: {selected.name}")
                return selected
            else:
                print(f"   ❌ Please enter a number between 1 and {len(files)}")
        except ValueError:
            print("   ❌ Please enter a valid number or 'q' to quit")
        except KeyboardInterrupt:
            print("\n\n⚠️  Cancelled by user")
            return None


def get_scraped_urls(scraped_csv):
    """Extract all URLs that have been successfully scraped"""
    scraped_urls = set()

    try:
        with open(scraped_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "url" in row and row["url"]:
                    scraped_urls.add(row["url"].strip())

        print(f"   ✅ Found {len(scraped_urls):,} scraped URLs")
    except Exception as e:
        print(f"   ❌ Error reading scraped data: {e}")

    return scraped_urls


def get_remaining_urls(original_csv, scraped_urls):
    """Get URLs that haven't been scraped yet"""
    remaining_urls = []
    total_urls = 0

    try:
        with open(original_csv, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # Skip header

            for row in reader:
                if row and row[0].strip():
                    total_urls += 1
                    url = row[0].strip()

                    if url not in scraped_urls:
                        remaining_urls.append(row)  # Keep original row (url, page)

        print(f"   ✅ Total URLs: {total_urls:,}")
        print(f"   ✅ Remaining URLs: {len(remaining_urls):,}")
        print(
            f"   ✅ Completion: {len(scraped_urls):,}/{total_urls:,} ({len(scraped_urls) / total_urls * 100:.1f}%)"
        )
    except Exception as e:
        print(f"   ❌ Error reading original URLs: {e}")

    return remaining_urls, total_urls


def save_remaining_urls(remaining_urls, urls_dir):
    """Save remaining URLs to a new CSV file"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_csv = urls_dir / f"remaining_urls_{timestamp}.csv"

    try:
        output_csv.parent.mkdir(parents=True, exist_ok=True)

        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["url", "page"])  # Write header
            writer.writerows(remaining_urls)

        print(f"   ✅ Saved {len(remaining_urls):,} remaining URLs")
        print(f"   📄 File: {output_csv.name}")
        return output_csv
    except Exception as e:
        print(f"   ❌ Error saving remaining URLs: {e}")
        return None


def concatenate_and_clean_data(data_dir, keep_original_columns=False):
    """
    Concatenate all scraped CSV files and clean the combined data.
    """
    print("\n" + "=" * 50)
    print("🧹 CLEANING DATA")
    print("=" * 50)

    data_files = list_csv_files(data_dir, "listings__*.csv")

    if not data_files:
        print("\n❌ No data files found to clean")
        return None

    print(f"\n📊 Found {len(data_files)} scraped file(s)")

    print("\n📥 Reading and concatenating files...")
    dfs = []
    total_rows = 0

    for file in data_files:
        try:
            df = pd.read_csv(file)
            rows = len(df)
            dfs.append(df)
            total_rows += rows
            print(f"   ✅ {file.name}: {rows:,} rows")
        except Exception as e:
            print(f"   ❌ Error reading {file.name}: {e}")

    if not dfs:
        print("\n❌ No data could be read")
        return None

    print(f"\n🔗 Concatenating {len(dfs)} file(s)...")
    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"   ✅ Total rows before removing duplicates: {len(combined_df):,}")

    # Remove duplicates based on URL
    if "url" in combined_df.columns:
        before_dedup = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=["url"], keep="first")
        after_dedup = len(combined_df)
        duplicates_removed = before_dedup - after_dedup

        if duplicates_removed > 0:
            print(f"   ✅ Removed {duplicates_removed:,} duplicate(s)")
        print(f"   ✅ Total unique rows: {after_dedup:,}")

    # Save concatenated file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    concatenated_path = data_dir / f"listings_combined_{timestamp}.csv"
    combined_df.to_csv(concatenated_path, index=False)
    print(f"   ✅ Saved file: {concatenated_path.name}")

    # Clean the data
    print("\n🧼 Cleaning data)...")
    try:
        cleaner = DataCleaner(keep_original_columns=keep_original_columns)
        cleaner.df = combined_df

        cleaner.extract_sub_location()
        cleaner.fill_missing_house_type()
        cleaner.clean_bathrooms_bedrooms()
        cleaner.extract_properties()
        cleaner.extract_amenities()
        cleaned_df = cleaner.get_dataframe()

        # Save cleaned file
        cleaned_path = data_dir / f"listings_cleaned_{timestamp}.csv"
        cleaned_df.to_csv(cleaned_path, index=False)

        print("\n✅ Cleaning completed!")
        print(f"   📄 Output file: {cleaned_path.name}")
        print(f"   📊 Final rows: {len(cleaned_df):,}")
        print(f"   📋 Final columns: {len(cleaned_df.columns)}")

        return cleaned_path

    except Exception as e:
        print(f"\n❌ Error during cleaning: {e}")
        import traceback

        traceback.print_exc()
        return None


def clean_single_file(file_path, keep_original_columns=False):
    """
    Clean a single scraped CSV file.
    """
    print("\n" + "=" * 50)
    print("🧹 CLEANING DATA")
    print("=" * 50)

    print(f"\n📥 Reading file: {file_path.name}")

    try:
        # Clean the data
        print("\n🧼 Cleaning data)...")
        cleaner = DataCleaner(keep_original_columns=keep_original_columns)
        cleaner.load_data(str(file_path))

        initial_rows = len(cleaner.df)
        print(f"   ✅ Loaded {initial_rows:,} rows")

        cleaner.extract_sub_location()
        cleaner.fill_missing_house_type()
        cleaner.clean_bathrooms_bedrooms()
        cleaner.extract_properties()
        cleaner.extract_amenities()
        cleaned_df = cleaner.get_dataframe()

        # Save cleaned file (overwrite original or create new)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        cleaned_path = file_path.parent / f"listings_cleaned_{timestamp}.csv"
        cleaned_df.to_csv(cleaned_path, index=False)

        print("\n✅ Cleaning completed!")
        print(f"   📄 Output file: {cleaned_path.name}")
        print(f"   📊 Final rows: {len(cleaned_df):,}")
        print(f"   📋 Final columns: {len(cleaned_df.columns)}")

        return cleaned_path

    except Exception as e:
        print(f"\n❌ Error during cleaning: {e}")
        import traceback

        traceback.print_exc()
        return None


def run_urlspider(base_url, start_page, max_page):
    """Run the URL spider to collect listing URLs."""
    print(f"\n{'=' * 60}")
    print("🚀 Starting URL Spider")
    print(f"🌐 Base URL: {base_url}")
    print(f"📄 Pages: {start_page} to {max_page}")
    print(f"{'=' * 50}\n")

    from scrappers.spiders.urlspider import UrlSpider

    settings = get_project_settings()
    settings.update(
        {
            "DOWNLOAD_HANDLERS": {
                "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
                "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            },
            "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
            "PLAYWRIGHT_BROWSER_TYPE": "chromium",
            "PLAYWRIGHT_LAUNCH_OPTIONS": {
                "headless": True,
            },
            "LOG_LEVEL": "INFO",
        }
    )

    process = CrawlerProcess(settings)
    process.crawl(UrlSpider, baseUrl=base_url, startPage=start_page, maxPage=max_page)
    process.start()

    print(f"\n{'=' * 50}")
    print("✅ URL Spider completed!")
    print(f"{'=' * 50}\n")


def run_listingspider(csv_path, auto_clean=True, keep_original_columns=False):
    """
    Run the listing spider to extract detailed information.

    Args:
        csv_path: Path to the CSV file with URLs
        auto_clean: If True, automatically clean data after scraping
        keep_original_columns: If False, drops original columns after transformation
    """
    print(f"\n{'=' * 50}")
    print("🚀 Starting Listing Spider")
    print(f"📂 CSV file: {csv_path}")
    print(f"{'=' * 50}\n")

    from scrappers.spiders.listingspider import ListingSpider

    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(ListingSpider, csv_path=csv_path)
    process.start()

    print(f"\n{'=' * 60}")
    print("✅ Listing Spider completed!")
    print(f"{'=' * 60}\n")

    # Auto-clean if enabled
    if auto_clean:
        PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[0]
        data_dir = PROJECT_ROOT / "outputs" / "data"

        # Find the most recent scraped file
        data_files = list_csv_files(data_dir, "listings__*.csv")
        if data_files:
            latest_file = data_files[0]  # Already sorted by most recent
            clean_single_file(latest_file, keep_original_columns=keep_original_columns)


def interactive_url_spider():
    """Interactive flow for URL spider"""
    print("\n📋 URL SPIDER CONFIGURATION\n")
    print("This spider will collect property listing URLs from a website")
    print_separator()

    print("\nURL Configuration:")
    print("  1. Use default (Jiji.com.gh Greater Accra rentals)")
    print("  2. Enter custom URL")

    url_choice = get_choice("\nChoice (1-2): ", ["1", "2"])

    if url_choice == "1":
        base_url = (
            "https://jiji.com.gh/greater-accra/houses-apartments-for-rent?page={}"
        )
        print(f"\n✓ Using: {base_url}")
    else:
        print("\n💡 Your URL should contain {{}} where the page number goes")
        print("   Example: https://example.com/listings?page={}")
        base_url = get_url("\nEnter base URL: ")

        if "{}" not in base_url:
            print("\n⚠️  Warning: URL doesn't contain {{}} for page numbers")
            add_placeholder = get_choice(
                "Add ?page={{}} to the end? (y/n): ", ["y", "n", "Y", "N"]
            )
            if add_placeholder.lower() == "y":
                base_url += "?page={}"

    print_separator()

    start_page = get_number("\nStarting page number (default 1): ", min_val=1)

    max_page = get_number(
        f"\nEnding page number (must be >= {start_page}): ", min_val=start_page
    )

    total_pages = max_page - start_page + 1

    print("\n📊 Summary:")
    print(f"   • Base URL: {base_url}")
    print(f"   • Start page: {start_page}")
    print(f"   • End page: {max_page}")
    print(f"   • Total pages: {total_pages}")

    confirm = get_choice("\nProceed? (y/n): ", ["y", "n", "Y", "N"])

    if confirm.lower() == "y":
        run_urlspider(base_url, start_page, max_page)
    else:
        print("\n❌ Cancelled\n")


def interactive_listing_spider():
    """Interactive flow for listing spider"""
    print("\n📋 LISTING SPIDER CONFIGURATION\n")
    print("This spider will extract detailed information from listing URLs")
    print_separator()

    PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[0]
    urls_dir = PROJECT_ROOT / "outputs" / "urls"

    # Check for available CSVs
    csv_files = list_csv_files(urls_dir, "*.csv")

    if not csv_files:
        print("\n❌ No CSV files found in outputs/urls/")
        print("Please run the URL spider first to collect URLs.\n")

        manual = get_choice("Enter a CSV path manually? (y/n): ", ["y", "n", "Y", "N"])
        if manual.lower() == "y":
            csv_path = input("\nEnter CSV file path: ").strip()
            if not os.path.exists(csv_path):
                print(f"\n❌ File not found: {csv_path}\n")
                return
        else:
            return
    else:
        print(f"\nFound {len(csv_files)} CSV file(s):\n")

        for i, csv_file in enumerate(csv_files[:10], 1):  # Show max 10
            filename = csv_file.name
            file_size = csv_file.stat().st_size / 1024  # KB
            modified = datetime.fromtimestamp(csv_file.stat().st_mtime)
            print(
                f"  {i}. {filename} ({file_size:.1f} KB) - {modified.strftime('%Y-%m-%d %H:%M')}"
            )

        if len(csv_files) > 10:
            print(f"  ... and {len(csv_files) - 10} more")

        print("\n  0. Enter custom path")
        print_separator()

        choice = get_number(
            f"\nSelect CSV file (0-{min(len(csv_files), 10)}): ",
            min_val=0,
            max_val=min(len(csv_files), 10),
        )

        if choice == 0:
            csv_path = input("\nEnter CSV file path: ").strip()
            if not os.path.exists(csv_path):
                print(f"\n❌ File not found: {csv_path}\n")
                return
        else:
            csv_path = str(csv_files[choice - 1])

    try:
        with open(csv_path, "r") as f:
            line_count = sum(1 for _ in f) - 1  # -1 for header
        print(f"\n📊 This CSV contains {line_count:,} URL(s)")
    except Exception as e:
        print(e)
        pass

    print(f"\n✓ Selected: {os.path.basename(csv_path)}")

    # preferences
    print_separator()
    print("\n🧹 Cleaning Options:")
    auto_clean = get_choice("Clean data after scraping? (y/n): ", ["y", "n", "Y", "N"])

    keep_original = False
    if auto_clean.lower() == "y":
        keep_original = get_choice(
            "Keep original columns? (y/n): ", ["y", "n", "Y", "N"]
        )
        keep_original = keep_original.lower() == "y"

    confirm = get_choice("\nProceed? (y/n): ", ["y", "n", "Y", "N"])

    if confirm.lower() == "y":
        run_listingspider(
            csv_path,
            auto_clean=(auto_clean.lower() == "y"),
            keep_original_columns=keep_original,
        )
    else:
        print("\n❌ Cancelled\n")


def interactive_resume_scraper():
    """Interactive flow for resuming scraping"""
    print("\n📋 RESUME SCRAPER\n")
    print("Find remaining URLs and continue scraping from where you left off")
    print_separator()

    PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[0]
    urls_dir = PROJECT_ROOT / "outputs" / "urls"
    data_dir = PROJECT_ROOT / "outputs" / "data"

    # Step 1: Select original URLs file
    print("\n📌 STEP 1: Select the original URLs file")
    urls_files = list_csv_files(urls_dir, "listingURLS_*.csv")

    if not display_files(urls_files, "URL"):
        print("\n❌ No URL files found. Please run the URL spider first.")
        return

    original_csv = select_file(urls_files, "URL")
    if not original_csv:
        return

    # Step 2: Select scraped data file
    print("\n📌 STEP 2: Select the scraped data file")
    data_files = list_csv_files(data_dir, "listings__*.csv")

    if not display_files(data_files, "scraped data"):
        print("\n⚠️  No scraped data files found.")
        choice = (
            input("\n➤ Continue anyway? This will create a file with all URLs. (y/n): ")
            .strip()
            .lower()
        )
        if choice != "y":
            return
        scraped_csv = None
    else:
        scraped_csv = select_file(data_files, "scraped data")
        if not scraped_csv:
            return

    # Step 3: Process files
    print("\n" + "=" * 50)
    print("\n🔍 Processing files...".center(80))
    print("=" * 50)

    # Get scraped URLs
    print("\n📊 Reading scraped data...")
    scraped_urls = get_scraped_urls(scraped_csv) if scraped_csv else set()

    if not scraped_urls and scraped_csv:
        print("\n⚠️  Warning: No scraped URLs found in the selected file.")

    # Get remaining URLs
    print("\n📊 Finding remaining URLs...")
    remaining_urls, total_urls = get_remaining_urls(original_csv, scraped_urls)

    if not remaining_urls:
        print("\n" + "=" * 50)
        print("🎉 ALL URLS HAVE BEEN SCRAPED!")
        print("=" * 50)

        # Offer to clean and concatenate all data
        clean_all = get_choice(
            "\n🧹 Clean and concatenate all scraped data? (y/n): ", ["y", "n", "Y", "N"]
        )
        if clean_all.lower() == "y":
            keep_original = get_choice(
                "Keep original columns? (y/n): ", ["y", "n", "Y", "N"]
            )
            concatenate_and_clean_data(
                data_dir, keep_original_columns=(keep_original.lower() == "y")
            )

        return

    # Save remaining URLs
    print("\n📊 Saving remaining URLs...")
    output_csv = save_remaining_urls(remaining_urls, urls_dir)

    if output_csv:
        # Final summary
        print("\n✅ SUCCESS!".center(80))
        print("=" * 50)
        print("\n\n📊 Summary:")
        print(f"   • Total URLs: {total_urls:,}")
        print(
            f"   • Already scraped: {len(scraped_urls):,} ({len(scraped_urls) / total_urls * 100:.1f}%)"
        )
        print(
            f"   • Remaining: {len(remaining_urls):,} ({len(remaining_urls) / total_urls * 100:.1f}%)"
        )

        # Ask if user wants to start scraping now
        print("\n" + "=" * 50)
        start_now = get_choice(
            "\n🚀 Start scraping the remaining URLs now? (y/n): ", ["y", "n", "Y", "N"]
        )

        if start_now.lower() == "y":
            # Ask about cleaning
            auto_clean = get_choice(
                "Auto-clean after scraping? (y/n): ", ["y", "n", "Y", "N"]
            )
            keep_original = False
            if auto_clean.lower() == "y":
                keep_original = get_choice(
                    "Keep original columns? (y/n): ", ["y", "n", "Y", "N"]
                )
                keep_original = keep_original.lower() == "y"

            run_listingspider(
                str(output_csv),
                auto_clean=(auto_clean.lower() == "y"),
                keep_original_columns=keep_original,
            )

            # After resuming, offer to concatenate and clean all
            concat_all = get_choice(
                "\n🔗 Concatenate and clean all scraped data? (y/n): ",
                ["y", "n", "Y", "N"],
            )
            if concat_all.lower() == "y":
                concatenate_and_clean_data(
                    data_dir, keep_original_columns=keep_original
                )
        else:
            print("\n📝 You can resume later with this command:")
            print(
                f'\n   scrapy crawl listingspider -a csv_path="{output_csv.relative_to(PROJECT_ROOT)}"'
            )
            print()
    else:
        print("\n❌ Failed to save remaining URLs")


def main():
    """Main interactive loop"""
    try:
        while True:
            print_header()

            print("Which spider would you like to run?\n")
            print("  1. 🔗 URL Spider      - Collect listing URLs from search pages")
            print("  2. 📝 Listing Spider  - Extract details from collected URLs")
            print("  3. 🔄 Resume Scraper  - Continue from where you left off")
            print("  4. ❌ Exit\n")
            print_separator()

            choice = get_choice("\nEnter your choice (1-4): ", ["1", "2", "3", "4"])

            if choice == "1":
                interactive_url_spider()
            elif choice == "2":
                interactive_listing_spider()
            elif choice == "3":
                interactive_resume_scraper()
            elif choice == "4":
                print("\n👋 Goodbye!\n")
                sys.exit(0)

            print_separator()
            another = get_choice("\nRun another task? (y/n): ", ["y", "n", "Y", "N"])
            if another.lower() != "y":
                print("\n👋 Goodbye!\n")
                break

    except KeyboardInterrupt:
        print("\n\n👋 Interrupted by user. Goodbye!\n")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
