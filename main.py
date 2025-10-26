#!/usr/bin/env python3

import sys
import os
import pathlib
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def print_header():
    """Print welcome header"""
    print("\n" + "=" * 60)
    print("🕷️ LISTING SCRAPER")
    print("=" * 60 + "\n")


def print_separator():
    """Print separator line"""
    print("-" * 60)


def get_choice(prompt, valid_choices):
    """
    Get valid user input from a list of choices.

    Args:
        prompt (str): Prompt to display
        valid_choices (list): List of valid choices

    Returns:
        str: User's choice
    """
    while True:
        choice = input(prompt).strip()
        if choice in valid_choices:
            return choice
        print(f"❌ Invalid choice. Please enter one of: {', '.join(valid_choices)}\n")


def get_number(prompt, min_val=1, max_val=None):
    """
    Get a valid number from user.

    Args:
        prompt (str): Prompt to display
        min_val (int): Minimum valid value
        max_val (int): Maximum valid value (None for no limit)

    Returns:
        int: User's number
    """
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
    """
    Get a valid URL from user.

    Args:
        prompt (str): Prompt to display
        default (str): Default URL if user presses enter

    Returns:
        str: User's URL
    """
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


def list_available_csvs():
    """List all available CSV files"""
    PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[0]
    urls_dir = PROJECT_ROOT / "outputs" / "urls"

    if not urls_dir.exists():
        return []

    csv_files = sorted(
        urls_dir.glob("listingURLS_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return [str(f) for f in csv_files]


def run_urlspider(base_url, start_page, max_page):
    """Run the URL spider to collect listing URLs."""
    print(f"\n{'=' * 60}")
    print(f"🚀 Starting URL Spider")
    print(f"🌐 Base URL: {base_url}")
    print(f"📄 Pages: {start_page} to {max_page}")
    print(f"{'=' * 60}\n")

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

    print(f"\n{'=' * 60}")
    print("✅ URL Spider completed!")
    print(f"{'=' * 60}\n")


def run_listingspider(csv_path):
    """Run the listing spider to extract detailed information."""
    print(f"\n{'=' * 60}")
    print(f"🚀 Starting Listing Spider")
    print(f"📂 CSV file: {csv_path}")
    print(f"{'=' * 60}\n")

    from scrappers.spiders.listingspider import ListingSpider

    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(ListingSpider, csv_path=csv_path)
    process.start()

    print(f"\n{'=' * 60}")
    print("✅ Listing Spider completed!")
    print(f"{'=' * 60}\n")


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

    print(f"\n📊 Summary:")
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

    # Check for available CSVs
    csv_files = list_available_csvs()

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
            filename = os.path.basename(csv_file)
            file_size = os.path.getsize(csv_file) / 1024  # KB
            print(f"  {i}. {filename} ({file_size:.1f} KB)")

        if len(csv_files) > 10:
            print(f"  ... and {len(csv_files) - 10} more")

        print(f"\n  0. Enter custom path")
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
            csv_path = csv_files[choice - 1]

    try:
        with open(csv_path, "r") as f:
            line_count = sum(1 for _ in f) - 1  # -1 for header
        print(f"\n📊 This CSV contains {line_count} URL(s)")
    except:
        pass

    print(f"\n✓ Selected: {os.path.basename(csv_path)}")
    confirm = get_choice("\nProceed? (y/n): ", ["y", "n", "Y", "N"])

    if confirm.lower() == "y":
        run_listingspider(csv_path)
    else:
        print("\n❌ Cancelled\n")


def main():
    """Main interactive loop"""
    try:
        while True:
            print_header()

            print("Which spider would you like to run?\n")
            print("  1. 🔗 URL Spider      - Collect listing URLs from search pages")
            print("  2. 📝 Listing Spider  - Extract details from collected URLs")
            print("  3. ❌ Exit\n")
            print_separator()

            choice = get_choice("\nEnter your choice (1-3): ", ["1", "2", "3"])

            if choice == "1":
                interactive_url_spider()
            elif choice == "2":
                interactive_listing_spider()
            elif choice == "3":
                print("\n👋 Goodbye!\n")
                sys.exit(0)

            print_separator()
            another = get_choice("\nRun another spider? (y/n): ", ["y", "n", "Y", "N"])
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
