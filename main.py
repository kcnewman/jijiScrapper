import sys
import os
import pathlib
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def print_header():
    """Print welcome header"""
    print("\n" + "=" * 60)
    print("🕷️  PROPERTY LISTING SCRAPER")
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


def find_latest_csv():
    """Find the most recent CSV file in outputs/urls/"""
    PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
    urls_dir = PROJECT_ROOT / "outputs" / "urls"

    if not urls_dir.exists():
        return None

    csv_files = list(urls_dir.glob("listingURLS_*.csv"))
    if not csv_files:
        return None

    # Sort by modification time, most recent first
    latest = max(csv_files, key=lambda p: p.stat().st_mtime)
    return str(latest)


def list_available_csvs():
    """List all available CSV files"""
    PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
    urls_dir = PROJECT_ROOT / "outputs" / "urls"

    if not urls_dir.exists():
        return []

    csv_files = sorted(
        urls_dir.glob("listingURLS_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return [str(f) for f in csv_files]


def run_urlspider(max_page):
    """Run the URL spider to collect listing URLs."""
    print(f"\n{'=' * 60}")
    print(f"🚀 Starting URL Spider")
    print(f"📄 Max pages to scrape: {max_page}")
    print(f"{'=' * 60}\n")

    from scrappers.spiders.urlspider import urlspiderSpider

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
    process.crawl(urlspiderSpider, maxPage=max_page)
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

    from scrappers.spiders.listingspider import ListingspiderSpider

    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(ListingspiderSpider, csv_path=csv_path)
    process.start()

    print(f"\n{'=' * 60}")
    print("✅ Listing Spider completed!")
    print(f"{'=' * 60}\n")


def interactive_url_spider():
    """Interactive flow for URL spider"""
    print("\n📋 URL SPIDER CONFIGURATION\n")
    print("This spider will collect property listing URLs from Jiji.com.gh")
    print_separator()

    max_page = get_number(
        "\nHow many pages would you like to scrape? (1-100): ", min_val=1, max_val=100
    )

    print(f"\n✓ Will scrape {max_page} page(s)")
    confirm = get_choice("\nProceed? (y/n): ", ["y", "n", "Y", "N"])

    if confirm.lower() == "y":
        run_urlspider(max_page)
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
        return

    print(f"\nFound {len(csv_files)} CSV file(s):\n")

    # Show available files
    for i, csv_file in enumerate(csv_files[:10], 1):  # Show max 10
        filename = os.path.basename(csv_file)
        print(f"  {i}. {filename}")

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

            # Ask if user wants to run another spider
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
