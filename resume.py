import csv
import os
from pathlib import Path

# Update these paths to match your files
PROJECT_ROOT = Path(__file__).resolve().parents[0]
ORIGINAL_URLS_CSV = (
    PROJECT_ROOT / "outputs" / "urls" / "listingURLS_20251027_060512.csv"
)
SCRAPED_DATA_CSV = (
    PROJECT_ROOT / "outputs" / "data" / "listings__20251027_061613.csv"
)  # Update with your actual filename
OUTPUT_CSV = PROJECT_ROOT / "outputs" / "urls" / "remaining_urls.csv"


def get_scraped_urls(scraped_csv):
    """Extract all URLs that have been successfully scraped"""
    scraped_urls = set()

    if not os.path.exists(scraped_csv):
        print(f"❌ Scraped data file not found: {scraped_csv}")
        return scraped_urls

    try:
        with open(scraped_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "url" in row and row["url"]:
                    scraped_urls.add(row["url"].strip())

        print(f"✅ Found {len(scraped_urls)} scraped URLs")
    except Exception as e:
        print(f"❌ Error reading scraped data: {e}")

    return scraped_urls


def get_remaining_urls(original_csv, scraped_urls):
    """Get URLs that haven't been scraped yet"""
    remaining_urls = []
    total_urls = 0

    if not os.path.exists(original_csv):
        print(f"❌ Original URLs file not found: {original_csv}")
        return remaining_urls, total_urls

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

        print(f"✅ Total URLs: {total_urls}")
        print(f"✅ Remaining URLs: {len(remaining_urls)}")
        print(
            f"✅ Completion: {len(scraped_urls)}/{total_urls} ({len(scraped_urls) / total_urls * 100:.1f}%)"
        )
    except Exception as e:
        print(f"❌ Error reading original URLs: {e}")

    return remaining_urls, total_urls


def save_remaining_urls(remaining_urls, output_csv):
    """Save remaining URLs to a new CSV file"""
    try:
        # Create directory if it doesn't exist
        output_csv.parent.mkdir(parents=True, exist_ok=True)

        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["url", "page"])  # Write header
            writer.writerows(remaining_urls)

        print(f"✅ Saved {len(remaining_urls)} remaining URLs to: {output_csv}")
        return True
    except Exception as e:
        print(f"❌ Error saving remaining URLs: {e}")
        return False


def main():
    print("=" * 60)
    print("RESUME SCRAPER - Finding Remaining URLs")
    print("=" * 60)
    print()

    # Check if files exist
    print("📁 Checking files...")
    print(f"   Original URLs: {ORIGINAL_URLS_CSV}")
    print(f"   Scraped Data: {SCRAPED_DATA_CSV}")
    print()

    # Get scraped URLs
    print("🔍 Step 1: Reading scraped data...")
    scraped_urls = get_scraped_urls(SCRAPED_DATA_CSV)
    print()

    if not scraped_urls:
        print("⚠️  No scraped data found. Using original URLs file.")
        print()

    # Get remaining URLs
    print("🔍 Step 2: Finding remaining URLs...")
    remaining_urls, total_urls = get_remaining_urls(ORIGINAL_URLS_CSV, scraped_urls)
    print()

    if not remaining_urls:
        print("🎉 All URLs have been scraped!")
        return

    # Save remaining URLs
    print("💾 Step 3: Saving remaining URLs...")
    if save_remaining_urls(remaining_urls, OUTPUT_CSV):
        print()
        print("=" * 60)
        print("✅ SUCCESS!")
        print("=" * 60)
        print()
        print(f"📊 Summary:")
        print(f"   Total URLs: {total_urls}")
        print(
            f"   Scraped: {len(scraped_urls)} ({len(scraped_urls) / total_urls * 100:.1f}%)"
        )
        print(
            f"   Remaining: {len(remaining_urls)} ({len(remaining_urls) / total_urls * 100:.1f}%)"
        )
        print()
        print(f"📝 Next step: Run the spider with the remaining URLs:")
        print(
            f'   scrapy crawl listingspider -a csv_path="{OUTPUT_CSV.relative_to(PROJECT_ROOT)}"'
        )
        print()
    else:
        print("❌ Failed to save remaining URLs")


if __name__ == "__main__":
    main()
