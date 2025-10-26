import scrapy
import csv
import json
from scrapy_playwright.page import PageMethod
from listingscraper.items import ListingscraperItem


class ListingspiderSpider(scrapy.Spider):
    name = "listingspider"

    custom_settings = {
        "PLAYWRIGHT_ABORT_REQUEST": lambda req: req.resource_type
        in ["image", "media", "font"],
        "DOWNLOAD_TIMEOUT": 120,
        "RETRY_TIMES": 5,
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": True,
        },
        "CONCURRENT_REQUESTS": 2,
        "DOWNLOAD_DELAY": 1,
    }

    def __init__(self, urlsFile=None, *args, **kwargs):
        super(ListingspiderSpider, self).__init__(*args, **kwargs)
        if not urlsFile:
            raise ValueError(
                "URL File is required to specify the CSV with listing URLs."
            )
        self.urlsFile = urlsFile
        self.logger.info(
            f"Listings spider initialized. Will read URLs from: {self.urlsFile}"
        )

    def start_requests(self):
        try:
            with open(self.urlsFile, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    listingURL = row.get("url")
                    if listingURL and listingURL.strip():
                        yield scrapy.Request(
                            url=listingURL.strip(),
                            meta={
                                "playwright": True,
                                "playwright_page_methods": [
                                    PageMethod(
                                        "wait_for_selector", "h1", timeout=60000
                                    ),
                                    PageMethod("wait_for_load_state", "networkidle"),
                                ],
                                "playwright_include_page": True,
                            },
                            callback=self.parse,
                            errback=self.errback_close_page,
                            dont_filter=True,
                        )
                    else:
                        self.logger.warning(f"Skipping row {row}. No link found.")
        except FileNotFoundError:
            self.logger.error(f"URLs file not found: {self.urlsFile}")
        except Exception as e:
            self.logger.error(f"Error reading URLs file {self.urlsFile}: {e}")

    async def parse(self, response):
        page = response.meta.get("playwright_page")
        if not page:
            self.logger.error(f"No playwright page found for {response.url}")
            return

        item = ListingscraperItem()
        item["url"] = response.url

        try:
            # Title
            try:
                title_element = await page.wait_for_selector("h1 div", timeout=5000)
                item["title"] = (
                    (await title_element.text_content()).strip()
                    if title_element
                    else None
                )
            except Exception as e:
                self.logger.warning(f"Could not extract title from {response.url}: {e}")
                item["title"] = None

            # Full location
            try:
                location_element = await page.query_selector(
                    "div.b-advert-info-statistics--region"
                )
                item["location"] = (
                    (await location_element.text_content()).strip()
                    if location_element
                    else None
                )
            except Exception as e:
                self.logger.warning(
                    f"Could not extract location from {response.url}: {e}"
                )
                item["location"] = None

            item["house_type"] = None
            item["num_bathrooms"] = None
            item["num_bedrooms"] = None

            # House Type, No. of Bathrooms, No. of Bedrooms
            try:
                attribute_elements = await page.query_selector_all(
                    "div.b-advert-icon-attribute"
                )
                for attr_el in attribute_elements:
                    name_el = await attr_el.query_selector(
                        "div.b-advert-icon-attribute__name"
                    )
                    value_el = await attr_el.query_selector(
                        "div.b-advert-icon-attribute__value"
                    )
                    if name_el and value_el:
                        name_text = (await name_el.text_content()).strip().lower()
                        value_text = (await value_el.text_content()).strip()
                        if "house type" in name_text or "type" in name_text:
                            item["house_type"] = value_text
                        elif "bathroom" in name_text:
                            item["num_bathrooms"] = value_text
                        elif "bedroom" in name_text:
                            item["num_bedrooms"] = value_text
            except Exception as e:
                self.logger.warning(
                    f"Could not extract house attributes from {response.url}: {e}"
                )

            # Listing properties
            all_properties = {}

            async def extract_properties_from_section(section_selector):
                """Helper function to extract properties from a section"""
                try:
                    section_element = await page.query_selector(section_selector)
                    if section_element:
                        rows = await section_element.query_selector_all(
                            "div.b-advert-item-details__row"
                        )
                        for row in rows:
                            name_el = await row.query_selector(
                                "div.b-advert-item-details__name"
                            )
                            value_el = await row.query_selector(
                                "div.b-advert-item-details__value"
                            )
                            if name_el and value_el:
                                name = (await name_el.text_content()).strip()
                                value = (await value_el.text_content()).strip()
                                if name and value:  # Only add if both exist
                                    all_properties[name] = value
                except Exception as e:
                    self.logger.debug(
                        f"Could not extract from section {section_selector}: {e}"
                    )

            await extract_properties_from_section(
                "div.b-advert-item-details-collapser__visible"
            )
            await extract_properties_from_section(
                "div.b-advert-item-details-collapser__rest"
            )

            item["properties"] = (
                json.dumps(all_properties) if all_properties else json.dumps({})
            )

            # All amenities
            amenities = []
            try:
                amenity_elements = await page.query_selector_all(
                    "div.b-advert-icon-attribute__name"
                )
                for el in amenity_elements:
                    text = await el.text_content()
                    if text and text.strip():
                        amenity_text = text.strip()
                        if amenity_text.lower() not in [
                            "house type",
                            "bathrooms",
                            "bedrooms",
                        ]:
                            amenities.append(amenity_text)

                amenities = list(dict.fromkeys(amenities))
            except Exception as e:
                self.logger.warning(
                    f"Could not extract amenities from {response.url}: {e}"
                )

            item["amenities"] = json.dumps(amenities)

            # Listing price
            try:
                price_selectors = [
                    "div.b-alt-advert-price-wrapper div.qa-advert-price",
                    "div.b-alt-advert-price-wrapper > div > div",
                    "div.qa-advert-price",
                ]

                for selector in price_selectors:
                    price_element = await page.query_selector(selector)
                    if price_element:
                        price_text = await price_element.text_content()
                        if price_text and price_text.strip():
                            item["price"] = price_text.strip()
                            break
                else:
                    item["price"] = None
            except Exception as e:
                self.logger.warning(f"Could not extract price from {response.url}: {e}")
                item["price"] = None

            self.logger.info(f"Successfully scraped: {response.url}")
            yield item

        except Exception as e:
            self.logger.error(f"Error parsing {response.url}: {e}", exc_info=True)
        finally:
            if page:
                try:
                    await page.close()
                except Exception as e:
                    self.logger.warning(f"Error closing page: {e}")

    async def errback_close_page(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            try:
                await page.close()
            except Exception as e:
                self.logger.warning(f"Error closing page in errback: {e}")
        self.logger.error(f"Request failed for {failure.request.url}: {failure.value}")
