# End-of-Season Offers Scraper

Scraper for the **End-of-Season Offers (نهاية الموسم)** category on sheeel.com

## Configuration
- **Type:** Async (with subcategories)
- **Category:** end_of_season_offers
- **Base URL:** https://www.sheeel.com/ar/end-of-season-offers.html

## Features
- Automated product discovery
- Price and brand extraction
- Pagination support
- Excel export with automatic formatting
- **Concurrent subcategory scraping (3 parallel)**
- S3 integration with date partitioning
- Multi-sheet Excel output (one sheet per subcategory + ALL_PRODUCTS)

## Performance
- ~3x faster with async concurrent scraping + incremental S3 upload
- Concurrent limit: 3 subcategories (configurable via MAX_CONCURRENT_SUBCATEGORIES)
- Incremental image upload: images uploaded to S3 during download

## Setup

```bash
pip install -r requirements.txt
playwright install chromium
```

## Usage

```bash
# Set environment variables
export AWS_ACCESS_KEY_ID="your_key"
export AWS_SECRET_ACCESS_KEY="your_secret"
export S3_BUCKET_NAME="your_bucket"
export MAX_CONCURRENT_SUBCATEGORIES="3"  # Optional, default is 3

# Run scraper
python scraper.py
```

## Output

- **Local:** `data/end_of_season_offers_YYYYMMDD_HHMMSS.xlsx`
  - Multiple sheets (one per subcategory + ALL_PRODUCTS combined)
- **S3:** `sheeel_data/year=YYYY/month=MM/day=DD/end_of_season_offers/`
  - `excel-files/` - Excel file
  - `images/` - Product images

## Auto-generated
Created by category discovery system on April 18, 2026
