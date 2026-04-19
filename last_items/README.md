# Last Items Scraper

Scraper for the **Sales-4 (آخر العروض)** category on sheeel.com

## Configuration
- **Type:** Sync (single-level, no subcategories)
- **Category:** last_items
- **Base URL:** https://www.sheeel.com/ar/sales-4.html

## Features
- Automated product discovery
- Price and brand extraction
- Pagination support
- Excel export with automatic formatting
- Sequential single-level scraping
- S3 integration with date partitioning

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

# Run scraper
python scraper.py
```

## Output

- **Local:** `data/last_items_YYYYMMDD_HHMMSS.xlsx`
- **S3:** `sheeel_data/year=YYYY/month=MM/day=DD/last_items/`

## Auto-generated
Created by category discovery system on April 18, 2026
