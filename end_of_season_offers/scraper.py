"""
Async scraper for End-of-Season Offers category on sheeel.com
Handles subcategories with concurrent scraping for performance
"""
import asyncio
from playwright.async_api import async_playwright
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
import os
import requests
from io import BytesIO
import boto3
from botocore.exceptions import ClientError

class EndOfSeasonOffersScraper:
    def __init__(self, s3_bucket=None, aws_access_key=None, aws_secret_key=None):
        self.base_url = "https://www.sheeel.com/ar/end-of-season-offers.html"
        self.category = "end_of_season_offers"
        self.s3_bucket = s3_bucket or os.getenv('S3_BUCKET_NAME')
        self.aws_access_key = aws_access_key or os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = aws_secret_key or os.getenv('AWS_SECRET_ACCESS_KEY')
        
        self.data_dir = Path('data')
        self.images_dir = self.data_dir / 'images'
        self.images_dir.mkdir(parents=True, exist_ok=True)
        
        self.all_products = []
        self.failed_urls = []
        
        # Concurrency control
        self.max_concurrent = int(os.getenv('MAX_CONCURRENT_SUBCATEGORIES', '3'))
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        
        # S3 client
        if self.s3_bucket and self.aws_access_key and self.aws_secret_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
                region_name='us-east-1'
            )
        else:
            self.s3_client = None
    
    async def get_subcategories(self, page):
        """Extract subcategory links from page"""
        print("\n📂 Extracting subcategories...")
        
        subcategories = []
        try:
            # Get all subcategory links
            links = await page.query_selector_all('.subcategory-link')
            print(f"  Found {len(links)} subcategories\n")
            
            for link in links:
                try:
                    href = await link.get_attribute('href')
                    text = await link.inner_text()
                    
                    if href and '/ar/end-of-season-offers/' in href:
                        subcategories.append({
                            'name': text.strip(),
                            'url': href,
                            'slug': href.split('/ar/')[-1].replace('.html', '').strip()
                        })
                except:
                    continue
        
        except Exception as e:
            print(f"  ⚠️  Error extracting subcategories: {e}")
        
        return subcategories
    
    async def has_next_page(self, page):
        """Check if next page button exists"""
        try:
            next_button = await page.query_selector('.pages-item-next a.next')
            return next_button is not None
        except:
            return False
    
    async def scrape_page(self, page, page_num, subcategory_name):
        """Extract product links from list page"""
        try:
            product_links = await page.query_selector_all('[id^="product-item-info_"] > a')
            
            urls = []
            for link in product_links:
                try:
                    href = await link.get_attribute('href')
                    if href and '/ar/' in href:
                        urls.append(href)
                except:
                    continue
            
            return urls
        
        except Exception as e:
            print(f"    ❌ Error extracting links: {e}")
            return []
    
    async def scrape_product_detail(self, page, url, subcategory_name):
        """Extract all product fields from detail page"""
        try:
            await page.goto(url, wait_until='networkidle', timeout=20000)
            
            # Wait for main content
            await page.wait_for_selector('#maincontent .product-info-main', timeout=10000)
            
            product_data = {}
            
            # Product ID
            try:
                product_input = await page.query_selector('input[name="product"]')
                product_data['product_id'] = int(await product_input.get_attribute('value')) if product_input else None
            except:
                product_data['product_id'] = None
            
            # Title
            try:
                title_el = await page.query_selector('.page-title .base')
                product_data['name'] = await title_el.inner_text() if title_el else 'N/A'
                product_data['name'] = self.clean_for_excel(product_data['name']).strip()
            except:
                product_data['name'] = 'N/A'
            
            # SKU
            try:
                sku_el = await page.query_selector('.product-info.sku')
                sku_text = await sku_el.inner_text() if sku_el else ''
                sku_text = self.clean_for_excel(sku_text).strip()
                product_data['sku'] = sku_text.split(':', 1)[1].strip() if ':' in sku_text else sku_text
            except:
                product_data['sku'] = 'N/A'
            
            # Availability
            try:
                avail_el = await page.query_selector('.availability-info')
                product_data['availability'] = await avail_el.inner_text() if avail_el else 'N/A'
                product_data['availability'] = self.clean_for_excel(product_data['availability']).strip()
            except:
                product_data['availability'] = 'N/A'
            
            # Times bought
            try:
                times_el = await page.query_selector('.x-bought-count')
                product_data['times_bought'] = await times_el.inner_text() if times_el else 'N/A'
                product_data['times_bought'] = self.clean_for_excel(product_data['times_bought']).strip()
            except:
                product_data['times_bought'] = 'N/A'
            
            # Old price
            try:
                old_price_el = await page.query_selector('.old-price .price')
                product_data['old_price'] = await old_price_el.inner_text() if old_price_el else 'N/A'
                product_data['old_price'] = self.clean_for_excel(product_data['old_price']).strip()
            except:
                product_data['old_price'] = 'N/A'
            
            # Special price
            try:
                special_price_el = await page.query_selector('.special-price .price')
                product_data['special_price'] = await special_price_el.inner_text() if special_price_el else 'N/A'
                product_data['special_price'] = self.clean_for_excel(product_data['special_price']).strip()
            except:
                product_data['special_price'] = 'N/A'
            
            # Description
            try:
                desc_el = await page.query_selector('.product.attribute.overview .value')
                product_data['description'] = await desc_el.inner_text() if desc_el else 'N/A'
                product_data['description'] = self.clean_for_excel(product_data['description']).strip()
            except:
                product_data['description'] = 'N/A'
            
            # Images
            try:
                image_elements = await page.query_selector_all('.product-gallery-image')
                image_urls = []
                for img_el in image_elements:
                    try:
                        src = await img_el.get_attribute('data-src') or await img_el.get_attribute('src')
                        if src and ('http' in src or src.startswith('/')):
                            if src.startswith('/'):
                                src = 'https://www.sheeel.com' + src
                            image_urls.append(src)
                    except:
                        continue
                product_data['image_urls'] = image_urls
            except:
                product_data['image_urls'] = []
            
            # Deal timer
            try:
                timer_el = await page.query_selector('#deal-timer .time')
                product_data['deal_time_left'] = await timer_el.inner_text() if timer_el else 'N/A'
                product_data['deal_time_left'] = self.clean_for_excel(product_data['deal_time_left']).strip()
            except:
                product_data['deal_time_left'] = 'N/A'
            
            # Discount badge
            try:
                discount_el = await page.query_selector('.discount-percent-item')
                product_data['discount_badge'] = await discount_el.inner_text() if discount_el else 'N/A'
                product_data['discount_badge'] = self.clean_for_excel(product_data['discount_badge']).strip()
            except:
                product_data['discount_badge'] = 'N/A'
            
            # Features & Specifications
            try:
                features_specs = []
                more_info = await page.query_selector('#more-info')
                if more_info:
                    sections = await more_info.query_selector_all('.attribute-info')
                    for section in sections:
                        try:
                            label_el = await section.query_selector('.attribute-info.label')
                            if label_el:
                                section_name = await label_el.inner_text()
                                items = await section.query_selector_all('ul > li')
                                for item in items:
                                    text = await item.inner_text()
                                    text = self.clean_for_excel(text).strip()
                                    if text:
                                        features_specs.append(text)
                        except:
                            continue
                
                product_data['features_specs'] = features_specs
                for i, feature in enumerate(features_specs):
                    product_data[f'feature_spec_{i}'] = feature
            except:
                product_data['features_specs'] = []
            
            # Box contents
            try:
                more_info = await page.query_selector('#more-info')
                if more_info:
                    sections = await more_info.query_selector_all('.attribute-info')
                    for section in sections:
                        try:
                            label_el = await section.query_selector('.attribute-info.label')
                            if label_el:
                                section_name = await label_el.inner_text()
                                if 'محتوى' in section_name or 'العلبة' in section_name:
                                    value_el = await section.query_selector('+ p')
                                    if value_el:
                                        box_text = await value_el.inner_text()
                                        product_data['box_contents'] = self.clean_for_excel(box_text).strip()
                        except:
                            continue
                if 'box_contents' not in product_data:
                    product_data['box_contents'] = 'N/A'
            except:
                product_data['box_contents'] = 'N/A'
            
            # Warranty
            try:
                more_info = await page.query_selector('#more-info')
                if more_info:
                    sections = await more_info.query_selector_all('.attribute-info')
                    for section in sections:
                        try:
                            label_el = await section.query_selector('.attribute-info.label')
                            if label_el:
                                section_name = await label_el.inner_text()
                                if 'الكفالة' in section_name or 'ضمان' in section_name:
                                    value_el = await section.query_selector('+ p')
                                    if value_el:
                                        warranty_text = await value_el.inner_text()
                                        product_data['warranty'] = self.clean_for_excel(warranty_text).strip()
                        except:
                            continue
                if 'warranty' not in product_data:
                    product_data['warranty'] = 'N/A'
            except:
                product_data['warranty'] = 'N/A'
            
            # Subcategory
            product_data['subcategory'] = subcategory_name
            
            # URL and timestamp
            product_data['url'] = url
            product_data['scraped_at'] = datetime.now().isoformat()
            
            return product_data
        
        except Exception as e:
            print(f"    ❌ Error: {str(e)[:100]}")
            self.failed_urls.append(url)
            return None
    
    async def scrape_subcategory(self, browser, subcat):
        """Scrape individual subcategory with pagination"""
        async with self.semaphore:
            try:
                print(f"  📂 Scraping: {subcat['name']}")
                
                context = await browser.new_context()
                page = await context.new_page()
                
                await page.goto(subcat['url'], wait_until='networkidle', timeout=30000)
                
                page_num = 1
                subcat_products = 0
                
                while True:
                    # Extract products from current page
                    urls = await self.scrape_page(page, page_num, subcat['name'])
                    
                    for url in urls:
                        product_data = await self.scrape_product_detail(page, url, subcat['name'])
                        if product_data:
                            self.all_products.append(product_data)
                            subcat_products += 1
                    
                    # Check for next page
                    if await self.has_next_page(page):
                        next_button = await page.query_selector('.pages-item-next a.next')
                        if next_button:
                            await next_button.click()
                            await page.wait_for_load_state('networkidle')
                            page_num += 1
                        else:
                            break
                    else:
                        break
                
                await context.close()
                print(f"    ✓ {subcat_products} products")
                
            except Exception as e:
                print(f"    ❌ Error: {e}")
    
    async def scrape_all_subcategories(self):
        """Scrape all subcategories concurrently"""
        print("\n" + "="*70)
        print("🌐 SCRAPING PRODUCT DETAILS (ASYNC)")
        print("="*70)
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                print(f"\n📡 Loading: {self.base_url}")
                await page.goto(self.base_url, wait_until='networkidle', timeout=30000)
                print("✓ Page loaded\n")
                
                # Get subcategories
                subcategories = await self.get_subcategories(page)
                
                if not subcategories:
                    print("No subcategories found")
                    await browser.close()
                    return
                
                # Close initial page
                await page.close()
                
                # Scrape subcategories concurrently
                print(f"🔄 Scraping {len(subcategories)} subcategories with max {self.max_concurrent} concurrent...\n")
                tasks = [self.scrape_subcategory(browser, subcat) for subcat in subcategories]
                await asyncio.gather(*tasks)
                
                await browser.close()
        
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"\n✓ Successfully extracted {len(self.all_products)} products")
    
    def download_image(self, url, product_id, index, upload_immediately=False):
        """Download single image with optional immediate S3 upload"""
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, timeout=10, headers=headers)
            if response.status_code == 200:
                # Determine extension from content-type
                content_type = response.headers.get('content-type', '').lower()
                if 'jpeg' in content_type or 'jpg' in content_type:
                    ext = '.jpg'
                elif 'png' in content_type:
                    ext = '.png'
                elif 'gif' in content_type:
                    ext = '.gif'
                elif 'webp' in content_type:
                    ext = '.webp'
                else:
                    ext = '.jpg'
                
                filename = f"{product_id}_{index}{ext}"
                filepath = self.images_dir / filename
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                # Upload immediately if requested
                if upload_immediately and self.s3_client:
                    try:
                        now = datetime.now()
                        date_partition = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
                        s3_key = f"sheeel_data/{date_partition}/{self.category}/images/{filename}"
                        self.s3_client.upload_file(str(filepath), self.s3_bucket, s3_key)
                        return f"https://{self.s3_bucket}.s3.amazonaws.com/{s3_key}"
                    except Exception as e:
                        print(f"      Error uploading to S3: {e}")
                        return str(filepath)
                
                return str(filepath)
        except Exception as e:
            print(f"      Error downloading image: {e}")
        
        return None
    
    def download_all_images(self):
        """Download all product images with incremental S3 upload"""
        print("\n📷 DOWNLOADING IMAGES (INCREMENTAL UPLOAD)")
        total_images = sum(len(p.get('image_urls', [])) for p in self.all_products)
        print(f"  Total images: {total_images}")
        
        downloaded = 0
        for i, product in enumerate(self.all_products, 1):
            product_id = product.get('product_id')
            image_urls = product.get('image_urls', [])
            
            s3_paths = []
            for img_idx, img_url in enumerate(image_urls):
                path = self.download_image(img_url, product_id, img_idx, upload_immediately=True)
                if path:
                    s3_paths.append(path)
                    downloaded += 1
            
            product['s3_image_paths'] = s3_paths
            
            if i % 10 == 0:
                print(f"  Downloaded {downloaded}/{total_images} images...")
        
        print(f"✓ Downloaded and uploaded {downloaded}/{total_images} images")
    
    def upload_to_s3(self, local_file, s3_key):
        """Upload file to S3"""
        try:
            self.s3_client.upload_file(local_file, self.s3_bucket, s3_key)
            return True
        except ClientError as e:
            print(f"    S3 Error: {e}")
            return False
    
    def save_to_excel(self):
        """Generate multi-sheet Excel with subcategories"""
        print("\n📊 SAVING TO EXCEL")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        excel_filename = f"end_of_season_offers_{timestamp}.xlsx"
        excel_path = self.data_dir / excel_filename
        
        # Create writer
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # Get unique subcategories
            subcategories = sorted(set(p.get('subcategory', 'Unknown') for p in self.all_products))
            
            all_data = []
            
            # Write one sheet per subcategory
            for subcat in subcategories:
                subcat_products = [p for p in self.all_products if p.get('subcategory') == subcat]
                df = pd.DataFrame(subcat_products)
                
                # Remove local image paths
                if 'local_image_paths' in df.columns:
                    df = df.drop(columns=['local_image_paths'])
                
                # Clean sheet name (Excel limit: 31 chars)
                sheet_name = subcat[:31] if len(subcat) <= 31 else subcat[:28] + '...'
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                all_data.extend(subcat_products)
            
            # Write ALL_PRODUCTS sheet
            if all_data:
                df_all = pd.DataFrame(all_data)
                if 'local_image_paths' in df_all.columns:
                    df_all = df_all.drop(columns=['local_image_paths'])
                df_all.to_excel(writer, sheet_name='ALL_PRODUCTS', index=False)
        
        print(f"✓ Saved to: {excel_path}")
        print(f"  Sheets: {len(subcategories)} categories + ALL_PRODUCTS")
        
        return excel_path
    
    def upload_results_to_s3(self):
        """Upload Excel to S3"""
        print("\n☁️  UPLOADING TO S3")
        
        excel_path = self.save_to_excel()
        
        # Upload Excel
        try:
            now = datetime.now()
            date_partition = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
            excel_filename = excel_path.name
            s3_key = f"sheeel_data/{date_partition}/{self.category}/excel-files/{excel_filename}"
            
            if self.upload_to_s3(str(excel_path), s3_key):
                print(f"✓ Uploaded to s3://{self.s3_bucket}/{s3_key}")
        except Exception as e:
            print(f"❌ Error uploading Excel: {e}")
    
    @staticmethod
    def clean_for_excel(value):
        """Remove illegal Excel characters from Arabic text"""
        if isinstance(value, str):
            return ''.join(char for char in value if ord(char) >= 32 or char in '\t\n\r')
        return value
    
    async def run(self):
        """Main execution flow"""
        print("\n" + "="*70)
        print("🚀 END-OF-SEASON OFFERS SCRAPER (ASYNC)")
        print("="*70)
        print(f"\n⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📍 Category: {self.category}")
        print(f"🌐 URL: {self.base_url}")
        print(f"⚙️  Max concurrent: {self.max_concurrent}")
        
        if not self.s3_bucket:
            print("\n⚠️  Warning: S3 not configured. Images will not be uploaded.")
            print("   Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME environment variables")
        
        # Step 1: Scrape all subcategories concurrently
        await self.scrape_all_subcategories()
        
        if len(self.all_products) == 0:
            print("\n❌ No products scraped")
            return False
        
        # Step 2: Download images
        self.download_all_images()
        
        # Step 3: Upload to S3 (if configured)
        if self.s3_bucket:
            self.upload_results_to_s3()
        else:
            # Just save Excel locally
            print("\n📊 Saving Excel file locally...")
            self.save_to_excel()
        
        print("\n" + "="*70)
        print("✅ SCRAPING COMPLETED")
        print("="*70)
        print(f"⏱️  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        return True

async def main():
    scraper = EndOfSeasonOffersScraper()
    await scraper.run()

if __name__ == '__main__':
    asyncio.run(main())
