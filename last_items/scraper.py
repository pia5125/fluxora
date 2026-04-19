"""
Sync scraper for Sales-4 (آخر العروض) category on sheeel.com
Scrapes product information and downloads images
"""
from playwright.sync_api import sync_playwright
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
import os
import requests
from io import BytesIO
import boto3
from botocore.exceptions import ClientError

class LastItemsScraper:
    def __init__(self, s3_bucket=None, aws_access_key=None, aws_secret_key=None):
        self.base_url = "https://www.sheeel.com/ar/sales-4.html"
        self.category = "last_items"
        self.s3_bucket = s3_bucket or os.getenv('S3_BUCKET_NAME')
        self.aws_access_key = aws_access_key or os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = aws_secret_key or os.getenv('AWS_SECRET_ACCESS_KEY')
        
        self.data_dir = Path('data')
        self.images_dir = self.data_dir / 'images'
        self.images_dir.mkdir(parents=True, exist_ok=True)
        
        self.all_products = []
        self.failed_urls = []
        
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
    
    def has_next_page(self, page):
        """Check if next page button exists"""
        try:
            next_button = page.query_selector('.pages-item-next a.next')
            return next_button is not None
        except:
            return False
    
    def get_current_page_number(self, page):
        """Get current page number"""
        try:
            current = page.query_selector('.pages-items .item.current .page span:last-child')
            if current:
                return int(current.inner_text().strip())
        except:
            pass
        return 1
    
    def scrape_page(self, page, page_num):
        """Extract product links from list page"""
        print(f"\n📄 Page {page_num}:")
        
        try:
            # Get all product links
            product_links = page.query_selector_all('[id^="product-item-info_"] > a')
            print(f"  Found {len(product_links)} products")
            
            urls = []
            for link in product_links:
                try:
                    href = link.get_attribute('href')
                    if href and '/ar/' in href:
                        urls.append(href)
                except:
                    continue
            
            return urls
        
        except Exception as e:
            print(f"  ❌ Error extracting links: {e}")
            return []
    
    def scrape_product_detail(self, context, url):
        """Extract all product fields from detail page"""
        try:
            page = context.new_page()
            page.goto(url, wait_until='networkidle', timeout=20000)
            
            # Wait for main content
            page.wait_for_selector('#maincontent .product-info-main', timeout=10000)
            
            product_data = {}
            
            # Product ID
            try:
                product_input = page.query_selector('input[name="product"]')
                product_data['product_id'] = int(product_input.get_attribute('value')) if product_input else None
            except:
                product_data['product_id'] = None
            
            # Title
            try:
                title_el = page.query_selector('.page-title .base')
                product_data['name'] = title_el.inner_text().strip() if title_el else 'N/A'
            except:
                product_data['name'] = 'N/A'
            
            # SKU
            try:
                sku_el = page.query_selector('.product-info.sku')
                sku_text = sku_el.inner_text().strip() if sku_el else ''
                product_data['sku'] = sku_text.split(':', 1)[1].strip() if ':' in sku_text else sku_text
            except:
                product_data['sku'] = 'N/A'
            
            # Availability
            try:
                avail_el = page.query_selector('.availability-info')
                product_data['availability'] = avail_el.inner_text().strip() if avail_el else 'N/A'
            except:
                product_data['availability'] = 'N/A'
            
            # Times bought
            try:
                times_el = page.query_selector('.x-bought-count')
                product_data['times_bought'] = times_el.inner_text().strip() if times_el else 'N/A'
            except:
                product_data['times_bought'] = 'N/A'
            
            # Old price
            try:
                old_price_el = page.query_selector('.old-price .price')
                product_data['old_price'] = old_price_el.inner_text().strip() if old_price_el else 'N/A'
            except:
                product_data['old_price'] = 'N/A'
            
            # Special price
            try:
                special_price_el = page.query_selector('.special-price .price')
                product_data['special_price'] = special_price_el.inner_text().strip() if special_price_el else 'N/A'
            except:
                product_data['special_price'] = 'N/A'
            
            # Description
            try:
                desc_el = page.query_selector('.product.attribute.overview .value')
                product_data['description'] = desc_el.inner_text().strip() if desc_el else 'N/A'
            except:
                product_data['description'] = 'N/A'
            
            # Images
            try:
                image_elements = page.query_selector_all('.product-gallery-image')
                image_urls = []
                for img_el in image_elements:
                    try:
                        src = img_el.get_attribute('data-src') or img_el.get_attribute('src')
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
                timer_el = page.query_selector('#deal-timer .time')
                product_data['deal_time_left'] = timer_el.inner_text().strip() if timer_el else 'N/A'
            except:
                product_data['deal_time_left'] = 'N/A'
            
            # Discount badge
            try:
                discount_el = page.query_selector('.discount-percent-item')
                product_data['discount_badge'] = discount_el.inner_text().strip() if discount_el else 'N/A'
            except:
                product_data['discount_badge'] = 'N/A'
            
            # Features & Specifications (from #more-info tab)
            try:
                features_specs = []
                more_info = page.query_selector('#more-info')
                if more_info:
                    sections = more_info.query_selector_all('.attribute-info')
                    for section in sections:
                        try:
                            label_el = section.query_selector('.attribute-info.label')
                            if label_el:
                                section_name = label_el.inner_text().strip()
                                # Get list items
                                items = section.query_selector_all('ul > li')
                                for item in items:
                                    text = item.inner_text().strip()
                                    if text:
                                        features_specs.append(text)
                        except:
                            continue
                
                product_data['features_specs'] = features_specs
                # Flatten features
                for i, feature in enumerate(features_specs):
                    product_data[f'feature_spec_{i}'] = self.clean_for_excel(feature)
            except:
                product_data['features_specs'] = []
            
            # Box contents
            try:
                more_info = page.query_selector('#more-info')
                if more_info:
                    sections = more_info.query_selector_all('.attribute-info')
                    for section in sections:
                        try:
                            label_el = section.query_selector('.attribute-info.label')
                            if label_el:
                                section_name = label_el.inner_text().strip()
                                if 'محتوى' in section_name or 'العلبة' in section_name:
                                    value_el = section.query_selector('+ p')
                                    if value_el:
                                        product_data['box_contents'] = self.clean_for_excel(value_el.inner_text().strip())
                        except:
                            continue
                if 'box_contents' not in product_data:
                    product_data['box_contents'] = 'N/A'
            except:
                product_data['box_contents'] = 'N/A'
            
            # Warranty
            try:
                more_info = page.query_selector('#more-info')
                if more_info:
                    sections = more_info.query_selector_all('.attribute-info')
                    for section in sections:
                        try:
                            label_el = section.query_selector('.attribute-info.label')
                            if label_el:
                                section_name = label_el.inner_text().strip()
                                if 'الكفالة' in section_name or 'ضمان' in section_name:
                                    value_el = section.query_selector('+ p')
                                    if value_el:
                                        product_data['warranty'] = self.clean_for_excel(value_el.inner_text().strip())
                        except:
                            continue
                if 'warranty' not in product_data:
                    product_data['warranty'] = 'N/A'
            except:
                product_data['warranty'] = 'N/A'
            
            # URL and timestamp
            product_data['url'] = url
            product_data['scraped_at'] = datetime.now().isoformat()
            
            page.close()
            return product_data
        
        except Exception as e:
            print(f"    ❌ Error: {str(e)[:100]}")
            self.failed_urls.append(url)
            return None
    
    def download_image(self, url, product_id, index):
        """Download single image"""
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
                return str(filepath)
        except Exception as e:
            print(f"      Error downloading image: {e}")
        
        return None
    
    def download_all_images(self):
        """Download all product images"""
        print("\n📷 DOWNLOADING IMAGES")
        total_images = sum(len(p.get('image_urls', [])) for p in self.all_products)
        print(f"  Total images to download: {total_images}")
        
        downloaded = 0
        for i, product in enumerate(self.all_products, 1):
            product_id = product.get('product_id')
            image_urls = product.get('image_urls', [])
            
            local_paths = []
            for img_idx, img_url in enumerate(image_urls):
                local_path = self.download_image(img_url, product_id, img_idx)
                if local_path:
                    local_paths.append(local_path)
                    downloaded += 1
            
            product['local_image_paths'] = local_paths
            
            if i % 5 == 0:
                print(f"  Downloaded {downloaded}/{total_images} images...")
        
        print(f"✓ Downloaded {downloaded}/{total_images} images")
    
    def upload_to_s3(self, local_file, s3_key):
        """Upload file to S3"""
        try:
            self.s3_client.upload_file(local_file, self.s3_bucket, s3_key)
            return True
        except ClientError as e:
            print(f"    S3 Error: {e}")
            return False
    
    def upload_results_to_s3(self):
        """Upload images and Excel to S3"""
        print("\n☁️  UPLOADING TO S3")
        
        # Get date for partitioning
        now = datetime.now()
        date_partition = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
        
        # Upload images
        print("📷 Uploading images...")
        uploaded_images = 0
        
        for product in self.all_products:
            product_id = product.get('product_id')
            local_paths = product.get('local_image_paths', [])
            s3_paths = []
            
            for idx, local_path in enumerate(local_paths):
                try:
                    filename = Path(local_path).name
                    s3_key = f"sheeel_data/{date_partition}/{self.category}/images/{filename}"
                    if self.upload_to_s3(local_path, s3_key):
                        s3_url = f"https://{self.s3_bucket}.s3.amazonaws.com/{s3_key}"
                        s3_paths.append(s3_url)
                        uploaded_images += 1
                except Exception as e:
                    print(f"    Error uploading image: {e}")
            
            product['s3_image_paths'] = s3_paths
        
        print(f"✓ Uploaded {uploaded_images} images to S3")
        
        # Create Excel file with S3 paths (remove local paths)
        print("📊 Creating Excel file with S3 paths...")
        df = pd.DataFrame(self.all_products)
        
        # Remove local image paths column
        if 'local_image_paths' in df.columns:
            df = df.drop(columns=['local_image_paths'])
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        excel_filename = f"last_items_{timestamp}.xlsx"
        excel_path = self.data_dir / excel_filename
        
        df.to_excel(excel_path, index=False, engine='openpyxl')
        print(f"✓ Saved to: {excel_path}")
        print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
        
        # Upload Excel to S3
        try:
            s3_key = f"sheeel_data/{date_partition}/{self.category}/excel-files/{excel_filename}"
            if self.upload_to_s3(str(excel_path), s3_key):
                print(f"✓ Uploaded to s3://{self.s3_bucket}/{s3_key}")
        except Exception as e:
            print(f"❌ Error uploading Excel: {e}")
    
    @staticmethod
    def clean_for_excel(value):
        """Remove illegal Excel characters from Arabic text"""
        if isinstance(value, str):
            # Remove control characters (except tab, newline, carriage return)
            return ''.join(char for char in value if ord(char) >= 32 or char in '\t\n\r')
        return value
    
    def scrape_all_pages(self):
        """Main scraping loop with pagination"""
        print("\n" + "="*70)
        print("🌐 SCRAPING PRODUCT DETAILS")
        print("="*70)
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context()
                page = context.new_page()
                
                print(f"\n📡 Loading: {self.base_url}")
                page.goto(self.base_url, wait_until='networkidle', timeout=30000)
                print("✓ Page loaded")
                
                page_num = 1
                total_products = 0
                
                while True:
                    print(f"\n{'='*70}")
                    print(f"📄 PAGE {page_num}")
                    print(f"{'='*70}")
                    
                    # Get product links from current page
                    urls = self.scrape_page(page, page_num)
                    
                    if not urls:
                        print("No products found on this page")
                        break
                    
                    # Scrape each product
                    print(f"\n  Scraping {len(urls)} products...")
                    for i, url in enumerate(urls, 1):
                        product_data = self.scrape_product_detail(context, url)
                        if product_data:
                            self.all_products.append(product_data)
                            total_products += 1
                            
                            if i % 5 == 0:
                                print(f"    ✓ Progress: {i}/{len(urls)} products ({(i/len(urls)*100):.1f}%)")
                    
                    # Check for next page
                    if self.has_next_page(page):
                        next_button = page.query_selector('.pages-item-next a.next')
                        if next_button:
                            print(f"\n  → Moving to next page...")
                            next_button.click()
                            page.wait_for_load_state('networkidle')
                            page_num += 1
                        else:
                            break
                    else:
                        print("\n  ✓ Last page reached")
                        break
                
                context.close()
                browser.close()
        
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"\n✓ Successfully extracted {total_products} products")
        return total_products
    
    def run(self):
        """Main execution flow"""
        print("\n" + "="*70)
        print("🚀 LAST ITEMS SCRAPER")
        print("="*70)
        print(f"\n⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📍 Category: {self.category}")
        print(f"🌐 URL: {self.base_url}")
        
        if not self.s3_bucket:
            print("\n⚠️  Warning: S3 not configured. Images will not be uploaded.")
            print("   Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME environment variables")
        
        # Step 1: Scrape all pages
        total = self.scrape_all_pages()
        
        if total == 0:
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
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            excel_filename = f"last_items_{timestamp}.xlsx"
            excel_path = self.data_dir / excel_filename
            
            df = pd.DataFrame(self.all_products)
            if 'local_image_paths' in df.columns:
                df = df.drop(columns=['local_image_paths'])
            
            df.to_excel(excel_path, index=False, engine='openpyxl')
            print(f"✓ Saved to: {excel_path}")
        
        print("\n" + "="*70)
        print("✅ SCRAPING COMPLETED")
        print("="*70)
        print(f"⏱️  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        return True

if __name__ == '__main__':
    scraper = LastItemsScraper()
    scraper.run()
