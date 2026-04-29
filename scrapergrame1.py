import csv
import time
import json
import os
from playwright.sync_api import sync_playwright
from datetime import datetime
from collections import OrderedDict
from multiprocessing import Pool, Manager
import re

# ============================================================
# CONFIGURATION - Edit these paths/settings as needed
# ============================================================
CONFIG = {
    'MAX_WORKERS': 5,               # Number of concurrent browser instances
    'SCROLL_DELAY': 2,
    'MAX_SCROLLS': 15,
    'PAGE_LOAD_DELAY': 5,
    'DELAY_BETWEEN_PINCODES': 2,
    'RESULTS_FILE': r"D:\GSTCSV\hospital_results_playwright.json",
    'CHECKPOINT_FILE': 'hospital_scraping_checkpoint.json',
    'CSV_FILE': r'E:/5c2f62fe-5afa-4119-a499-fec9d604d5bd.csv',
    'HEADLESS': False,              # Set to True for headless mode (no visible browser)
}
# ============================================================


class HospitalScraper:
    def __init__(self, instance_id=1):
        self.instance_id = instance_id
        self.checkpoint_file = CONFIG['CHECKPOINT_FILE']
        self.results_file = CONFIG['RESULTS_FILE']
        self.completed_pincodes = set()
        self.load_checkpoint()

    def load_checkpoint(self):
        """Load previously scraped pincodes from results file and checkpoint"""
        if os.path.exists(self.results_file):
            try:
                with open(self.results_file, 'r', encoding='utf-8') as f:
                    existing_results = json.load(f)
                    self.completed_pincodes = set(existing_results.keys())
                print(f"📚 Instance {self.instance_id}: Found {len(self.completed_pincodes)} already scraped pincodes")
            except Exception:
                print(f"📚 Instance {self.instance_id}: No existing results found")

        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    self.completed_pincodes.update(checkpoint.get('completed_pincodes', []))
                print(f"📚 Instance {self.instance_id}: Loaded checkpoint with {len(self.completed_pincodes)} total completed pincodes")
            except Exception:
                pass

    def save_checkpoint(self, all_results):
        """Save current progress to checkpoint file"""
        checkpoint = {
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'completed_pincodes': list(self.completed_pincodes),
            'total_hospitals': sum(data.get('count', 0) for data in all_results.values())
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint, f, indent=2)

    def save_results(self, all_results):
        """Save results to JSON file, merging with existing data"""
        os.makedirs(os.path.dirname(self.results_file), exist_ok=True)

        if os.path.exists(self.results_file):
            try:
                with open(self.results_file, 'r', encoding='utf-8') as f:
                    existing_results = json.load(f)
                existing_results.update(all_results)
                all_results = existing_results
            except Exception:
                pass

        with open(self.results_file, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)

        print(f"💾 Instance {self.instance_id}: Results saved → {self.results_file}")
        return self.results_file


def setup_browser(instance_id):
    """Setup Playwright browser instance"""
    playwright = sync_playwright().start()
    
    # Launch browser with options
    browser = playwright.chromium.launch(
        headless=CONFIG['HEADLESS'],
        args=[
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-blink-features=AutomationControlled',
        ]
    )
    
    # Create context with realistic viewport
    context = browser.new_context(
        viewport={'width': 1920, 'height': 1080},
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    )
    
    page = context.new_page()
    
    # Add script to hide automation
    page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
    """)
    
    return playwright, browser, context, page


def load_unique_pincodes(csv_file):
    """Load unique pincodes from CSV, preserving order"""
    pincodes = OrderedDict()
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if 'pincode' in row and row['pincode']:
                pincode = row['pincode'].strip()
                if pincode not in pincodes:
                    pincodes[pincode] = {
                        'district': row.get('district', ''),
                        'state': row.get('statename', ''),
                        'office': row.get('officename', '')
                    }
    return pincodes


def scrape_pincode(pincode, pincode_info, instance_id):
    """
    Scrape all hospitals for a single pincode from Google Maps.
    Uses Playwright with exact selectors.
    """
    playwright = None
    browser = None
    context = None
    page = None
    
    try:
        playwright, browser, context, page = setup_browser(instance_id)
        
        url = f"https://www.google.co.in/maps/search/hospital+in+{pincode}/"
        print(f"  🔍 Instance {instance_id}: Loading {url}")
        page.goto(url, wait_until='networkidle')
        time.sleep(CONFIG['PAGE_LOAD_DELAY'])
        
        # EXACT SELECTORS for Google Maps elements
        hospitals = []
        seen_names = set()
        last_count = 0
        scroll_attempts = 0
        max_scrolls = CONFIG['MAX_SCROLLS']
        
        while scroll_attempts < max_scrolls:
            # Wait for results panel to load
            try:
                page.wait_for_selector('[role="feed"], [role="main"]', timeout=5000)
            except:
                pass
            
            # SELECTOR 1: Hospital result cards
            # Google Maps uses various selectors for result cards
            cards = page.query_selector_all('[role="article"], .Nv2PK, .bfdHYd, .m6QErb, .DkEaL')
            
            for card in cards:
                try:
                    # Get all text from the card
                    text = card.inner_text()
                    if not text or len(text) <= 10:
                        continue
                    
                    lines = text.split('\n')
                    name = lines[0] if lines else None
                    
                    if not name or len(name) <= 2:
                        continue
                    if any(x in name for x in ['Collapse', 'Results', 'Press', 'Google', 'Maps']):
                        continue
                    if name in seen_names:
                        continue
                    
                    seen_names.add(name)
                    
                    # SELECTOR 2: Phone number extraction using regex
                    phone = None
                    phone_match = re.search(r'(\+91|0)?[\s-]?[6-9]\d{9}|\d{5}[\s-]?\d{5}', text)
                    if phone_match:
                        phone = phone_match.group()
                    
                    # SELECTOR 3: Rating extraction
                    rating = None
                    reviews = None
                    # Pattern: "4.5 (123)" or "4.5(123)"
                    rating_match = re.search(r'(\d+\.?\d*)\s*\((\d+(?:,\d+)?)\)', text)
                    if not rating_match:
                        # Alternative pattern: "4.5 ★ (123)"
                        rating_match = re.search(r'(\d+\.?\d*)\s*[★☆]\s*\((\d+(?:,\d+)?)\)', text)
                    if rating_match:
                        rating = rating_match.group(1)
                        reviews = rating_match.group(2).replace(',', '')
                    
                    # SELECTOR 4: Address extraction (lines containing '·' or address markers)
                    address = None
                    for line in lines:
                        # Look for address patterns
                        if ('·' in line or 
                            'Address' in line or 
                            re.search(r'\d+\s+\w+', line) or  # numbers followed by words
                            (len(line) > 20 and ',' in line)):  # longer lines with commas
                            parts = line.split('·')
                            if len(parts) > 1:
                                address = parts[-1].strip()
                            else:
                                address = line.strip()
                            break
                    
                    # If no address found, try alternative selectors
                    if not address:
                        try:
                            # SELECTOR 5: Specific address element
                            address_elem = card.query_selector('.W4Efsd, .I5k9Mc, .fontBodyMedium, .sXLa0e')
                            if address_elem:
                                address = address_elem.inner_text()
                        except:
                            pass
                    
                    hospital_entry = {
                        'name': name,
                        'rating': rating,
                        'reviews': reviews,
                        'phone': phone,
                        'address': address,
                        'pincode': pincode,
                        'district': pincode_info['district'],
                        'state': pincode_info['state']
                    }
                    hospitals.append(hospital_entry)
                    
                except Exception as e:
                    continue
            
            current_count = len(hospitals)
            if current_count > last_count:
                print(f"  📊 Instance {instance_id} - {pincode}: Found {current_count} hospitals...")
            
            # SELECTOR 6: Scroll the results panel
            scroll_selectors = ['[role="feed"]', '[role="main"]', '.m6QErb', '.DkEaL']
            scrolled = False
            
            for selector in scroll_selectors:
                try:
                    scroll_div = page.query_selector(selector)
                    if scroll_div:
                        page.evaluate('(element) => { element.scrollTop = element.scrollHeight; }', scroll_div)
                        scrolled = True
                        break
                except:
                    continue
            
            if not scrolled:
                # Fallback: scroll entire window
                page.evaluate('window.scrollBy(0, window.innerHeight)')
            
            time.sleep(CONFIG['SCROLL_DELAY'])
            
            if current_count == last_count:
                scroll_attempts += 1
                if scroll_attempts >= 2:
                    print(f"  ⏸️ No new hospitals ({scroll_attempts}/{max_scrolls})")
            else:
                scroll_attempts = 0
                last_count = current_count
        
        result = {
            'pincode': pincode,
            'district': pincode_info['district'],
            'state': pincode_info['state'],
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'count': len(hospitals),
            'hospitals': hospitals
        }
        
        print(f"✅ Instance {instance_id}: Completed {pincode} — Found {len(hospitals)} hospitals")
        return result
        
    except Exception as e:
        print(f"❌ Instance {instance_id}: Error scraping {pincode}: {e}")
        return None
    finally:
        if page:
            page.close()
        if context:
            context.close()
        if browser:
            browser.close()
        if playwright:
            playwright.stop()


def scrape_chunk_wrapper(args):
    """
    Worker function for multiprocessing Pool.
    Processes a chunk of pincodes assigned to one browser instance.
    """
    instance_id, pincodes, shared_completed, results_list = args
    
    scraper = HospitalScraper(instance_id=instance_id)
    local_completed = set(list(shared_completed))
    
    for pincode, info in pincodes:
        if pincode in local_completed:
            print(f"⏭️  Instance {instance_id}: Skipping {pincode} (already scraped)")
            continue
        
        result = scrape_pincode(pincode, info, instance_id)
        
        if result:
            results_list.append(result)
            local_completed.add(pincode)
            shared_completed.append(pincode)
            scraper.completed_pincodes.add(pincode)
            
            # Incremental save after every pincode
            all_results = {r['pincode']: r for r in list(results_list)}
            scraper.save_results(all_results)
            scraper.save_checkpoint(all_results)
        
        time.sleep(CONFIG['DELAY_BETWEEN_PINCODES'])
    
    return list(results_list)


def scrape_hospital_multi_instance():
    """Multi-instance scraping: splits pincodes across N browser windows"""
    csv_file = CONFIG['CSV_FILE']
    results_file = CONFIG['RESULTS_FILE']
    
    if not os.path.exists(csv_file):
        print(f"❌ CSV file not found: {csv_file}")
        return
    
    print("📖 Reading CSV file...")
    all_pincodes_dict = load_unique_pincodes(csv_file)
    all_pincodes = list(all_pincodes_dict.keys())
    print(f"✅ Total unique pincodes: {len(all_pincodes):,}")
    
    # Load already-completed pincodes
    completed_pincodes = set()
    os.makedirs(os.path.dirname(results_file), exist_ok=True)
    if os.path.exists(results_file):
        try:
            with open(results_file, 'r', encoding='utf-8') as f:
                existing_results = json.load(f)
                completed_pincodes = set(existing_results.keys())
            print(f"📚 Found {len(completed_pincodes)} already scraped pincodes")
        except Exception as e:
            print(f"⚠️ Error loading existing results: {e}")
    
    pending_pincodes = [(p, all_pincodes_dict[p]) for p in all_pincodes if p not in completed_pincodes]
    print(f"📊 Pending pincodes: {len(pending_pincodes):,}")
    
    if not pending_pincodes:
        print("🎉 All pincodes already scraped!")
        return
    
    try:
        num_instances = int(input(f"\nHow many browser instances? (default {CONFIG['MAX_WORKERS']}): ").strip() or CONFIG['MAX_WORKERS'])
    except Exception:
        num_instances = CONFIG['MAX_WORKERS']
    
    print(f"\n🚀 Starting {num_instances} browser instances...")
    print(f"⚠️  Each instance uses ~500MB RAM\n")
    
    # Split pincodes evenly across instances
    chunk_size = (len(pending_pincodes) + num_instances - 1) // num_instances
    pincode_chunks = [pending_pincodes[i:i + chunk_size] for i in range(0, len(pending_pincodes), chunk_size)]
    
    manager = Manager()
    shared_completed = manager.list(list(completed_pincodes))
    results_list = manager.list()
    
    chunks_with_ids = [
        (i + 1, chunk, shared_completed, results_list)
        for i, chunk in enumerate(pincode_chunks) if chunk
    ]
    
    print("📋 Distribution:")
    for instance_id, chunk, _, _ in chunks_with_ids:
        print(f"   Instance {instance_id}: {len(chunk)} pincodes")
    
    print("\n🔄 Starting scraping... (Press Ctrl+C to stop — progress is saved)\n")
    
    all_results_combined = []
    with Pool(processes=num_instances) as pool:
        try:
            all_results_combined = pool.map(scrape_chunk_wrapper, chunks_with_ids)
        except KeyboardInterrupt:
            print("\n⚠️ Interrupted by user. Saving progress...")
            pool.terminate()
            pool.join()
    
    # Merge all results and do a final save
    final_results = {}
    if os.path.exists(results_file):
        try:
            with open(results_file, 'r', encoding='utf-8') as f:
                final_results = json.load(f)
        except Exception:
            pass
    
    for result_list in all_results_combined:
        for result in result_list:
            final_results[result['pincode']] = result
    
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(final_results, f, indent=2, ensure_ascii=False)
    
    total_hospitals = sum(data.get('count', 0) for data in final_results.values())
    print(f"\n{'='*60}")
    print("🎉 SCRAPING COMPLETE!")
    print(f"{'='*60}")
    print(f"📊 Total pincodes processed : {len(final_results):,}")
    print(f"📊 Total hospitals collected: {total_hospitals:,}")
    print(f"💾 Results saved to         : {results_file}")


def scrape_single_instance_with_resume():
    """Single-instance scraping with full resume support"""
    results_file = CONFIG['RESULTS_FILE']
    csv_file = CONFIG['CSV_FILE']
    all_results = {}
    
    os.makedirs(os.path.dirname(results_file), exist_ok=True)
    
    # Load existing results
    completed_pincodes = set()
    if os.path.exists(results_file):
        try:
            with open(results_file, 'r', encoding='utf-8') as f:
                all_results = json.load(f)
                completed_pincodes = set(all_results.keys())
            print(f"📚 Loaded {len(completed_pincodes)} already scraped pincodes")
        except Exception as e:
            print(f"⚠️ Error loading existing results: {e}")
    
    if not os.path.exists(csv_file):
        print(f"❌ CSV file not found: {csv_file}")
        return
    
    print("📖 Reading CSV file...")
    pincode_info = load_unique_pincodes(csv_file)
    all_pincodes = list(pincode_info.keys())
    
    print(f"✅ Total unique pincodes : {len(all_pincodes):,}")
    print(f"✅ Already scraped       : {len(completed_pincodes):,}")
    print(f"✅ Remaining             : {len(all_pincodes) - len(completed_pincodes):,}")
    
    pending_pincodes = [(p, pincode_info[p]) for p in all_pincodes if p not in completed_pincodes]
    
    if not pending_pincodes:
        print("🎉 All pincodes already scraped!")
        return
    
    print(f"\n🔄 Processing {len(pending_pincodes)} remaining pincodes\n")
    
    playwright, browser, context, page = setup_browser(1)
    
    try:
        for i, (pincode, info) in enumerate(pending_pincodes, 1):
            print(f"{'='*60}")
            print(f"📍 [{i}/{len(pending_pincodes)}] Processing pincode: {pincode}")
            if info['district']:
                print(f"   District: {info['district']}, State: {info['state']}")
            print(f"{'='*60}")
            
            url = f"https://www.google.co.in/maps/search/hospital+in+{pincode}/"
            page.goto(url, wait_until='networkidle')
            time.sleep(CONFIG['PAGE_LOAD_DELAY'])
            
            hospitals = []
            seen_names = set()
            last_count = 0
            scroll_attempts = 0
            max_scrolls = CONFIG['MAX_SCROLLS']
            
            while scroll_attempts < max_scrolls:
                # Wait for results to load
                try:
                    page.wait_for_selector('[role="feed"], [role="main"]', timeout=5000)
                except:
                    pass
                
                # Get all result cards using multiple selectors
                cards = page.query_selector_all('[role="article"], .Nv2PK, .bfdHYd, .m6QErb, .DkEaL')
                
                for card in cards:
                    try:
                        text = card.inner_text()
                        if not text or len(text) <= 10:
                            continue
                        
                        lines = text.split('\n')
                        name = lines[0] if lines else None
                        
                        if not name or len(name) <= 2:
                            continue
                        if any(x in name for x in ['Collapse', 'Results', 'Press', 'Google', 'Maps']):
                            continue
                        if name in seen_names:
                            continue
                        
                        seen_names.add(name)
                        
                        # Extract phone number
                        phone = None
                        phone_match = re.search(r'(\+91|0)?[\s-]?[6-9]\d{9}|\d{5}[\s-]?\d{5}', text)
                        if phone_match:
                            phone = phone_match.group()
                        
                        # Extract rating and review count
                        rating = None
                        reviews = None
                        rating_match = re.search(r'(\d+\.?\d*)\s*\((\d+(?:,\d+)?)\)', text)
                        if not rating_match:
                            rating_match = re.search(r'(\d+\.?\d*)\s*[★☆]\s*\((\d+(?:,\d+)?)\)', text)
                        if rating_match:
                            rating = rating_match.group(1)
                            reviews = rating_match.group(2).replace(',', '')
                        
                        # Extract address
                        address = None
                        for line in lines:
                            if ('·' in line or 
                                'Address' in line or 
                                re.search(r'\d+\s+\w+', line) or
                                (len(line) > 20 and ',' in line)):
                                parts = line.split('·')
                                if len(parts) > 1:
                                    address = parts[-1].strip()
                                else:
                                    address = line.strip()
                                break
                        
                        if not address:
                            try:
                                address_elem = card.query_selector('.W4Efsd, .I5k9Mc, .fontBodyMedium, .sXLa0e')
                                if address_elem:
                                    address = address_elem.inner_text()
                            except:
                                pass
                        
                        hospital_entry = {
                            'name': name,
                            'rating': rating,
                            'reviews': reviews,
                            'phone': phone,
                            'address': address,
                            'pincode': pincode,
                            'district': info['district'],
                            'state': info['state']
                        }
                        hospitals.append(hospital_entry)
                        
                    except Exception:
                        continue
                
                current_count = len(hospitals)
                if current_count > last_count:
                    print(f"  📊 Found {current_count} hospitals...")
                
                # Scroll for more results
                scroll_selectors = ['[role="feed"]', '[role="main"]', '.m6QErb', '.DkEaL']
                scrolled = False
                
                for selector in scroll_selectors:
                    try:
                        scroll_div = page.query_selector(selector)
                        if scroll_div:
                            page.evaluate('(element) => { element.scrollTop = element.scrollHeight; }', scroll_div)
                            scrolled = True
                            break
                    except:
                        continue
                
                if not scrolled:
                    page.evaluate('window.scrollBy(0, window.innerHeight)')
                
                time.sleep(CONFIG['SCROLL_DELAY'])
                
                if current_count == last_count:
                    scroll_attempts += 1
                    print(f"  ⏸️ No new hospitals ({scroll_attempts}/{max_scrolls})")
                else:
                    scroll_attempts = 0
                    last_count = current_count
            
            all_results[pincode] = {
                'pincode': pincode,
                'district': info['district'],
                'state': info['state'],
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'count': len(hospitals),
                'hospitals': hospitals
            }
            
            # Save after every pincode
            with open(results_file, 'w', encoding='utf-8') as f:
                json.dump(all_results, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Saved {len(hospitals)} hospitals for {pincode}")
            if hospitals:
                print(f"   📋 Sample: {hospitals[0]['name']}")
            
            if i < len(pending_pincodes):
                print(f"\n⏳ Waiting {CONFIG['DELAY_BETWEEN_PINCODES']}s before next pincode...")
                time.sleep(CONFIG['DELAY_BETWEEN_PINCODES'])
        
        total = sum(data.get('count', 0) for data in all_results.values())
        print(f"\n{'='*60}")
        print("🎉 SCRAPING COMPLETE!")
        print(f"{'='*60}")
        print(f"📊 Total pincodes processed : {len(all_results):,}")
        print(f"📊 Total hospitals collected: {total:,}")
        
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user. Saving progress...")
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        print(f"💾 Progress saved to: {results_file}")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        if all_results:
            with open(results_file, 'w', encoding='utf-8') as f:
                json.dump(all_results, f, indent=2, ensure_ascii=False)
            print(f"💾 Partial results saved to: {results_file}")
    
    finally:
        page.close()
        context.close()
        browser.close()
        playwright.stop()
        print("\n🔒 Browser closed")


if __name__ == "__main__":
    print("=" * 60)
    print("🏥 HOSPITAL SCRAPER WITH PLAYWRIGHT - EXACT SELECTORS")
    print("=" * 60)
    print("\nChoose scraping mode:")
    print("1. Single instance with resume capability (slower but stable)")
    print("2. Multi-instance (multiple browser windows — FASTER, uses more RAM)")
    
    choice = input("\nEnter choice (1 or 2): ").strip()
    
    if choice == "2":
        scrape_hospital_multi_instance()
    else:
        scrape_single_instance_with_resume()