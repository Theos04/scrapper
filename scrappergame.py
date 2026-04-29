import csv
import time
import json
import os
import re
import sqlite3
import logging
from datetime import datetime
from collections import OrderedDict
from multiprocessing import Pool, Manager, cpu_count
from playwright.sync_api import sync_playwright
import traceback

# ============================================================
CONFIG = {
    "MAX_WORKERS": 3,  # Reduced for stability
    "SCROLL_DELAY": 2,
    "MAX_SCROLLS": 15,
    "PAGE_LOAD_DELAY": 5,
    "DELAY_BETWEEN_PINCODES": 2,
    "MAX_RETRIES": 2,
    "DB_FILE": "hospitals.db",
    "LOG_DIR": "scraper_logs",
    "CSV_FILE": r"E:\5c2f62fe-5afa-4119-a499-fec9d604d5bd.csv",
    "BROWSER_TIMEOUT": 60000,
}

# ============================================================

def setup_logger(worker_id):
    os.makedirs(CONFIG["LOG_DIR"], exist_ok=True)
    logger = logging.getLogger(f'worker_{worker_id}')
    logger.setLevel(logging.DEBUG)
    
    # Remove existing handlers
    if logger.handlers:
        logger.handlers.clear()
    
    # File handler
    fh = logging.FileHandler(f"{CONFIG['LOG_DIR']}/worker_{worker_id}.log", encoding='utf-8')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)
    
    # Console handler with proper format
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(f'Worker {worker_id}: %(message)s'))
    logger.addHandler(ch)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger

# ============================================================

class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        self.init_db()
    
    def get_connection(self):
        return sqlite3.connect(self.db_path, timeout=30)
    
    def init_db(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS pincode_progress (
                    pincode TEXT PRIMARY KEY,
                    district TEXT,
                    state TEXT,
                    status TEXT,
                    retry_count INTEGER DEFAULT 0,
                    last_attempt TEXT,
                    completed_at TEXT,
                    business_count INTEGER DEFAULT 0
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS businesses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pincode TEXT,
                    name TEXT,
                    phone TEXT,
                    rating REAL,
                    reviews INTEGER,
                    address TEXT,
                    district TEXT,
                    state TEXT,
                    scraped_at TEXT,
                    UNIQUE(pincode, name, address)
                )
            ''')
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_pincode ON businesses(pincode)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON pincode_progress(status)')
            
            conn.commit()
    
    def get_pending_pincodes(self, limit=None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            query = '''
                SELECT pincode, district, state 
                FROM pincode_progress 
                WHERE status IN ('pending', 'failed') AND retry_count < ?
                ORDER BY retry_count, last_attempt
            '''
            params = [CONFIG["MAX_RETRIES"]]
            
            if limit:
                query += " LIMIT ?"
                params.append(limit)
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            return [(row[0], {"district": row[1], "state": row[2]}) for row in results]
    
    def mark_pincode_started(self, pincode):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE pincode_progress 
                SET status = 'processing', 
                    last_attempt = ?
                WHERE pincode = ?
            ''', (datetime.now().isoformat(), pincode))
            conn.commit()
    
    def mark_pincode_completed(self, pincode, business_count):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE pincode_progress 
                SET status = 'completed', 
                    completed_at = ?,
                    business_count = ?
                WHERE pincode = ?
            ''', (datetime.now().isoformat(), business_count, pincode))
            conn.commit()
    
    def mark_pincode_failed(self, pincode):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE pincode_progress 
                SET status = 'failed',
                retry_count = retry_count + 1
                WHERE pincode = ?
            ''', (pincode,))
            conn.commit()
    
    def insert_businesses(self, businesses):
        if not businesses:
            return 0
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            inserted = 0
            for biz in businesses:
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO businesses 
                        (pincode, name, phone, rating, reviews, address, district, state, scraped_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        biz.get('pincode'),
                        biz.get('name'),
                        biz.get('phone'),
                        biz.get('rating'),
                        biz.get('reviews'),
                        biz.get('address'),
                        biz.get('district'),
                        biz.get('state'),
                        biz.get('scraped_at', datetime.now().isoformat())
                    ))
                    if cursor.rowcount > 0:
                        inserted += 1
                except sqlite3.Error as e:
                    continue
            conn.commit()
            return inserted
    
    def initialize_pincodes(self, pincodes_dict):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            for pincode, info in pincodes_dict.items():
                cursor.execute('''
                    INSERT OR IGNORE INTO pincode_progress 
                    (pincode, district, state, status, retry_count)
                    VALUES (?, ?, ?, 'pending', 0)
                ''', (pincode, info['district'], info['state']))
            conn.commit()
    
    def get_stats(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT status, COUNT(*) FROM pincode_progress GROUP BY status")
            status_counts = dict(cursor.fetchall())
            
            cursor.execute("SELECT COUNT(*) FROM businesses")
            total_businesses = cursor.fetchone()[0]
            
            return {
                "status_counts": status_counts,
                "total_businesses": total_businesses
            }
    
    def reset_stuck_processing(self):
        """Reset pincodes that have been processing for too long"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # Reset pincodes that have been processing for more than 30 minutes
            cursor.execute('''
                UPDATE pincode_progress 
                SET status = 'pending'
                WHERE status = 'processing' 
                AND julianday('now') - julianday(last_attempt) > 0.02
            ''')
            conn.commit()
            return cursor.rowcount

# ============================================================

def load_unique_pincodes(csv_file):
    pincodes = OrderedDict()
    
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("pincode"):
                p = row["pincode"].strip()
                if p not in pincodes:
                    pincodes[p] = {
                        "district": row.get("district", ""),
                        "state": row.get("statename", "")
                    }
    return pincodes

# ============================================================

def scrape_pincode_playwright(pincode, info, worker_id, logger):
    """Scrape hospitals for a single pincode"""
    hospitals = []
    seen = set()
    
    try:
        logger.info(f"Starting scrape for pincode {pincode}")
        
        with sync_playwright() as p:
            # Launch browser with error handling
            browser = p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
            page = browser.new_page()
            
            # Set timeout
            page.set_default_timeout(CONFIG["BROWSER_TIMEOUT"])
            
            url = f"https://www.google.co.in/maps/search/hospital+in+{pincode}/"
            logger.debug(f"Navigating to {url}")
            
            page.goto(url, wait_until='domcontentloaded')
            time.sleep(CONFIG["PAGE_LOAD_DELAY"])
            
            last_count = 0
            scroll_attempts = 0
            
            while scroll_attempts < CONFIG["MAX_SCROLLS"]:
                try:
                    # Wait for cards to load
                    page.wait_for_selector('[role="article"], .Nv2PK', timeout=10000)
                    cards = page.locator('[role="article"], .Nv2PK')
                    count = cards.count()
                    
                    logger.debug(f"Found {count} cards on page")
                    
                    for i in range(count):
                        try:
                            text = cards.nth(i).inner_text()
                            if not text or len(text) < 10:
                                continue
                            
                            lines = text.split("\n")
                            name = lines[0] if lines else None
                            
                            if not name or name in seen:
                                continue
                            if any(x in name.lower() for x in ["collapse", "results", "press"]):
                                continue
                            
                            seen.add(name)
                            
                            # Extract phone
                            phone = None
                            m = re.search(r'(\+91|0)?[\s-]?[6-9]\d{9}', text)
                            if m:
                                phone = m.group()
                            
                            # Extract rating and reviews
                            rating = None
                            reviews = None
                            rm = re.search(r'(\d+\.?\d*)\s*\((\d+)\)', text)
                            if rm:
                                try:
                                    rating = float(rm.group(1))
                                    reviews = int(rm.group(2))
                                except:
                                    pass
                            
                            # Extract address
                            address = None
                            for line in lines:
                                if "·" in line:
                                    parts = line.split("·")
                                    if len(parts) > 1:
                                        address = parts[-1].strip()
                                    break
                            
                            hospital = {
                                "name": name,
                                "phone": phone,
                                "rating": rating,
                                "reviews": reviews,
                                "address": address,
                                "pincode": pincode,
                                "district": info["district"],
                                "state": info["state"],
                                "scraped_at": datetime.now().isoformat()
                            }
                            
                            hospitals.append(hospital)
                            logger.debug(f"Found hospital: {name}")
                            
                        except Exception as e:
                            logger.debug(f"Error parsing card {i}: {e}")
                            continue
                    
                    logger.info(f"Worker {worker_id} | {pincode}: {len(hospitals)} hospitals so far")
                    
                    # Scroll down
                    page.evaluate("window.scrollBy(0, window.innerHeight)")
                    time.sleep(CONFIG["SCROLL_DELAY"])
                    
                    if len(hospitals) == last_count:
                        scroll_attempts += 1
                        logger.debug(f"No new hospitals, scroll_attempts={scroll_attempts}")
                    else:
                        scroll_attempts = 0
                        last_count = len(hospitals)
                        
                except Exception as e:
                    logger.error(f"Error during scrolling: {e}")
                    break
            
            browser.close()
            
        logger.info(f"Completed {pincode} with {len(hospitals)} hospitals")
        return hospitals
        
    except Exception as e:
        logger.error(f"Failed to scrape {pincode}: {e}")
        logger.error(traceback.format_exc())
        raise

# ============================================================

def worker(worker_id, pincode_list, db_path):
    """Worker process for scraping"""
    logger = setup_logger(worker_id)
    db = Database(db_path)
    
    logger.info(f"Worker {worker_id} started with {len(pincode_list)} pincodes")
    
    success_count = 0
    fail_count = 0
    
    for idx, (pincode, info) in enumerate(pincode_list, 1):
        try:
            logger.info(f"[{idx}/{len(pincode_list)}] Processing pincode: {pincode}")
            
            # Mark as started
            db.mark_pincode_started(pincode)
            
            # Scrape with retries
            hospitals = None
            for attempt in range(CONFIG["MAX_RETRIES"]):
                try:
                    hospitals = scrape_pincode_playwright(pincode, info, worker_id, logger)
                    break
                except Exception as e:
                    logger.error(f"Attempt {attempt + 1} failed for {pincode}: {e}")
                    if attempt < CONFIG["MAX_RETRIES"] - 1:
                        wait_time = 5 * (attempt + 1)
                        logger.info(f"Waiting {wait_time} seconds before retry...")
                        time.sleep(wait_time)
            
            if hospitals is not None:
                # Insert into database
                inserted = db.insert_businesses(hospitals)
                logger.info(f"Inserted {inserted} new businesses for {pincode}")
                
                # Mark as completed
                db.mark_pincode_completed(pincode, len(hospitals))
                success_count += 1
            else:
                db.mark_pincode_failed(pincode)
                fail_count += 1
                logger.error(f"Failed to process {pincode} after all retries")
            
            # Delay between pincodes
            if idx < len(pincode_list):
                time.sleep(CONFIG["DELAY_BETWEEN_PINCODES"])
                
        except Exception as e:
            logger.error(f"Unexpected error for {pincode}: {e}")
            logger.error(traceback.format_exc())
            db.mark_pincode_failed(pincode)
            fail_count += 1
    
    logger.info(f"Worker {worker_id} finished. Success: {success_count}, Failed: {fail_count}")
    return success_count

# ============================================================

def run():
    print("📖 Loading CSV...")
    pincodes = load_unique_pincodes(CONFIG["CSV_FILE"])
    print(f"✅ Total unique pincodes: {len(pincodes)}")
    
    # Initialize database
    db = Database(CONFIG["DB_FILE"])
    db.initialize_pincodes(pincodes)
    
    # Reset stuck processing pincodes
    reset_count = db.reset_stuck_processing()
    if reset_count > 0:
        print(f"🔄 Reset {reset_count} stuck pincodes")
    
    # Show current stats
    stats = db.get_stats()
    print(f"📊 Current status: {stats['status_counts']}")
    print(f"🏥 Total businesses in DB: {stats['total_businesses']}")
    
    # Get pending pincodes
    pending = db.get_pending_pincodes(limit=100)  # Process in batches of 100
    print(f"🔄 Pending pincodes in this batch: {len(pending)}")
    
    if not pending:
        print("✅ All pincodes processed successfully!")
        return
    
    try:
        workers = int(input(f"Workers (default {CONFIG['MAX_WORKERS']}): ") or CONFIG["MAX_WORKERS"])
    except:
        workers = CONFIG["MAX_WORKERS"]
    
    # Process in batches to avoid memory issues
    batch_size = 100
    total_batches = (len(pending) + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(pending))
        batch = pending[start_idx:end_idx]
        
        print(f"\n📦 Processing batch {batch_num + 1}/{total_batches} ({len(batch)} pincodes)")
        
        # Split batch among workers
        chunk_size = (len(batch) + workers - 1) // workers
        chunks = [batch[i:i + chunk_size] for i in range(0, len(batch), chunk_size)]
        
        print(f"🚀 Starting {len(chunks)} workers for this batch...")
        
        # Use starmap with timeout
        with Pool(workers) as pool:
            args = [(i + 1, chunk, CONFIG["DB_FILE"]) for i, chunk in enumerate(chunks)]
            results = pool.starmap(worker, args)
        
        print(f"✅ Batch {batch_num + 1} completed")
        
        # Show progress
        stats = db.get_stats()
        print(f"📊 Progress: {stats['status_counts']}")
        print(f"🏥 Total businesses: {stats['total_businesses']}")
        
        # Small delay between batches
        if batch_num < total_batches - 1:
            print("⏸️ Waiting 5 seconds before next batch...")
            time.sleep(5)
    
    # Show final stats
    final_stats = db.get_stats()
    print("\n" + "="*50)
    print("🎉 SCRAPING COMPLETE!")
    print(f"📊 Final status: {final_stats['status_counts']}")
    print(f"🏥 Total businesses collected: {final_stats['total_businesses']}")
    print(f"📁 Database: {CONFIG['DB_FILE']}")
    print(f"📝 Logs: {CONFIG['LOG_DIR']}")
    print("="*50)

# ============================================================

if __name__ == "__main__":
    # Install playwright browsers if not already installed
    print("🔧 Checking Playwright browsers...")
    os.system("playwright install chromium")
    
    run()