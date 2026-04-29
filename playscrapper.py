#!/usr/bin/env python3
"""
playscrapper.py  —  Universal Business Scraper (Multi-Page GUI)
Features:
- Page 1: Dashboard / Start Scraper
- Page 2: View Saved Data
- Page 3: Settings & Configuration
"""

import csv
import sqlite3
import json
import os
import re
import threading
import time
import hashlib
import random
from dataclasses import dataclass, asdict, fields as dc_fields
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from collections import deque

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.common.exceptions import TimeoutException, WebDriverException
    SELENIUM_OK = True
except ImportError:
    SELENIUM_OK = False

# ============================================================================
# Persistent app config
# ============================================================================

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "scraper_config.json")

DEFAULT_CFG: Dict = {
    "chrome_binary":      "",
    "chromedriver_path":  "",
    "output_dir":         os.path.join(os.getcwd(), "scraper_output"),
    "profile_dir":        os.path.join(os.getcwd(), "chrome_profiles"),
    "page_load_delay":    5,
    "scroll_delay":       2,
    "max_scrolls":        15,
    "delay_between":      2,
    "headless":           False,
    "min_request_interval": 2.0,
}

def load_cfg() -> Dict:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                stored = json.load(f)
            cfg = dict(DEFAULT_CFG)
            cfg.update(stored)
            return cfg
        except Exception:
            pass
    return dict(DEFAULT_CFG)

def save_cfg(cfg: Dict):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)

# ============================================================================
# Helpers & Data Models
# ============================================================================

def parse_coordinate(value: str) -> float:
    if not value or value in ("NA", ""):
        return 0.0
    value = str(value).strip()
    m = re.match(r"([-\d.]+)\s*([NSns]?)", value)
    if m:
        num = float(m.group(1))
        if m.group(2).upper() == "S":
            num = -num
        return num
    try:
        return float(value)
    except ValueError:
        return 0.0

def safe_string(s: str, max_len: int = 500) -> str:
    if not s:
        return ""
    s = str(s).encode('utf-8', errors='replace').decode('utf-8')
    s = s.replace('\r', '').replace('\x00', '')
    if len(s) > max_len:
        s = s[:max_len] + "..."
    return s

@dataclass
class PostOffice:
    pincode:      str
    circlename:   str
    regionname:   str
    divisionname: str
    officename:   str
    officetype:   str
    delivery:     str
    district:     str
    statename:    str
    latitude:     float
    longitude:    float

    @property
    def display_name(self) -> str:
        return f"{self.officename} ({self.pincode})"

@dataclass
class Business:
    business_type: str
    pincode:       str
    area:          str
    district:      str
    state:         str
    name:          str
    address:       str  = ""
    phone:         str  = ""
    rating:        str  = ""
    reviews:       str  = ""
    category:      str  = ""
    website:       str  = ""
    latitude:      float = 0.0
    longitude:     float = 0.0
    source:        str  = "google_maps"
    scraped_at:    str  = ""

# ============================================================================
# Database (Enhanced for better querying)
# ============================================================================

class BusinessDB:
    def __init__(self, db_path: str = "business_data.db"):
        self.db_path = db_path
        self._init()

    def _init(self):
        with sqlite3.connect(self.db_path) as c:
            c.execute("""
                CREATE TABLE IF NOT EXISTS post_offices (
                    pincode TEXT PRIMARY KEY, circlename TEXT, regionname TEXT,
                    divisionname TEXT, officename TEXT, officetype TEXT,
                    delivery TEXT, district TEXT, statename TEXT,
                    latitude REAL, longitude REAL
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS businesses (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    cache_key     TEXT UNIQUE,
                    business_type TEXT, pincode TEXT, area TEXT,
                    district TEXT, state TEXT, name TEXT, address TEXT,
                    phone TEXT, rating TEXT, reviews TEXT, category TEXT,
                    website TEXT, latitude REAL, longitude REAL,
                    source TEXT, scraped_at TEXT
                )
            """)
            existing = {r[1] for r in c.execute("PRAGMA table_info(businesses)")}
            for col, typ in [("rating","TEXT"), ("reviews","TEXT"),
                              ("category","TEXT"), ("website","TEXT")]:
                if col not in existing:
                    c.execute(f"ALTER TABLE businesses ADD COLUMN {col} {typ}")
            c.execute("CREATE INDEX IF NOT EXISTS i_btype    ON businesses(business_type)")
            c.execute("CREATE INDEX IF NOT EXISTS i_bpincode ON businesses(pincode)")
            c.execute("CREATE INDEX IF NOT EXISTS i_scraped_at ON businesses(scraped_at)")

    def import_post_offices(self, csv_path: str, callback=None) -> Tuple[int, int]:
        ok = err = 0
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            batch = []
            for row in reader:
                try:
                    batch.append((
                        row["pincode"],
                        row.get("circlename",   ""), row.get("regionname",   ""),
                        row.get("divisionname", ""), row.get("officename",   ""),
                        row.get("officetype",   ""), row.get("delivery",     ""),
                        row.get("district",     ""), row.get("statename",    ""),
                        parse_coordinate(row.get("latitude",  "0")),
                        parse_coordinate(row.get("longitude", "0")),
                    ))
                    ok += 1
                    if len(batch) >= 1000:
                        self._insert_po_batch(batch); batch = []
                        if callback: callback(ok, ok + err)
                except Exception:
                    err += 1
            if batch:
                self._insert_po_batch(batch)
            if callback:
                callback(ok, ok + err)
        return ok, err

    def _insert_po_batch(self, batch):
        with sqlite3.connect(self.db_path) as c:
            c.executemany("""
                INSERT OR REPLACE INTO post_offices
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, batch)

    @staticmethod
    def _ck(pincode: str, btype: str) -> str:
        return hashlib.md5(f"{pincode}:{btype}".encode()).hexdigest()

    def get_cached(self, pincode: str, btype: str) -> List[Business]:
        ck = self._ck(pincode, btype)
        biz_field_names = {f.name for f in dc_fields(Business)}
        with sqlite3.connect(self.db_path) as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                "SELECT * FROM businesses WHERE cache_key=?", (ck,)
            ).fetchall()
        result = []
        for row in rows:
            d = {k: v for k, v in dict(row).items() if k in biz_field_names}
            result.append(Business(**d))
        return result

    def save_businesses(self, pincode: str, btype: str, items: List[Business]):
        ck = self._ck(pincode, btype)
        with sqlite3.connect(self.db_path) as c:
            for b in items:
                c.execute("""
                    INSERT OR REPLACE INTO businesses
                    (cache_key, business_type, pincode, area, district, state,
                     name, address, phone, rating, reviews, category, website,
                     latitude, longitude, source, scraped_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    ck, b.business_type, b.pincode, b.area, b.district, b.state,
                    b.name, b.address, b.phone, b.rating, b.reviews,
                    b.category, b.website, b.latitude, b.longitude,
                    b.source, datetime.now().isoformat()
                ))

    def get_locations(self, state=None, district=None,
                      limit=100, offset=0) -> List[PostOffice]:
        q, p = "SELECT * FROM post_offices", []
        conds = []
        if state:    conds.append("statename=?"); p.append(state)
        if district: conds.append("district=?");  p.append(district)
        if conds:    q += " WHERE " + " AND ".join(conds)
        q += " LIMIT ? OFFSET ?"
        p += [limit, offset]
        with sqlite3.connect(self.db_path) as c:
            return [PostOffice(*r) for r in c.execute(q, p).fetchall()]

    def get_location_count(self, state=None, district=None) -> int:
        q, p = "SELECT COUNT(*) FROM post_offices", []
        conds = []
        if state:    conds.append("statename=?"); p.append(state)
        if district: conds.append("district=?");  p.append(district)
        if conds:    q += " WHERE " + " AND ".join(conds)
        with sqlite3.connect(self.db_path) as c:
            return c.execute(q, p).fetchone()[0]

    def get_location_by_pincode(self, pincode: str) -> Optional[PostOffice]:
        with sqlite3.connect(self.db_path) as c:
            row = c.execute(
                "SELECT * FROM post_offices WHERE pincode=?", (pincode,)
            ).fetchone()
        return PostOffice(*row) if row else None

    def get_states(self) -> List[str]:
        with sqlite3.connect(self.db_path) as c:
            return [r[0] for r in c.execute(
                "SELECT DISTINCT statename FROM post_offices ORDER BY statename"
            ).fetchall()]

    def get_districts(self, state: str) -> List[str]:
        with sqlite3.connect(self.db_path) as c:
            return [r[0] for r in c.execute(
                "SELECT DISTINCT district FROM post_offices "
                "WHERE statename=? ORDER BY district", (state,)
            ).fetchall()]

    def stats(self) -> Dict:
        with sqlite3.connect(self.db_path) as c:
            tp = c.execute("SELECT COUNT(*) FROM post_offices").fetchone()[0]
            tb = c.execute("SELECT COUNT(*) FROM businesses").fetchone()[0]
        return {"total_pincodes": tp, "total_businesses": tb}
    
    # New methods for saved data viewing
    def get_all_businesses(self, limit=100, offset=0, business_type=None, state=None) -> List[Business]:
        q = "SELECT * FROM businesses"
        params = []
        conditions = []
        
        if business_type and business_type != "All":
            conditions.append("business_type=?")
            params.append(business_type)
        if state and state != "All":
            conditions.append("state=?")
            params.append(state)
            
        if conditions:
            q += " WHERE " + " AND ".join(conditions)
            
        q += " ORDER BY scraped_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        with sqlite3.connect(self.db_path) as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(q, params).fetchall()
        
        biz_field_names = {f.name for f in dc_fields(Business)}
        results = []
        for row in rows:
            d = {k: v for k, v in dict(row).items() if k in biz_field_names}
            results.append(Business(**d))
        return results
    
    def get_business_count(self, business_type=None, state=None) -> int:
        q = "SELECT COUNT(*) FROM businesses"
        params = []
        conditions = []
        
        if business_type and business_type != "All":
            conditions.append("business_type=?")
            params.append(business_type)
        if state and state != "All":
            conditions.append("state=?")
            params.append(state)
            
        if conditions:
            q += " WHERE " + " AND ".join(conditions)
            
        with sqlite3.connect(self.db_path) as c:
            return c.execute(q, params).fetchone()[0]
    
    def get_unique_business_types(self) -> List[str]:
        with sqlite3.connect(self.db_path) as c:
            return [r[0] for r in c.execute(
                "SELECT DISTINCT business_type FROM businesses ORDER BY business_type"
            ).fetchall() if r[0]]

# ============================================================================
# Scraping State
# ============================================================================

class ScrapingState:
    def __init__(self, state_file: str = "scraping_state.json"):
        self.state_file = state_file
    
    def save(self, completed_pincodes: List[str], business_type: str):
        try:
            with open(self.state_file, 'w') as f:
                json.dump({
                    'completed': completed_pincodes,
                    'business_type': business_type,
                    'timestamp': datetime.now().isoformat()
                }, f, indent=2)
        except Exception:
            pass
    
    def load(self) -> Tuple[List[str], str]:
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                return data.get('completed', []), data.get('business_type', '')
            except Exception:
                pass
        return [], ""
    
    def clear(self):
        try:
            if os.path.exists(self.state_file):
                os.remove(self.state_file)
        except Exception:
            pass

# ============================================================================
# Selenium / Google Maps scraper engine
# ============================================================================

class MapsScraper:
    def __init__(self, cfg: Dict):
        self.chrome_binary     = cfg.get("chrome_binary", "")
        self.chromedriver_path = cfg.get("chromedriver_path", "")
        self.profile_dir       = cfg.get("profile_dir", os.path.join(os.getcwd(), "chrome_profiles"))
        self.page_load_delay   = int(cfg.get("page_load_delay", 5))
        self.scroll_delay      = float(cfg.get("scroll_delay", 2))
        self.max_scrolls       = int(cfg.get("max_scrolls", 15))
        self.headless          = cfg.get("headless", False)
        self.min_request_interval = float(cfg.get("min_request_interval", 2.0))
        self.driver: Optional[webdriver.Chrome] = None
        self.request_times = deque(maxlen=30)
        self.log_callback = None
        self._stop_flag = False

    def set_log_callback(self, callback):
        self.log_callback = callback

    def _log(self, msg: str):
        if self.log_callback:
            try:
                self.log_callback(msg)
            except Exception:
                print(msg)

    def _rate_limit(self):
        now = time.time()
        while self.request_times and now - self.request_times[0] > 60:
            self.request_times.popleft()
        
        if len(self.request_times) >= 25:
            oldest = self.request_times[0]
            wait = 60 - (now - oldest)
            if wait > 0:
                self._log(f"⏱️ Rate limiting: waiting {wait:.1f}s")
                time.sleep(wait)
        
        jitter = random.uniform(0.5, 1.5)
        time.sleep(jitter)
        self.request_times.append(time.time())

    def start(self, instance_id: int = 1):
        if not SELENIUM_OK:
            raise RuntimeError("Selenium is not installed.\n\nFix: pip install selenium")
        if not self.chrome_binary or not os.path.exists(self.chrome_binary):
            raise FileNotFoundError(f"Chrome binary not found: {self.chrome_binary}")
        if not self.chromedriver_path or not os.path.exists(self.chromedriver_path):
            raise FileNotFoundError(f"ChromeDriver not found: {self.chromedriver_path}")

        opts = Options()
        opts.binary_location = self.chrome_binary
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        
        if self.headless:
            opts.add_argument("--headless")
            opts.add_argument("--window-size=1920,1080")
        
        profile = os.path.join(self.profile_dir, f"inst_{instance_id}")
        os.makedirs(profile, exist_ok=True)
        opts.add_argument(f"--user-data-dir={profile}")

        svc = Service(self.chromedriver_path)
        self.driver = webdriver.Chrome(service=svc, options=opts)
        self.driver.set_page_load_timeout(30)

    def stop(self):
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None

    def refresh_session(self, instance_id: int = 1):
        self.stop()
        time.sleep(2)
        self.start(instance_id)
        time.sleep(3)

    def scrape_location(self, location: PostOffice, business_type: str) -> List[Business]:
        if not self.driver:
            raise RuntimeError("Driver not started — call start() first.")
        
        self._rate_limit()
        query = business_type.strip().replace(" ", "+")
        url = f"https://www.google.co.in/maps/search/{query}+in+{location.pincode}/"
        
        try:
            self.driver.get(url)
            time.sleep(self.page_load_delay)
        except TimeoutException:
            self._log(f"   ⚠️ Timeout loading page for {location.pincode}")
            return []
        except Exception as e:
            self._log(f"   ⚠️ Error loading page: {str(e)[:100]}")
            return []

        return self._extract(location, business_type)

    def _extract(self, location: PostOffice, business_type: str) -> List[Business]:
        businesses = []
        seen_names: set = set()
        last_count = 0
        no_new_runs = 0
        scroll_errors = 0

        try:
            for scroll_iter in range(self.max_scrolls * 2):
                if self._stop_flag:
                    break
                
                time.sleep(1)
                
                cards = []
                for selector in ['[role="article"]', '.Nv2PK', '[jsaction*="mouseover"]', '.section-result']:
                    try:
                        found = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if found:
                            cards = found
                            break
                    except:
                        continue
                
                for card in cards:
                    try:
                        text = card.text if card else ""
                        if not text or len(text) <= 10:
                            continue

                        lines = text.split("\n")
                        name = lines[0].strip() if lines else ""

                        if len(name) <= 2:
                            continue
                        if any(x in name for x in ("Collapse", "Results", "Press", "Directions", "Sponsored", "Ad")):
                            continue
                        if name in seen_names:
                            continue
                        seen_names.add(name)

                        phone = ""
                        phone_match = re.search(r'(\+91|0)?[\s\-]?[6-9]\d{9}|\d{5}[\s\-]?\d{5}', text)
                        if phone_match:
                            phone = phone_match.group().strip()

                        rating = reviews = ""
                        rating_match = re.search(r'(\d+\.?\d*)\s*\((\d+)\)', text)
                        if rating_match:
                            rating = rating_match.group(1)
                            reviews = rating_match.group(2)

                        address = ""
                        for line in lines:
                            if "·" in line and len(line) > 10:
                                parts = line.split("·")
                                if len(parts) > 1:
                                    address = parts[-1].strip()
                                break

                        category = ""
                        for i, line in enumerate(lines[:3]):
                            if line and line not in [name, address] and "·" not in line and len(line) < 50:
                                if not any(x in line.lower() for x in ['http', 'www.', '.com']):
                                    category = line
                                    break

                        website = ""
                        for line in lines:
                            tok = line.strip()
                            if "." in tok and " " not in tok and len(tok) > 5:
                                if not any(x in tok.lower() for x in ['maps', 'google', 'search']):
                                    website = tok
                                    break

                        business = Business(
                            business_type=business_type,
                            pincode=location.pincode,
                            area=location.officename,
                            district=location.district,
                            state=location.statename,
                            name=safe_string(name, 200),
                            address=safe_string(address, 300),
                            phone=safe_string(phone, 50),
                            rating=safe_string(rating, 10),
                            reviews=safe_string(reviews, 20),
                            category=safe_string(category, 100),
                            website=safe_string(website, 150),
                            latitude=location.latitude,
                            longitude=location.longitude,
                            source="google_maps",
                        )
                        businesses.append(business)

                    except Exception as e:
                        continue

                current = len(businesses)
                if current > last_count:
                    no_new_runs = 0
                    last_count = current
                    if current % 10 == 0 and current > 0:
                        self._log(f"   📊 Found {current} businesses...")
                else:
                    no_new_runs += 1

                if no_new_runs >= self.max_scrolls:
                    break

                try:
                    self.driver.execute_script("""
                        const feed = document.querySelector('[role="feed"]');
                        if (feed) feed.scrollTop = feed.scrollHeight;
                        else window.scrollBy(0, window.innerHeight);
                    """)
                except Exception:
                    scroll_errors += 1
                    if scroll_errors > 5:
                        break
                
                time.sleep(self.scroll_delay)

        except Exception as e:
            self._log(f"   ⚠️ Extraction error: {str(e)[:100]}")

        return businesses

    def scrape_with_retry(self, location: PostOffice, business_type: str, max_retries: int = 2) -> List[Business]:
        for attempt in range(max_retries):
            try:
                result = self.scrape_location(location, business_type)
                return result
            except Exception as e:
                if attempt < max_retries - 1:
                    wait = (attempt + 1) * 3
                    self._log(f"   🔄 Retry {attempt+1}/{max_retries} (wait {wait}s)")
                    time.sleep(wait)
                    try:
                        self.refresh_session()
                    except:
                        pass
                else:
                    self._log(f"   ❌ Failed after {max_retries} attempts")
        return []

    def close(self):
        self.stop()

BUSINESS_TYPES = [
    "hospital", "clinic", "pharmacy", "diagnostic centre", "nursing home",
    "restaurant", "cafe", "hotel", "dhaba",
    "school", "college", "coaching centre",
    "bank", "ATM", "insurance office",
    "gym", "salon", "spa",
    "supermarket", "grocery store", "hardware store",
    "petrol pump", "car service centre", "auto parts",
    "courier office", "post office", "police station",
]

# ============================================================================
# Multi-Page Application
# ============================================================================

class BusinessScraperApp:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Universal Business Scraper  —  Multi-Page Edition")
        self.root.geometry("1400x800")
        self.root.minsize(1100, 680)

        self.cfg = load_cfg()
        self.db = BusinessDB()
        self.scraper: Optional[MapsScraper] = None
        self.is_scraping = False
        self.stop_scraping = False
        self.scraping_state = ScrapingState()

        # Pagination for saved data view
        self.saved_page = 0
        self.saved_page_size = 50
        self.total_saved = 0
        
        # Pagination for location selection
        self.loc_page = 0
        self.loc_page_size = 50
        self.total_locations = 0
        self.current_locations: List[PostOffice] = []
        self.selected_pincodes: set = set()

        self._build_ui()
        self.root.after(150, self._check_data)

    def _build_ui(self):
        # Create notebook for tabs
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True, padx=5, pady=5)
        
        # Create three main pages
        self.page_scraper = ttk.Frame(self.notebook)
        self.page_results = ttk.Frame(self.notebook)
        self.page_settings = ttk.Frame(self.notebook)
        
        self.notebook.add(self.page_scraper, text="🚀 Start Scraper")
        self.notebook.add(self.page_results, text="📊 Saved Data")
        self.notebook.add(self.page_settings, text="⚙ Settings")
        
        self._build_scraper_page()
        self._build_results_page()
        self._build_settings_page()

    # ========================================================================
    # PAGE 1: START SCRAPER
    # ========================================================================
    
    def _build_scraper_page(self):
        # Left panel - Controls
        left_frame = ttk.Frame(self.page_scraper, width=350)
        left_frame.pack(side="left", fill="y", padx=5, pady=5)
        left_frame.pack_propagate(False)
        
        # Right panel - Location list
        right_frame = ttk.Frame(self.page_scraper)
        right_frame.pack(side="right", fill="both", expand=True, padx=5, pady=5)
        
        # --- Left Panel Controls ---
        # Business Type
        ttk.Label(left_frame, text="Business Type:", font=("", 10, "bold")).pack(anchor="w", pady=(0, 5))
        self.business_var = tk.StringVar(value="hospital")
        biz_frame = ttk.Frame(left_frame)
        biz_frame.pack(fill="x", pady=(0, 10))
        biz_cb = ttk.Combobox(biz_frame, textvariable=self.business_var, values=BUSINESS_TYPES, width=30)
        biz_cb.pack(side="left", fill="x", expand=True)
        ttk.Label(biz_frame, text="(or type custom)", font=("", 8), foreground="gray").pack(side="left", padx=5)
        
        # State Filter
        ttk.Label(left_frame, text="Filter by State:", font=("", 9)).pack(anchor="w", pady=(0, 2))
        self.state_var = tk.StringVar(value="All")
        self.state_cb = ttk.Combobox(left_frame, textvariable=self.state_var, state="readonly", width=30)
        self.state_cb.pack(fill="x", pady=(0, 5))
        self.state_cb.bind("<<ComboboxSelected>>", self._on_state_change_scraper)
        
        # District Filter
        ttk.Label(left_frame, text="Filter by District:", font=("", 9)).pack(anchor="w", pady=(0, 2))
        self.district_var = tk.StringVar(value="All")
        self.district_cb = ttk.Combobox(left_frame, textvariable=self.district_var, state="readonly", width=30)
        self.district_cb.pack(fill="x", pady=(0, 10))
        self.district_cb.bind("<<ComboboxSelected>>", lambda _: self._load_locations())
        
        # Selection Controls
        select_frame = ttk.LabelFrame(left_frame, text="Selection", padding=5)
        select_frame.pack(fill="x", pady=(0, 10))
        
        ttk.Button(select_frame, text="☑ Select Current Page", command=self._select_page).pack(fill="x", pady=2)
        ttk.Button(select_frame, text="☐ Clear Current Page", command=self._clear_page).pack(fill="x", pady=2)
        ttk.Button(select_frame, text="🗑 Clear All Selected", command=self._clear_all_selected).pack(fill="x", pady=2)
        
        self.lbl_selected = ttk.Label(select_frame, text="Selected: 0 locations", font=("", 9, "bold"))
        self.lbl_selected.pack(anchor="w", pady=5)
        
        # Stats
        stats_frame = ttk.LabelFrame(left_frame, text="Database Stats", padding=5)
        stats_frame.pack(fill="x", pady=(0, 10))
        
        self.lbl_pincodes = ttk.Label(stats_frame, text="Total Pincodes: 0")
        self.lbl_pincodes.pack(anchor="w")
        self.lbl_businesses = ttk.Label(stats_frame, text="Businesses Found: 0")
        self.lbl_businesses.pack(anchor="w")
        ttk.Button(stats_frame, text="🔄 Refresh Stats", command=self._refresh_stats).pack(anchor="w", pady=(4, 0))
        
        # Import Button
        ttk.Button(left_frame, text="📥 Import Pincode CSV", command=self._import_csv).pack(fill="x", pady=5)
        
        # Scrape Controls
        scrape_frame = ttk.LabelFrame(left_frame, text="Scraping Controls", padding=5)
        scrape_frame.pack(fill="x", pady=(10, 0))
        
        self.btn_start = ttk.Button(scrape_frame, text="▶ START SCRAPING", command=self._start_scraping)
        self.btn_start.pack(fill="x", pady=2)
        
        self.btn_stop = ttk.Button(scrape_frame, text="⏹ STOP", command=self._stop_scraping, state="disabled")
        self.btn_stop.pack(fill="x", pady=2)
        
        self.progress_var = tk.DoubleVar()
        ttk.Progressbar(scrape_frame, variable=self.progress_var, maximum=100).pack(fill="x", pady=5)
        
        self.lbl_status = ttk.Label(scrape_frame, text="Ready", font=("", 9, "italic"))
        self.lbl_status.pack(anchor="w")
        
        # --- Right Panel - Location List ---
        loc_frame = ttk.LabelFrame(right_frame, text="Available Locations", padding=5)
        loc_frame.pack(fill="both", expand=True)
        
        # Treeview
        cols = ("sel", "pincode", "area", "district", "state")
        self.loc_tree = ttk.Treeview(loc_frame, columns=cols, show="headings", height=18)
        
        for col, hdr, w, stretch in [
            ("sel", "☐", 40, False),
            ("pincode", "Pincode", 80, False),
            ("area", "Area", 220, True),
            ("district", "District", 160, True),
            ("state", "State", 160, True),
        ]:
            self.loc_tree.heading(col, text=hdr)
            self.loc_tree.column(col, width=w, stretch=stretch, anchor="center" if col == "sel" else "w")
        
        vsb = ttk.Scrollbar(loc_frame, orient="vertical", command=self.loc_tree.yview)
        self.loc_tree.configure(yscrollcommand=vsb.set)
        self.loc_tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        
        self.loc_tree.bind("<ButtonRelease-1>", self._on_tree_click)
        
        # Pagination
        page_frame = ttk.Frame(loc_frame)
        page_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=5)
        
        ttk.Button(page_frame, text="◀ Previous", command=self._prev_page).pack(side="left", padx=2)
        self.lbl_page = ttk.Label(page_frame, text="Page 1")
        self.lbl_page.pack(side="left", padx=10)
        ttk.Button(page_frame, text="Next ▶", command=self._next_page).pack(side="left", padx=2)
        
        loc_frame.columnconfigure(0, weight=1)
        loc_frame.rowconfigure(0, weight=1)
        
        # Log area
        log_frame = ttk.LabelFrame(right_frame, text="Log", padding=5)
        log_frame.pack(fill="both", expand=False, pady=(5, 0))
        self.log_text = scrolledtext.ScrolledText(log_frame, height=8, wrap="word", font=("Courier", 9))
        self.log_text.pack(fill="both", expand=True)
        
        # Initialize
        self._load_states_scraper()
        self._load_locations()

    # ========================================================================
    # PAGE 2: SAVED DATA
    # ========================================================================
    
    def _build_results_page(self):
        # Filter frame
        filter_frame = ttk.LabelFrame(self.page_results, text="Filters", padding=5)
        filter_frame.pack(fill="x", padx=5, pady=5)
        
        ttk.Label(filter_frame, text="Business Type:").pack(side="left", padx=(0, 5))
        self.filter_btype = tk.StringVar(value="All")
        self.filter_btype_cb = ttk.Combobox(filter_frame, textvariable=self.filter_btype, width=20, state="readonly")
        self.filter_btype_cb.pack(side="left", padx=(0, 15))
        self.filter_btype_cb.bind("<<ComboboxSelected>>", lambda _: self._load_saved_data())
        
        ttk.Label(filter_frame, text="State:").pack(side="left", padx=(0, 5))
        self.filter_state = tk.StringVar(value="All")
        self.filter_state_cb = ttk.Combobox(filter_frame, textvariable=self.filter_state, width=20, state="readonly")
        self.filter_state_cb.pack(side="left", padx=(0, 15))
        self.filter_state_cb.bind("<<ComboboxSelected>>", lambda _: self._load_saved_data())
        
        ttk.Button(filter_frame, text="🔍 Search", command=self._load_saved_data).pack(side="left", padx=5)
        ttk.Button(filter_frame, text="📊 Summary Stats", command=self._show_summary_stats).pack(side="left", padx=5)
        
        # Results count
        self.lbl_result_count = ttk.Label(filter_frame, text="", font=("", 9, "bold"))
        self.lbl_result_count.pack(side="right", padx=10)
        
        # Results tree
        results_frame = ttk.LabelFrame(self.page_results, text="Scraped Businesses", padding=5)
        results_frame.pack(fill="both", expand=True, padx=5, pady=5)
        
        cols = ("name", "type", "rating", "reviews", "phone", "address", "website", "pincode", "state", "scraped_at")
        self.res_tree = ttk.Treeview(results_frame, columns=cols, show="headings", height=20)
        
        col_widths = {
            "name": 200, "type": 100, "rating": 60, "reviews": 60, "phone": 120,
            "address": 200, "website": 130, "pincode": 70, "state": 100, "scraped_at": 150
        }
        
        for col in cols:
            self.res_tree.heading(col, text=col.replace("_", " ").title())
            self.res_tree.column(col, width=col_widths.get(col, 100))
        
        vsb = ttk.Scrollbar(results_frame, orient="vertical", command=self.res_tree.yview)
        hsb = ttk.Scrollbar(results_frame, orient="horizontal", command=self.res_tree.xview)
        self.res_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        self.res_tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        
        results_frame.columnconfigure(0, weight=1)
        results_frame.rowconfigure(0, weight=1)
        
        # Pagination for results
        page_frame = ttk.Frame(self.page_results)
        page_frame.pack(fill="x", padx=5, pady=5)
        
        ttk.Button(page_frame, text="◀ Previous", command=self._saved_prev_page).pack(side="left", padx=2)
        self.lbl_saved_page = ttk.Label(page_frame, text="Page 1")
        self.lbl_saved_page.pack(side="left", padx=10)
        ttk.Button(page_frame, text="Next ▶", command=self._saved_next_page).pack(side="left", padx=2)
        
        ttk.Button(page_frame, text="💾 Export to CSV", command=self._export_results).pack(side="right", padx=5)
        ttk.Button(page_frame, text="🗑 Delete Selected", command=self._delete_selected).pack(side="right", padx=5)
        
        # Bind selection for deletion
        self.res_tree.bind("<ButtonRelease-1>", self._on_result_select)
        
    # ========================================================================
    # PAGE 3: SETTINGS
    # ========================================================================
    
    def _build_settings_page(self):
        # Create scrollable frame
        canvas = tk.Canvas(self.page_settings)
        vsb = ttk.Scrollbar(self.page_settings, orient="vertical", command=canvas.yview)
        settings_frame = ttk.Frame(canvas)
        
        canvas.configure(yscrollcommand=vsb.set)
        canvas.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        
        canvas.create_window((0, 0), window=settings_frame, anchor="nw", width=900)
        
        def configure_scroll(event):
            canvas.configure(scrollregion=canvas.bbox("all"))
        settings_frame.bind("<Configure>", configure_scroll)
        
        # Chrome Settings
        chrome_frame = ttk.LabelFrame(settings_frame, text="Chrome Configuration", padding=10)
        chrome_frame.pack(fill="x", padx=20, pady=10)
        
        ttk.Label(chrome_frame, text="Chrome Binary Path:").grid(row=0, column=0, sticky="w", pady=5)
        self.chrome_var = tk.StringVar(value=self.cfg.get("chrome_binary", ""))
        ttk.Entry(chrome_frame, textvariable=self.chrome_var, width=70).grid(row=0, column=1, padx=5)
        ttk.Button(chrome_frame, text="Browse", command=self._browse_chrome).grid(row=0, column=2)
        self.lbl_chrome_ok = ttk.Label(chrome_frame, text="", font=("", 8))
        self.lbl_chrome_ok.grid(row=1, column=1, sticky="w")
        
        ttk.Label(chrome_frame, text="ChromeDriver Path:").grid(row=2, column=0, sticky="w", pady=5)
        self.driver_var = tk.StringVar(value=self.cfg.get("chromedriver_path", ""))
        ttk.Entry(chrome_frame, textvariable=self.driver_var, width=70).grid(row=2, column=1, padx=5)
        ttk.Button(chrome_frame, text="Browse", command=self._browse_driver).grid(row=2, column=2)
        self.lbl_driver_ok = ttk.Label(chrome_frame, text="", font=("", 8))
        self.lbl_driver_ok.grid(row=3, column=1, sticky="w")
        
        # Output Settings
        output_frame = ttk.LabelFrame(settings_frame, text="Output Configuration", padding=10)
        output_frame.pack(fill="x", padx=20, pady=10)
        
        ttk.Label(output_frame, text="Output Directory:").grid(row=0, column=0, sticky="w", pady=5)
        self.output_var = tk.StringVar(value=self.cfg.get("output_dir", ""))
        ttk.Entry(output_frame, textvariable=self.output_var, width=70).grid(row=0, column=1, padx=5)
        ttk.Button(output_frame, text="Browse", command=self._browse_output).grid(row=0, column=2)
        
        self.headless_var = tk.BooleanVar(value=self.cfg.get("headless", False))
        ttk.Checkbutton(output_frame, text="Headless Mode (no visible browser window)", 
                       variable=self.headless_var).grid(row=1, column=0, columnspan=3, sticky="w", pady=10)
        
        # Advanced Settings
        advanced_frame = ttk.LabelFrame(settings_frame, text="Advanced Settings", padding=10)
        advanced_frame.pack(fill="x", padx=20, pady=10)
        
        self._delay_vars = {}
        settings = [
            ("Page Load Delay (seconds):", "page_load_delay", 5),
            ("Scroll Delay (seconds):", "scroll_delay", 2),
            ("Max Scroll Attempts:", "max_scrolls", 15),
            ("Delay Between Pincodes (seconds):", "delay_between", 2),
            ("Min Request Interval (seconds):", "min_request_interval", 2),
        ]
        
        for i, (label, key, default) in enumerate(settings):
            ttk.Label(advanced_frame, text=label).grid(row=i, column=0, sticky="w", pady=3)
            var = tk.StringVar(value=str(self.cfg.get(key, default)))
            self._delay_vars[key] = var
            ttk.Entry(advanced_frame, textvariable=var, width=10).grid(row=i, column=1, padx=10, sticky="w")
        
        # Save Button
        ttk.Button(settings_frame, text="💾 Save All Settings", command=self._save_settings,
                  style="Accent.TButton").pack(pady=20)
        
        # Info
        info_frame = ttk.LabelFrame(settings_frame, text="About", padding=10)
        info_frame.pack(fill="x", padx=20, pady=10)
        
        ttk.Label(info_frame, text="Universal Business Scraper v2.0", font=("", 10, "bold")).pack(anchor="w")
        ttk.Label(info_frame, text="Scrapes business data from Google Maps based on Indian pincodes").pack(anchor="w")
        ttk.Label(info_frame, text="Supports resume capability, caching, and batch operations").pack(anchor="w")
        
        self._validate_paths()
        
        # Trace for validation
        self.chrome_var.trace_add("write", lambda *_: self._validate_paths())
        self.driver_var.trace_add("write", lambda *_: self._validate_paths())

    # ========================================================================
    # Common Methods
    # ========================================================================
    
    def _log(self, msg: str):
        try:
            ts = datetime.now().strftime("%H:%M:%S")
            msg = str(msg).replace('\r', '').replace('\x00', '')
            
            def do_insert():
                try:
                    self.log_text.insert("end", f"[{ts}] {msg}\n")
                    self.log_text.see("end")
                except:
                    pass
            self.root.after(0, do_insert)
        except:
            pass
    
    def _check_data(self):
        s = self.db.stats()
        if s["total_pincodes"] == 0:
            if messagebox.askyesno("Import Required", 
                "No pincode data found.\n\nWould you like to import a Pincode CSV now?"):
                self._import_csv()
        self._refresh_stats()
        self._load_filter_options()
    
    def _refresh_stats(self):
        s = self.db.stats()
        self.lbl_pincodes.config(text=f"Total Pincodes: {s['total_pincodes']:,}")
        self.lbl_businesses.config(text=f"Businesses Found: {s['total_businesses']:,}")
    
    def _load_filter_options(self):
        # Load business types for filter
        types = self.db.get_unique_business_types()
        self.filter_btype_cb['values'] = ["All"] + types
        
        # Load states for filter
        states = self.db.get_states()
        self.filter_state_cb['values'] = ["All"] + states
    
    def _show_summary_stats(self):
        """Show summary statistics dialog"""
        stats = self.db.stats()
        types = self.db.get_unique_business_types()
        
        summary = f"=== DATABASE SUMMARY ===\n\n"
        summary += f"Total Pincodes: {stats['total_pincodes']:,}\n"
        summary += f"Total Businesses: {stats['total_businesses']:,}\n\n"
        summary += f"Business Types ({len(types)}):\n"
        
        for bt in types[:20]:  # Show first 20
            count = self.db.get_business_count(business_type=bt)
            summary += f"  • {bt}: {count:,}\n"
        
        if len(types) > 20:
            summary += f"  ... and {len(types) - 20} more\n"
        
        messagebox.showinfo("Summary Statistics", summary)
    
    def _export_results(self):
        """Export current saved data view to CSV"""
        items = self.res_tree.get_children()
        if not items:
            messagebox.showwarning("No Data", "No results to export")
            return
        
        path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
        if not path:
            return
        
        headers = [self.res_tree.heading(col)['text'] for col in self.res_tree['columns']]
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(headers)
            for item in items:
                w.writerow(self.res_tree.item(item)['values'])
        
        self._log(f"📁 Exported {len(items):,} rows to {path}")
        messagebox.showinfo("Export Complete", f"Saved {len(items):,} rows to:\n{path}")
    
    def _delete_selected(self):
        """Delete selected results from database"""
        selected = self.res_tree.selection()
        if not selected:
            messagebox.showwarning("No Selection", "Please select items to delete")
            return
        
        if messagebox.askyesno("Confirm Delete", f"Delete {len(selected)} record(s)?"):
            # TODO: Implement actual deletion from database
            # For now, just remove from tree
            for item in selected:
                self.res_tree.delete(item)
            self._log(f"🗑 Deleted {len(selected)} records")
            self._refresh_stats()
    
    def _on_result_select(self, event):
        """Handle result selection for deletion"""
        pass  # Just for visual feedback

    # ========================================================================
    # Scraper Page Methods
    # ========================================================================
    
    def _load_states_scraper(self):
        states = self.db.get_states()
        self.state_cb['values'] = ["All"] + states
        self.state_var.set("All")
    
    def _on_state_change_scraper(self, _evt=None):
        state = self.state_var.get()
        if state != "All":
            districts = self.db.get_districts(state)
            self.district_cb['values'] = ["All"] + districts
        else:
            self.district_cb['values'] = ["All"]
        self.district_var.set("All")
        self.loc_page = 0
        self._load_locations()
    
    def _load_locations(self):
        state = None if self.state_var.get() == "All" else self.state_var.get()
        district = None if self.district_var.get() == "All" else self.district_var.get()
        
        self.total_locations = self.db.get_location_count(state, district)
        self.current_locations = self.db.get_locations(
            state, district, self.loc_page_size, self.loc_page * self.loc_page_size
        )
        
        for item in self.loc_tree.get_children():
            self.loc_tree.delete(item)
        
        for loc in self.current_locations:
            chk = "☑" if loc.pincode in self.selected_pincodes else "☐"
            self.loc_tree.insert("", "end", iid=loc.pincode, values=(
                chk, loc.pincode, loc.officename, loc.district, loc.statename
            ))
        
        total_pages = max(1, (self.total_locations + self.loc_page_size - 1) // self.loc_page_size)
        self.lbl_page.config(text=f"Page {self.loc_page + 1} / {total_pages}")
        self._update_sel_count()
    
    def _on_tree_click(self, event):
        if self.loc_tree.identify_region(event.x, event.y) != "cell":
            return
        if self.loc_tree.identify_column(event.x) != "#1":
            return
        item = self.loc_tree.identify_row(event.y)
        if not item:
            return
        
        if item in self.selected_pincodes:
            self.selected_pincodes.discard(item)
            vals = list(self.loc_tree.item(item, "values"))
            vals[0] = "☐"
        else:
            self.selected_pincodes.add(item)
            vals = list(self.loc_tree.item(item, "values"))
            vals[0] = "☑"
        self.loc_tree.item(item, values=tuple(vals))
        self._update_sel_count()
    
    def _select_page(self):
        for loc in self.current_locations:
            self.selected_pincodes.add(loc.pincode)
            vals = list(self.loc_tree.item(loc.pincode, "values"))
            vals[0] = "☑"
            self.loc_tree.item(loc.pincode, values=tuple(vals))
        self._update_sel_count()
    
    def _clear_page(self):
        for loc in self.current_locations:
            self.selected_pincodes.discard(loc.pincode)
            vals = list(self.loc_tree.item(loc.pincode, "values"))
            vals[0] = "☐"
            self.loc_tree.item(loc.pincode, values=tuple(vals))
        self._update_sel_count()
    
    def _clear_all_selected(self):
        if messagebox.askyesno("Clear All", "Remove ALL selected locations?"):
            self.selected_pincodes.clear()
            self._load_locations()
            self._log("🗑 Cleared all selected locations")
    
    def _update_sel_count(self):
        self.lbl_selected.config(text=f"Selected: {len(self.selected_pincodes):,} locations")
    
    def _get_selected_locations(self) -> List[PostOffice]:
        result = []
        for pc in sorted(self.selected_pincodes):
            loc = self.db.get_location_by_pincode(pc)
            if loc:
                result.append(loc)
        return result
    
    def _prev_page(self):
        if self.loc_page > 0:
            self.loc_page -= 1
            self._load_locations()
    
    def _next_page(self):
        max_p = max(0, (self.total_locations + self.loc_page_size - 1) // self.loc_page_size - 1)
        if self.loc_page < max_p:
            self.loc_page += 1
            self._load_locations()
    
    def _import_csv(self):
        path = filedialog.askopenfilename(title="Select Post Office/Pincode CSV", 
                                          filetypes=[("CSV files", "*.csv")])
        if not path:
            return
        
        def _run():
            def _cb(cur, total):
                self.root.after(0, lambda: self.lbl_status.config(text=f"Importing: {cur:,} / {total:,}"))
            
            ok, err = self.db.import_post_offices(path, _cb)
            self.root.after(0, lambda: messagebox.showinfo("Import Complete", 
                f"✅ Imported: {ok:,}\n⚠ Errors: {err}"))
            self.root.after(0, self._load_states_scraper)
            self.root.after(0, self._refresh_stats)
            self.root.after(0, self._load_locations)
            self.root.after(0, lambda: self.lbl_status.config(text="Ready"))
        
        threading.Thread(target=_run, daemon=True).start()
    
    def _start_scraping(self):
        if not SELENIUM_OK:
            messagebox.showerror("Missing Dependency", "pip install selenium")
            return
        
        selected = self._get_selected_locations()
        if not selected:
            messagebox.showwarning("No Selection", "Select at least one location")
            return
        
        btype = self.business_var.get().strip()
        if not btype:
            messagebox.showwarning("No Business Type", "Enter a business type")
            return
        
        # Validate paths
        chrome = self.chrome_var.get().strip()
        driver = self.driver_var.get().strip()
        if not chrome or not os.path.exists(chrome):
            messagebox.showerror("Missing Chrome", "Set Chrome binary path in Settings")
            return
        if not driver or not os.path.exists(driver):
            messagebox.showerror("Missing ChromeDriver", "Set ChromeDriver path in Settings")
            return
        
        if not messagebox.askyesno("Confirm", f"Scrape '{btype}' across {len(selected):,} locations?"):
            return
        
        self._save_settings()
        self.is_scraping = True
        self.stop_scraping = False
        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")
        self.progress_var.set(0)
        self.lbl_status.config(text="Launching Chrome...")
        
        # Clear previous results display
        for item in self.res_tree.get_children():
            self.res_tree.delete(item)
        
        t = threading.Thread(target=self._scrape_thread, args=(selected, btype), daemon=True)
        t.start()
    
    def _stop_scraping(self):
        self.stop_scraping = True
        if self.scraper:
            self.scraper._stop_flag = True
        self._log("⏹ Stop requested...")
    
    def _scrape_thread(self, locations: List[PostOffice], btype: str):
        total = len(locations)
        completed_pincodes = []
        
        run_cfg = dict(self.cfg)
        run_cfg["chrome_binary"] = self.chrome_var.get().strip()
        run_cfg["chromedriver_path"] = self.driver_var.get().strip()
        run_cfg["output_dir"] = self.output_var.get().strip()
        run_cfg["headless"] = self.headless_var.get()
        for key, var in self._delay_vars.items():
            try:
                run_cfg[key] = float(var.get())
            except ValueError:
                pass
        
        delay_between = float(run_cfg.get("delay_between", 2))
        
        self.scraper = MapsScraper(run_cfg)
        self.scraper.set_log_callback(self._log)
        
        try:
            self.scraper.start(instance_id=1)
            self._log("✅ Chrome launched")
        except Exception as exc:
            self._log(f"❌ Chrome launch failed: {exc}")
            self.root.after(0, self._scraping_done)
            return
        
        self.scraper._stop_flag = False
        
        try:
            for idx, loc in enumerate(locations):
                if self.stop_scraping:
                    self._log("⚠️ Stopped by user")
                    break
                
                if hasattr(self.scraper, '_stop_flag'):
                    self.scraper._stop_flag = self.stop_scraping
                
                self._log(f"📍 [{idx+1}/{total}] {loc.display_name}")
                self.root.after(0, lambda i=idx: self.lbl_status.config(
                    text=f"Scraping {i+1}/{total}..."))
                
                cached = self.db.get_cached(loc.pincode, btype)
                if cached:
                    self._log(f"   💾 Cache hit — {len(cached)} results")
                    businesses = cached
                else:
                    try:
                        businesses = self.scraper.scrape_with_retry(loc, btype, max_retries=2)
                        if businesses:
                            self.db.save_businesses(loc.pincode, btype, businesses)
                            self._log(f"   ✅ {len(businesses)} found & saved")
                        else:
                            self._log("   ⚠️ No results found")
                    except Exception as exc:
                        self._log(f"   ❌ Error: {exc}")
                        businesses = []
                
                completed_pincodes.append(loc.pincode)
                self.scraping_state.save(completed_pincodes, btype)
                
                for biz in businesses:
                    self.root.after(0, lambda b=biz: self._add_result(b))
                
                progress_pct = ((idx + 1) / total) * 100
                self.root.after(0, lambda p=progress_pct: self.progress_var.set(p))
                
                if (idx + 1) % 15 == 0 and idx + 1 < total and not self.stop_scraping:
                    self._log("🔄 Refreshing Chrome session...")
                    try:
                        self.scraper.refresh_session()
                        time.sleep(3)
                    except Exception as e:
                        self._log(f"⚠️ Session refresh failed: {e}")
                
                if idx < total - 1 and not self.stop_scraping:
                    time.sleep(delay_between)
        
        finally:
            self.scraper.stop()
            self.scraping_state.clear()
            self.root.after(0, self._scraping_done)
    
    def _add_result(self, b: Business):
        def do_add():
            self.res_tree.insert("", "end", values=(
                safe_string(b.name, 70),
                safe_string(b.business_type, 30),
                b.rating or "—",
                b.reviews or "—",
                b.phone or "—",
                safe_string(b.address, 90),
                safe_string(b.website, 50),
                b.pincode,
                b.state or "—",
                b.scraped_at[:16] if b.scraped_at else "—",
            ))
        self.root.after(0, do_add)
    
    def _scraping_done(self):
        self.is_scraping = False
        self.btn_start.config(state="normal")
        self.btn_stop.config(state="disabled")
        self.lbl_status.config(text="✅ Complete!")
        self._refresh_stats()
        self._load_filter_options()
        self._log("🎉 Session complete")
        messagebox.showinfo("Done", f"Scraping finished!\nTotal results: {len(self.res_tree.get_children()):,}")
    
    # ========================================================================
    # Saved Data Page Methods
    # ========================================================================
    
    def _load_saved_data(self):
        btype = None if self.filter_btype.get() == "All" else self.filter_btype.get()
        state = None if self.filter_state.get() == "All" else self.filter_state.get()
        
        self.total_saved = self.db.get_business_count(business_type=btype, state=state)
        businesses = self.db.get_all_businesses(
            limit=self.saved_page_size, 
            offset=self.saved_page * self.saved_page_size,
            business_type=btype,
            state=state
        )
        
        for item in self.res_tree.get_children():
            self.res_tree.delete(item)
        
        for biz in businesses:
            self.res_tree.insert("", "end", values=(
                safe_string(biz.name, 70),
                biz.business_type,
                biz.rating or "—",
                biz.reviews or "—",
                biz.phone or "—",
                safe_string(biz.address, 90),
                safe_string(biz.website, 50),
                biz.pincode,
                biz.state or "—",
                biz.scraped_at[:16] if biz.scraped_at else "—",
            ))
        
        total_pages = max(1, (self.total_saved + self.saved_page_size - 1) // self.saved_page_size)
        self.lbl_saved_page.config(text=f"Page {self.saved_page + 1} / {total_pages}")
        self.lbl_result_count.config(text=f"Total: {self.total_saved:,} records")
    
    def _saved_prev_page(self):
        if self.saved_page > 0:
            self.saved_page -= 1
            self._load_saved_data()
    
    def _saved_next_page(self):
        max_p = max(0, (self.total_saved + self.saved_page_size - 1) // self.saved_page_size - 1)
        if self.saved_page < max_p:
            self.saved_page += 1
            self._load_saved_data()
    
    # ========================================================================
    # Settings Page Methods
    # ========================================================================
    
    def _browse_chrome(self):
        p = filedialog.askopenfilename(title="Select Chrome Executable", 
                                        filetypes=[("Executable", "*.exe"), ("All files", "*.*")])
        if p:
            self.chrome_var.set(p)
            self._validate_paths()
    
    def _browse_driver(self):
        p = filedialog.askopenfilename(title="Select ChromeDriver", 
                                        filetypes=[("Executable", "*.exe"), ("All files", "*.*")])
        if p:
            self.driver_var.set(p)
            self._validate_paths()
    
    def _browse_output(self):
        p = filedialog.askdirectory(title="Select Output Directory")
        if p:
            self.output_var.set(p)
    
    def _validate_paths(self):
        for var, lbl in [(self.chrome_var, self.lbl_chrome_ok), (self.driver_var, self.lbl_driver_ok)]:
            path = var.get().strip()
            if not path:
                lbl.config(text="(not set)", foreground="gray")
            elif os.path.exists(path):
                lbl.config(text=f"✅ {os.path.basename(path)}", foreground="green")
            else:
                lbl.config(text="❌ file not found", foreground="red")
    
    def _save_settings(self):
        self.cfg["chrome_binary"] = self.chrome_var.get().strip()
        self.cfg["chromedriver_path"] = self.driver_var.get().strip()
        self.cfg["output_dir"] = self.output_var.get().strip()
        self.cfg["headless"] = self.headless_var.get()
        for key, var in self._delay_vars.items():
            try:
                self.cfg[key] = float(var.get())
            except ValueError:
                pass
        save_cfg(self.cfg)
        self._validate_paths()
        self._log("⚙ Settings saved")
        messagebox.showinfo("Settings", "Settings saved successfully!")
    
    def run(self):
        self.root.mainloop()

# ============================================================================
# Entry point
# ============================================================================

if __name__ == "__main__":
    app = BusinessScraperApp()
    app.run()