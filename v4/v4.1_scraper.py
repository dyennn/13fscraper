import requests, os, time, sqlite3  # Core libraries for HTTP, DB
from bs4 import BeautifulSoup  # HTML parsing
from concurrent.futures import ThreadPoolExecutor, as_completed  # Threaded scraping

BASE = "https://13f.info"  # Base URL for scraping
LETTERS = [chr(c) for c in range(ord('a'), ord('z')+1)] + ["0"]  # Manager index letters

OUT_DIR = "out"  # Output directory for DB
os.makedirs(OUT_DIR, exist_ok=True)
DB_FILE = os.path.join(OUT_DIR, "filings.db")  # SQLite DB file

MAX_WORKERS = 12  # Thread count for parallel scraping

def log(msg): 
    # Timestamped logger for progress and errors
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def fmt_eta(seconds):
    # Format seconds as HH:MM:SS for ETA display
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

##########################
# Database Initialization #
##########################
def init_db():
    # Create all required tables and unique index for deduplication
    con = sqlite3.connect(DB_FILE, check_same_thread=False)
    cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS summaries (
        Manager TEXT, Quarter TEXT, HoldingsCount INTEGER,
        Value TEXT, TopHoldings TEXT, Form TEXT,
        DateFiled TEXT, FilingID TEXT, ReportLink TEXT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS holdings (
        Symbol TEXT, IssuerName TEXT, Class TEXT, CUSIP TEXT,
        Value TEXT, Percent TEXT, Shares TEXT,
        Principal TEXT, OptionType TEXT,
        ReportLink TEXT, Manager TEXT, Quarter TEXT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS failed_reports (
        ReportLink TEXT PRIMARY KEY,
        Manager TEXT,
        Quarter TEXT,
        Error TEXT,
        LastTried TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS scrape_log (
        ReportLink TEXT,
        Status TEXT,
        Timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    # Prevent duplicate holdings per report+symbol
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_holding
        ON holdings(ReportLink, Symbol)""")
    con.commit()
    return con

def already_scraped(con):
    # Get set of already scraped report URLs to avoid duplicates
    cur = con.cursor()
    cur.execute("SELECT DISTINCT ReportLink FROM holdings")
    return {row[0] for row in cur.fetchall()}

# ---------------- Scraper ----------------
session = requests.Session()  # Persistent HTTP session for efficiency

def get_manager_links(letter):
    # Scrape all manager links for a given letter
    links = []
    r = session.get(f"{BASE}/managers/{letter}")
    s = BeautifulSoup(r.text, "html.parser")
    for a in s.select("a[href^='/manager/']"):
        links.append(BASE + a['href'])
    log(f"Collected {len(links)} managers for letter {letter.upper()}")
    return sorted(set(links))

def get_reports(manager_url):
    # For a manager, scrape all filings (reports) and summary info
    reports, summaries = [], []
    url = manager_url
    while url:
        s = BeautifulSoup(session.get(url).text, "html.parser")
        table = s.find("table", id="managerFilings")
        if table:
            for row in table.select("tbody tr"):
                cols = [c.get_text(strip=True) for c in row.find_all("td")]
                if len(cols) == 7:
                    # Save summary info for each filing
                    summaries.append((
                        manager_url, cols[0], cols[1], cols[2],
                        cols[3], cols[4], cols[5], cols[6], None
                    ))
                    # Save report link for scraping holdings
                    a = row.find("a", href=True)
                    if a: reports.append((BASE + a['href'], manager_url, cols[0]))
        nxt = s.find("a", rel="next")
        url = BASE + nxt['href'] if nxt else None
    return reports, summaries

def get_holdings(report_url, manager, quarter, retries=2):
    # Scrape holdings for a given report. Try JSON endpoint first, fallback to HTML table.
    try:
        r = session.get(report_url, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"   ERROR fetching report page {report_url}: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find(id="filingAggregated")
    if not table:
        return []

    data_url = table.get("data-url")
    if data_url:
        if data_url.startswith("/"): 
            data_url = BASE + data_url
        for attempt in range(retries):
            try:
                resp = session.get(data_url, timeout=30)
                resp.raise_for_status()
                j = resp.json()
                # Return holdings as tuples for DB insert
                return [
                    tuple(str(v) for v in row) + (report_url, manager, quarter)
                    for row in j.get("data", [])
                ]
            except Exception as e:
                log(f"   Failed JSON fetch ({attempt+1}/{retries}) at {data_url}: {e}")

    # fallback HTML parsing if JSON fails
    rows = []
    body = table.find("tbody")
    if body:
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        for tr in body.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if cells:
                rec = dict(zip(headers, cells))
                rows.append((
                    rec.get("Symbol"), rec.get("Issuer Name"), rec.get("Class"), rec.get("CUSIP"),
                    rec.get("Value ($000)"), rec.get("Percent"), rec.get("Shares"), 
                    rec.get("Principal"), rec.get("Option Type"), report_url, manager, quarter
                ))
    return rows

def scrape_report(report):
    # Scrape holdings for a report and update DB/logs accordingly
    url, manager, quarter = report
    holdings = get_holdings(url, manager, quarter)
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    if holdings:
        # Insert holdings and mark as scraped
        cur.executemany("""INSERT OR IGNORE INTO holdings 
            (Symbol, IssuerName, Class, CUSIP, Value, Percent, Shares, Principal, OptionType, ReportLink, Manager, Quarter) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", holdings)
        cur.execute("INSERT INTO scrape_log (ReportLink, Status) VALUES (?, 'scraped')", (url,))
    else:
        # Mark as failed if no holdings found
        cur.execute("""INSERT OR REPLACE INTO failed_reports 
            (ReportLink, Manager, Quarter, Error) VALUES (?, ?, ?, ?)""", (url, manager, quarter, "No data"))
        cur.execute("INSERT INTO scrape_log (ReportLink, Status) VALUES (?, 'failed')", (url,))
    con.commit()
    con.close()
    return url, holdings

# ---------------- Main Script ----------------
if __name__ == "__main__":
    # Main workflow: scrape all managers, save summaries/holdings, log status
    start = time.time()
    con = init_db()
    done_reports = already_scraped(con)
    log(f"Resuming: {len(done_reports)} reports already scraped")

    reports_done = 0
    for letter in LETTERS:
        log(f"\n=== Processing managers under letter: {letter.upper()} ===")
        managers = get_manager_links(letter)

        for m in managers:
            reports, summaries = get_reports(m)
            if summaries:
                # Save summary info for manager filings
                con.executemany("""INSERT INTO summaries 
                    (Manager, Quarter, HoldingsCount, Value, TopHoldings, Form, DateFiled, FilingID, ReportLink) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", summaries)
                con.commit()

            manager_reports = [r for r in reports if r[0] not in done_reports]
            if not manager_reports:
                # Log skipped reports
                for r in reports:
                    con.execute("INSERT INTO scrape_log (ReportLink, Status) VALUES (?, 'skipped')", (r[0],))
                    log(f"Skipped report (already scraped): {r[0]}")
                con.commit()
                continue

            # Scrape all new reports in parallel
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futures = [ex.submit(scrape_report, r) for r in manager_reports]
                for f in as_completed(futures):
                    url, holdings = f.result()
                    reports_done += 1
                    elapsed = time.time() - start
                    avg = elapsed / reports_done
                    eta_left = avg * (len(manager_reports) - reports_done)
                    log(f"   Report {reports_done}: {len(holdings)} holdings | "
                        f"Avg {avg:.1f}s | Elapsed {fmt_eta(elapsed)} | ETA {fmt_eta(eta_left)}")

    con.close()
    log(f"\nAll done. Total time: {fmt_eta(time.time()-start)}")
