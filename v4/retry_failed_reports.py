import sqlite3, time, requests
from bs4 import BeautifulSoup

BASE = "https://13f.info"
DB_FILE = "out/filings.db"

session = requests.Session()

def log(msg): print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def get_holdings(report_url, manager, quarter, retries=3):
    try:
        r = session.get(report_url, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"   ERROR fetching {report_url}: {e}")
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find(id="filingAggregated")
    if not table: return []
    data_url = table.get("data-url")
    if data_url:
        if data_url.startswith("/"): data_url = BASE + data_url
        for attempt in range(retries):
            try:
                resp = session.get(data_url, timeout=30)
                resp.raise_for_status()
                j = resp.json()
                return [tuple(str(v) for v in row)+(report_url, manager, quarter) for row in j.get("data",[])]
            except Exception as e:
                log(f"   Retry {attempt+1}: {e}"); time.sleep(2**attempt)
    rows=[]; body=table.find("tbody")
    if body:
        headers=[th.get_text(strip=True) for th in table.find_all("th")]
        for tr in body.find_all("tr"):
            cells=[td.get_text(strip=True) for td in tr.find_all("td")]
            if cells:
                rec=dict(zip(headers,cells))
                rows.append((rec.get("Symbol"), rec.get("Issuer Name"), rec.get("Class"), rec.get("CUSIP"),
                             rec.get("Value ($000)"), rec.get("Percent"), rec.get("Shares"),
                             rec.get("Principal"), rec.get("Option Type"), report_url, manager, quarter))
    return rows

if __name__=="__main__":
    con=sqlite3.connect(DB_FILE)
    cur=con.cursor()
    failed=cur.execute("SELECT ReportLink, Manager, Quarter FROM failed_reports").fetchall()
    log(f"Retrying {len(failed)} failed reports...")

    for url,manager,quarter in failed:
        holdings=get_holdings(url,manager,quarter)
        if holdings:
            cur.executemany("""INSERT OR IGNORE INTO holdings 
                (Symbol, IssuerName, Class, CUSIP, Value, Percent, Shares, Principal, OptionType, ReportLink, Manager, Quarter) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", holdings)
            cur.execute("DELETE FROM failed_reports WHERE ReportLink=?", (url,))
            cur.execute("INSERT INTO scrape_log (ReportLink, Status) VALUES (?, 'scraped')",(url,))
            log(f"Recovered {len(holdings)} holdings from {url}")
        else:
            cur.execute("UPDATE failed_reports SET LastTried=CURRENT_TIMESTAMP WHERE ReportLink=?", (url,))
            cur.execute("INSERT INTO scrape_log (ReportLink, Status) VALUES (?, 'failed')",(url,))
            log(f"Still failed: {url}")
        con.commit()
    con.close()
    log("Retry finished")
