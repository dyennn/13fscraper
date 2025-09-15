"""
v4.1_scraper.py - Main orchestration script for 13F Scraper

- Thread-safe: Each thread opens its own SQLite connection for DB writes.
- Highly configurable: YAML, .env, and CLI overrides.
- Progress bar, logging, and robust error handling.

Usage:
    python v4.1_scraper.py --help
"""
from db import init_db, already_scraped
from utils import log, fmt_eta, setup_logger
from scraper import get_manager_links, get_reports, scrape_report
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, List, Tuple, Optional
import logging
import traceback
import sqlite3
import argparse
import os
import yaml
from dotenv import load_dotenv

DEFAULT_MAX_WORKERS = 12
DEFAULT_OUT_DIR = "out"
DEFAULT_LETTERS = [chr(c) for c in range(ord('a'), ord('z')+1)] + ["0"]

# DB file path for per-thread connections

LETTERS_DONE_FILE = os.path.join("out", "letters_done.txt")

def load_config(config_path: str = "v4/config.yaml") -> dict:
    """
    Load configuration from YAML file and environment variables.
    Returns:
        dict: Configuration dictionary.
    """
    load_dotenv()
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    # Allow environment variable overrides
    config["BASE_URL"] = os.getenv("BASE_URL", config["BASE_URL"])
    config["OUT_DIR"] = os.getenv("OUT_DIR", config["OUT_DIR"])
    config["MAX_WORKERS"] = int(os.getenv("MAX_WORKERS", config["MAX_WORKERS"]))
    letters_env = os.getenv("LETTERS")
    if letters_env:
        config["LETTERS"] = [l.strip() for l in letters_env.split(",") if l.strip()]
    return config

def load_letters_done() -> set:
    """Load set of completed letters from out/letters_done.txt."""
    if not os.path.exists(LETTERS_DONE_FILE):
        return set()
    with open(LETTERS_DONE_FILE, "r") as f:
        return set(line.strip() for line in f if line.strip())

def mark_letter_done(letter: str) -> None:
    """Append a completed letter to out/letters_done.txt."""
    with open(LETTERS_DONE_FILE, "a") as f:
        f.write(f"{letter}\n")

def thread_db_write(result: Tuple[str, str, str, List[Tuple[Optional[str], ...]]], db_file: str) -> None:
    url, manager, quarter, holdings = result
    con = sqlite3.connect(db_file)
    try:
        cur = con.cursor()
        if holdings:
            cur.executemany("""INSERT OR IGNORE INTO holdings \
                (Symbol, IssuerName, Class, CUSIP, Value, Percent, Shares, Principal, OptionType, ReportLink, Manager, Quarter) \
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", holdings)
            cur.execute("INSERT INTO scrape_log (ReportLink, Status) VALUES (?, 'scraped')", (url,))
        else:
            cur.execute("""INSERT OR REPLACE INTO failed_reports \
                (ReportLink, Manager, Quarter, Error) VALUES (?, ?, ?, ?)""", (url, manager, quarter, "No data"))
            cur.execute("INSERT INTO scrape_log (ReportLink, Status) VALUES (?, 'failed')", (url,))
        con.commit()
    finally:
        con.close()

def main(max_workers: int, out_dir: str, letters: List[str], log_file: Optional[str]) -> None:
    """
    Main workflow: scrape all managers, save summaries/holdings, log status.
    Only summary info is written by the main thread; all other DB writes are done per-thread.
    """
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    db_file = os.path.join(out_dir, "filings.db")
    setup_logger(log_file)
    start = time.time()
    con = init_db()
    done_reports = already_scraped(con)
    log(f"Resuming: {len(done_reports)} reports already scraped", level=logging.INFO)

    reports_done = 0
    letters_done = load_letters_done()
    for letter in letters:
        if letter in letters_done:
            log(f"Skipping letter {letter.upper()} (already marked done)", level=logging.INFO)
            continue
        log(f"\n=== Processing managers under letter: {letter.upper()} ===", level=logging.INFO)
        managers = get_manager_links(letter)

        for m in managers:
            reports, summaries = get_reports(m)
            if summaries:
                con.executemany("""INSERT INTO summaries \
                    (Manager, Quarter, HoldingsCount, Value, TopHoldings, Form, DateFiled, FilingID, ReportLink) \
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", summaries)
                con.commit()

            manager_reports = [r for r in reports if r[0] not in done_reports]
            if not manager_reports:
                for r in reports:
                    con.execute("INSERT INTO scrape_log (ReportLink, Status) VALUES (?, 'skipped')", (r[0],))
                    log(f"Skipped report (already scraped): {r[0]}", level=logging.INFO)
                con.commit()
                continue

            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = [ex.submit(scrape_report, r) for r in manager_reports]
                for f in as_completed(futures):
                    result = f.result()
                    thread_db_write(result, db_file)
                    url, manager, quarter, holdings = result
                    reports_done += 1
                    elapsed = time.time() - start
                    avg = elapsed / reports_done
                    eta_left = avg * (len(manager_reports) - reports_done)
                    log(f"   Report {reports_done}: {len(holdings)} holdings | "
                        f"Avg {avg:.1f}s | Elapsed {fmt_eta(elapsed)} | ETA {fmt_eta(eta_left)}", level=logging.INFO | {m})

        mark_letter_done(letter)

    con.close()
    log(f"\nAll done. Total time: {fmt_eta(time.time()-start)}", level=logging.INFO)

def parse_letters(letters_arg: str) -> List[str]:
    """
    Parse a string like 'a,b,c,0' or 'a-z' or mixed 'a-c,0' into a list of letters.
    """
    result = []
    for part in letters_arg.split(','):
        part = part.strip()
        if '-' in part and len(part) == 3:
            start, end = part[0], part[2]
            result.extend([chr(c) for c in range(ord(start), ord(end)+1)])
        elif part:
            result.append(part)
    return result

if __name__ == "__main__":
    config = load_config()
    parser = argparse.ArgumentParser(
        description="""
        13F Scraper: Scrape, analyze, and export 13f.info filings.
        
        Examples:
          python v4.1_scraper.py --threads 8 --out out --letters a-c,0 --log scraper.log
          python v4.1_scraper.py --help
        """,
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('--threads', type=int, default=config["MAX_WORKERS"],
                        help='Number of parallel threads (default from config.yaml or .env)')
    parser.add_argument('--out', type=str, default=config["OUT_DIR"],
                        help='Output directory for SQLite DB (default from config.yaml or .env)')
    parser.add_argument('--letters', type=str, default=','.join(str(l) for l in config["LETTERS"]),
                        help="Letters to scrape (e.g. 'a,b,c,0' or 'a-z'). Default: all")
    parser.add_argument('--log', type=str, default=None,
                        help='Log file path (optional, logs also go to console)')
    parser.add_argument('--config', type=str, default="v4/config.yaml",
                        help='Path to config.yaml (default: v4/config.yaml)')
    args = parser.parse_args()
    try:
        main(
            max_workers=args.threads,
            out_dir=args.out,
            letters=parse_letters(args.letters),
            log_file=args.log
        )
    except Exception as e:
        log(f"Uncaught exception: {e}", level=logging.ERROR)
        log(traceback.format_exc(), level=logging.ERROR)
