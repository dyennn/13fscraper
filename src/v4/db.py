"""
db.py - Database logic for 13F Scraper

Handles database initialization, schema creation, and deduplication helpers.
"""
import sqlite3
import os
from typing import Set, Any

OUT_DIR = "out"
os.makedirs(OUT_DIR, exist_ok=True)
DB_FILE = os.path.join(OUT_DIR, "filings.db")

def init_db() -> sqlite3.Connection:
    """
    Initialize the SQLite database and create required tables and indexes if they do not exist.
    Returns:
        sqlite3.Connection: Database connection object.
    """
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
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_holding
        ON holdings(ReportLink, Symbol)""")
    con.commit()
    return con

def already_scraped(con: sqlite3.Connection) -> Set[Any]:
    """
    Get set of already scraped report URLs to avoid duplicates.
    Args:
        con (sqlite3.Connection): Database connection object.
    Returns:
        Set[Any]: Set of already scraped report URLs.
    """
    cur = con.cursor()
    cur.execute("SELECT DISTINCT ReportLink FROM holdings")
    return {row[0] for row in cur.fetchall()}
