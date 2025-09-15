"""
scraper.py - Scraping logic for 13F Scraper

Contains all HTTP, HTML, and JSON parsing logic for scraping manager links, filings, and holdings from 13f.info.
Functions are robust to network errors and log all failures.
"""
import requests
from bs4 import BeautifulSoup
from typing import List, Tuple, Optional
from utils import log
import logging
import json

BASE = "https://13f.info"
session = requests.Session()

def get_manager_links(letter: str) -> List[str]:
    """
    Scrape all manager links for a given letter.
    Args:
        letter (str): Letter to scrape managers for.
    Returns:
        List[str]: List of manager URLs.
    """
    links = []
    try:
        r = session.get(f"{BASE}/managers/{letter}")
        r.raise_for_status()
    except requests.RequestException as e:
        log(f"Failed to fetch manager list for letter {letter}: {e}", level=logging.ERROR)
        return []
    s = BeautifulSoup(r.text, "html.parser")
    for a in s.select("a[href^='/manager/']"):
        links.append(BASE + a['href'])
    log(f"Collected {len(links)} managers for letter {letter.upper()}", level=logging.INFO)
    return sorted(set(links))

def get_reports(manager_url: str) -> Tuple[List[Tuple[str, str, str]], List[Tuple[str, ...]]]:
    """
    For a manager, scrape all filings (reports) and summary info.
    Args:
        manager_url (str): Manager URL.
    Returns:
        Tuple[List[Tuple[str, str, str]], List[Tuple[str, ...]]]: (reports, summaries)
    """
    reports, summaries = [], []
    url = manager_url
    while url:
        try:
            resp = session.get(url)
            resp.raise_for_status()
        except requests.RequestException as e:
            log(f"Failed to fetch filings for manager {manager_url}: {e}", level=logging.ERROR)
            break
        s = BeautifulSoup(resp.text, "html.parser")
        table = s.find("table", id="managerFilings")
        if table:
            for row in table.select("tbody tr"):
                cols = [c.get_text(strip=True) for c in row.find_all("td")]
                if len(cols) == 7:
                    summaries.append((
                        manager_url, cols[0], cols[1], cols[2],
                        cols[3], cols[4], cols[5], cols[6], None
                    ))
                    a = row.find("a", href=True)
                    if a:
                        reports.append((BASE + a['href'], manager_url, cols[0]))
        nxt = s.find("a", rel="next")
        url = BASE + nxt['href'] if nxt else None
    return reports, summaries

def get_holdings(report_url: str, manager: str, quarter: str, retries: int = 2) -> List[Tuple[Optional[str], ...]]:
    """
    Scrape holdings for a given report. Try JSON endpoint first, fallback to HTML table.
    Args:
        report_url (str): Report URL.
        manager (str): Manager URL.
        quarter (str): Quarter string.
        retries (int): Number of retries for JSON endpoint.
    Returns:
        List[Tuple[Optional[str], ...]]: List of holdings tuples.
    """
    try:
        r = session.get(report_url, timeout=20)
        r.raise_for_status()
    except requests.RequestException as e:
        log(f"   ERROR fetching report page {report_url}: {e}", level=logging.ERROR)
        return []
    except Exception as e:
        log(f"   Unexpected error fetching report page {report_url}: {e}", level=logging.ERROR)
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
                return [
                    tuple(str(v) for v in row) + (report_url, manager, quarter)
                    for row in j.get("data", [])
                ]
            except requests.RequestException as e:
                log(f"   Failed JSON fetch ({attempt+1}/{retries}) at {data_url}: {e}", level=logging.ERROR)
            except json.JSONDecodeError as e:
                log(f"   JSON decode error at {data_url}: {e}", level=logging.ERROR)
            except Exception as e:
                log(f"   Unexpected error fetching JSON at {data_url}: {e}", level=logging.ERROR)
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

def scrape_report(report: Tuple[str, str, str]) -> Tuple[str, str, str, List[Tuple[Optional[str], ...]]]:
    """
    Scrape holdings for a report and return the results.
    Args:
        report (Tuple[str, str, str]): (report_url, manager, quarter)
    Returns:
        Tuple[str, str, str, List[Tuple[Optional[str], ...]]]: (url, manager, quarter, holdings)
    """
    try:
        url, manager, quarter = report
        holdings = get_holdings(url, manager, quarter)
        return url, manager, quarter, holdings
    except Exception as e:
        log(f"Error in scrape_report for {report}: {e}", level=logging.ERROR)
        import traceback
        log(traceback.format_exc(), level=logging.ERROR)
        return report[0], report[1], report[2], []
