from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd
import os
import logging
from datetime import datetime # Import datetime for date parsing
import matplotlib.pyplot as plt # Import for visualizations
import seaborn as sns # Import for visualizations
import sys # Import sys to get script path



# -----------------------------
# 13F Scraper Script
# -----------------------------
# This script scrapes 13F filings and holdings data from https://13f.info for all listed managers.
# It handles pagination, dynamic content, and outputs CSV files for analysis.
#
# Usage:
#   1. Run this script to generate summarized_filings_data.csv and detailed_holdings_data.csv.
#   2. Use analysis_and_visualization.py for further analysis and visualization.
# -----------------------------

# Configure logging for info and debugging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



# Base URL of the website to scrape
BASE_URL = "https://13f.info"

# Manager list URLs for A-Z (and 0)
MANAGER_LIST_URLS = [f"{BASE_URL}/managers/{chr(c)}" for c in range(ord('a'), ord('z')+1)] + [f"{BASE_URL}/managers/0"]

# Get the directory where the script is located
if getattr(sys, 'frozen', False):
    script_dir = os.path.dirname(sys.executable)
else:
    script_dir = os.path.dirname(os.path.abspath(__file__))



def get_manager_links_from_list(url):
    """
    Scrape a manager list page (A-Z) to extract links to individual manager pages.
    Returns a list of manager URLs.
    """
    logging.info(f"Scraping manager list page: {url} for manager links...")
    manager_links = []
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        # Find all <a> tags that link to manager pages
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href.startswith('/manager/'):
                from urllib.parse import urljoin
                manager_links.append(urljoin(BASE_URL, href))
        logging.info(f"Found {len(manager_links)} manager links on {url}.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error scraping manager list page {url}: {e}")
        return []
    return manager_links

def get_quarterly_report_links_and_summary(manager_link, base_url):
    """
    For a given manager, scrape all quarterly report links and summarized filing data.
    Handles pagination for multiple filing pages.
    Returns (list of report links, list of summary dicts).
    """
    logging.info(f"Collecting quarterly report links and summary for manager: {manager_link}")
    current_page_url = manager_link
    manager_report_links = []
    manager_filings_summary = []
    page_count = 0

    # Loop through paginated pages until no next page is found
    while current_page_url:
        page_count += 1
        logging.info(f"  Processing page {page_count}: {current_page_url}")
        try:
            manager_response = requests.get(current_page_url)
            manager_response.raise_for_status()
            manager_soup = BeautifulSoup(manager_response.content, 'html.parser')

            # Find the table with id="managerFilings" which contains report links and summary
            filings_table = manager_soup.find('table', {'id': 'managerFilings'})

            if filings_table:
                # Find all rows in the table body (skip header)
                tbody = filings_table.find('tbody')
                if tbody:
                    rows = tbody.find_all('tr')
                    for row in rows:
                        cols = row.find_all('td')
                        # Check if the row has the expected number of columns for summary data (7)
                        if len(cols) == 7:
                             # Extracting text and cleaning up whitespace for each column
                            filing_details = {
                                'Manager Link': manager_link, # Add manager link for context
                                'Quarter': cols[0].text.strip() if len(cols) > 0 else None,
                                'Holdings Count': cols[1].text.strip() if len(cols) > 1 else None,
                                'Value ($000)': cols[2].text.strip() if len(cols) > 2 else None,
                                'Top Holdings Summary': cols[3].text.strip() if len(cols) > 3 else None,
                                'Form Type': cols[4].text.strip() if len(cols) > 4 else None,
                                'Date Filed': cols[5].text.strip() if len(cols) > 5 else None,
                                'Filing ID': cols[6].text.strip() if len(cols) > 6 else None,
                            }
                            manager_filings_summary.append(filing_details)

                            # Extract the report link from the first column
                            first_td = row.find('td')
                            if first_td:
                                a_tag = first_td.find('a', href=True)
                                if a_tag:
                                    report_url = base_url + a_tag['href']
                                    if report_url not in manager_report_links:
                                        manager_report_links.append(report_url)
                        else:
                             logging.warning(f"  Skipping filing summary row on page {current_page_url} due to unexpected number of columns ({len(cols)}).")


                logging.info(f"  Found {len(rows)} filings on page {page_count}.")
            else:
                logging.warning(f"  'managerFilings' table not found on page {current_page_url}.")


            # Find the pagination link to the next page
            next_page_link_tag = manager_soup.find('a', {'rel': 'next'})

            # Update current_page_url to the next page or None if no next page is found
            if next_page_link_tag and 'href' in next_page_link_tag.attrs:
                from urllib.parse import urljoin
                current_page_url = urljoin(base_url, next_page_link_tag['href'])
                logging.info(f"  Found next page: {current_page_url}")
            else:
                current_page_url = None  # No more pages

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {current_page_url}: {e}")
            current_page_url = None  # Stop if there's an error
        except Exception as e:
            logging.error(f"An unexpected error occurred processing {current_page_url}: {e}")
            current_page_url = None

    logging.info(f"Finished collecting reports and summary for {manager_link}. Found a total of {len(manager_report_links)} report links and {len(manager_filings_summary)} summary entries.")
    return manager_report_links, manager_filings_summary

def scrape_detailed_holdings(report_link, driver, max_retries=3):
    """
    Scrape detailed holdings data from a quarterly report page using Selenium.
    Handles table pagination and dynamic content loading.
    Returns a list of holding dicts.
    """
    import io
    import csv
    import tempfile
    import glob
    holdings_data = []
    logging.info(f"  Scraping detailed holdings from report: {report_link}")
    # Try Selenium CSV download first
    try:
        # Set up download directory for scalability
        download_dir = tempfile.mkdtemp(prefix='13f_csv_')
        driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
        driver.execute("send_command", params)
        driver.get(report_link)
        wait = WebDriverWait(driver, 10)  # Reduced wait time for faster response
        try:
            csv_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(translate(., 'CSV', 'csv'), 'csv')]")))
            csv_button.click()
            logging.info(f"  Clicked Download CSV button for {report_link}")
        except Exception as e:
            logging.info(f"  No Download CSV button found for {report_link}: {e}")
            raise Exception("No CSV button")
        # Wait for the CSV file to appear in the download directory
        csv_file = None
        for _ in range(15):  # Wait up to 15 seconds (reduced)
            files = glob.glob(f"{download_dir}/*.csv")
            if files:
                csv_file = files[0]
                break
            time.sleep(0.5)  # Reduced sleep interval
        if not csv_file:
            logging.error(f"  CSV file did not download for {report_link}")
            raise Exception("CSV download failed")
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row['Report Link'] = report_link
                holdings_data.append(row)
        logging.info(f"  Parsed {len(holdings_data)} holdings from downloaded CSV for {report_link}")
        return holdings_data
    except Exception as e:
        logging.warning(f"  Selenium CSV download failed for {report_link}: {e}. Falling back to static HTML and table scraping.")
    # ...existing static HTML and table scraping code...
    # ...existing code...

def clean_numeric(df, column_name):
    """
    Helper function to clean and convert a column to numeric.
    Removes commas and coerces errors to NaN.
    """
    if column_name in df.columns:
        df[column_name] = df[column_name].astype(str).str.replace(',', '', regex=False)
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
    return df

def extract_date_from_report_link(report_link):
    """
    Extracts the quarter and year from a report link URL (e.g., '.../q2-2025')
    and returns the last day of the quarter as a datetime object.
    """
    if isinstance(report_link, str) and len(report_link.split('/')) > 1:
        date_str_proxy = report_link.split('/')[-2]
        try:
            parts = date_str_proxy.lower().split('-')
            if len(parts) == 2 and parts[0].startswith('q'):
                quarter = int(parts[0][1])
                year = int(parts[1])
                month = quarter * 3
                return datetime(year, month, 1) + pd.offsets.MonthEnd(0)
        except (ValueError, IndexError):
            return None
    return None



# -----------------------------
# Main Execution Flow
# -----------------------------





# --- Parallel scraping for managers ---
import concurrent.futures

# Step 1: Collect all manager links (A-Z, deduplicated)
all_manager_links = []
seen_links = set()
for list_url in MANAGER_LIST_URLS:
    manager_links = get_manager_links_from_list(list_url)
    for link in manager_links:
        if link not in seen_links:
            seen_links.add(link)
            all_manager_links.append(link)
            # Removed the limit on the number of manager links
logging.info(f"Total unique manager links collected: {len(all_manager_links)}")

# Step 2: Parallel scrape quarterly report links and summary for each manager
quarterly_report_links = {}
all_managers_summary_data = []

def scrape_manager(manager_link):
    try:
        report_links, filings_summary = get_quarterly_report_links_and_summary(manager_link, BASE_URL)
        return (manager_link, report_links, filings_summary)
    except Exception as e:
        logging.error(f"Error scraping manager {manager_link}: {e}")
        return (manager_link, [], [])

# Use ThreadPoolExecutor for parallel scraping
with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
    future_to_manager = {executor.submit(scrape_manager, link): link for link in all_manager_links}
    for i, future in enumerate(concurrent.futures.as_completed(future_to_manager)):
        manager_link, report_links, filings_summary = future.result()
        quarterly_report_links[manager_link] = report_links
        all_managers_summary_data.extend(filings_summary)
        # Periodically save intermediate summary results
        if (i+1) % 50 == 0:
            temp_summary_file = os.path.join(script_dir, f"summarized_filings_data_partial_{i+1}.csv")
            pd.DataFrame(all_managers_summary_data).to_csv(temp_summary_file, index=False)
            logging.info(f"Intermediate summary saved to {temp_summary_file}")

logging.info(f"Total managers processed: {len(quarterly_report_links)}")


# Convert summarized filings data into a pandas DataFrame
df_managers_summary = pd.DataFrame(all_managers_summary_data)


# Save the summarized filings data to a CSV file
if not df_managers_summary.empty:
    summarized_csv_file_name = os.path.join(script_dir, "summarized_filings_data.csv") # Save in script directory
    df_managers_summary.to_csv(summarized_csv_file_name, index=False)
    logging.info(f"Summarized filings data saved to '{summarized_csv_file_name}'")
else:
    logging.warning("No summarized filings data collected to save.")




# --- Parallel scraping for detailed holdings using Selenium (headless, session reuse) ---


all_detailed_holdings_data = []
total_report_links = sum(len(links) for links in quarterly_report_links.values())

def scrape_report_task(args, driver):
    manager_link, report_link = args
    try:
        detailed_holdings = scrape_detailed_holdings(report_link, driver)
        for holding in detailed_holdings:
            holding['Manager Link'] = manager_link
        return detailed_holdings
    except Exception as e:
        logging.error(f"Error scraping report {report_link}: {e}")
        return []

def run_parallel_detailed_scraping(report_tasks, all_detailed_holdings_data, script_dir):
    max_workers = 4  # Optimized for RTX3050, i5-11400H, 8GB RAM
    logging.info(f"Using {max_workers} parallel Selenium drivers for scraping.")
    def driver_worker(task_chunk):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-dev-shm-usage')
        try:
            s = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=s, options=chrome_options)
        except Exception as e:
            logging.error(f"Failed to start ChromeDriver: {e}")
            return []
        results = []
        for args in task_chunk:
            results.extend(scrape_report_task(args, driver))
        try:
            driver.quit()
        except Exception as e:
            logging.warning(f"Error quitting driver: {e}")
        return results

    # Split report_tasks into chunks for each worker
    chunk_size = (len(report_tasks) + max_workers - 1) // max_workers
    task_chunks = [report_tasks[i:i+chunk_size] for i in range(0, len(report_tasks), chunk_size)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_chunk = {executor.submit(driver_worker, chunk): chunk for chunk in task_chunks}
        i = 0
        for future in concurrent.futures.as_completed(future_to_chunk):
            try:
                detailed_holdings = future.result()
                all_detailed_holdings_data.extend(detailed_holdings)
            except Exception as e:
                logging.error(f"Error in parallel scraping for chunk: {e}")
            i += len(future_to_chunk[future])
            # Save intermediate results every 100 reports
            if (i) % 100 == 0 or (i) >= len(report_tasks):
                temp_detailed_file = os.path.join(script_dir, f"detailed_holdings_data_partial_{i}.csv")
                pd.DataFrame(all_detailed_holdings_data).to_csv(temp_detailed_file, index=False)
                logging.info(f"Intermediate detailed holdings saved to {temp_detailed_file}")

# Prepare report tasks (manager_link, report_link) tuples
report_tasks = []
for manager_link, report_links in quarterly_report_links.items():
    for report_link in report_links:
        report_tasks.append((manager_link, report_link))
        # Removed the limit on the number of report tasks

# Run parallel detailed holdings scraping BEFORE analysis
if report_tasks:
    logging.info(f"Starting detailed holdings scraping for {len(report_tasks)} reports...")
    run_parallel_detailed_scraping(report_tasks, all_detailed_holdings_data, script_dir)
    # Save the detailed holdings data to a CSV file
    detailed_csv_file_name = os.path.join(script_dir, "detailed_holdings_data.csv")
    pd.DataFrame(all_detailed_holdings_data).to_csv(detailed_csv_file_name, index=False)
    logging.info(f"Detailed holdings data saved to '{detailed_csv_file_name}'")
    print(f"Total detailed holdings scraped: {len(all_detailed_holdings_data)}")
else:
    logging.warning("No report tasks found for detailed holdings scraping. Skipping.")


# -----------------------------
# Analysis and Visualization (Reload Data)
# -----------------------------

# Load the data from the CSV files for analysis (ensure latest data is used)
try:
    detailed_csv_file_name = os.path.join(script_dir, "detailed_holdings_data.csv")
    df_detailed_holdings = pd.read_csv(detailed_csv_file_name)
    print("\nSuccessfully loaded detailed holdings data for analysis.")
except FileNotFoundError:
    print("\nError: 'detailed_holdings_data.csv' not found for analysis.")
    df_detailed_holdings = pd.DataFrame() # Create an empty DataFrame

try:
    summarized_csv_file_name = os.path.join(script_dir, "summarized_filings_data.csv")
    df_summarized_filings = pd.read_csv(summarized_csv_file_name)
    print("Successfully loaded summarized filings data for analysis.")
except FileNotFoundError:
    print("Error: 'summarized_filings_data.csv' not found for analysis.")
    df_summarized_filings = pd.DataFrame() # Create an empty DataFrame



# Data Cleaning and Preparation for Analysis
if not df_detailed_holdings.empty:
    # Clean numeric columns
    df_detailed_holdings = clean_numeric(df_detailed_holdings, 'Value ($000)')
    df_detailed_holdings = clean_numeric(df_detailed_holdings, 'Shares') # Clean Shares as well

    # Extract Report Date using the robust function
    if 'Report Link' in df_detailed_holdings.columns:
        df_detailed_holdings['Report Date'] = df_detailed_holdings['Report Link'].apply(extract_date_from_report_link)
    else:
        df_detailed_holdings['Report Date'] = None


# Clean summarized filings data
if not df_summarized_filings.empty:
    df_summarized_filings = clean_numeric(df_summarized_filings, 'Value ($000)')
    if 'Date Filed' in df_summarized_filings.columns:
        df_summarized_filings['Date Filed'] = pd.to_datetime(df_summarized_filings['Date Filed'], errors='coerce')
