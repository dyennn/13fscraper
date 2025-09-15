
# 13F Scraper

A robust, modular, and configurable Python tool to scrape, analyze, and export institutional holdings data from [13f.info](https://13f.info). Data is saved to a local SQLite database and can be exported to CSV/JSON for further analysis.

---

## 🚀 Features
- **Parallel scraping** with thread-safe DB writes
- **Resume support** (skips already scraped reports)
- **Configurable** via YAML, .env, and CLI
- **Export** query results to CSV/JSON
- **Automated analysis** with built-in SQL queries
- **Logging** to console and optional file
- **Pip-installable** with `setup.py`

---

## 🗂️ Codebase Structure

- **v4.1_scraper.py**: Main orchestrator. Scrapes managers, filings, and holdings. Highly configurable via CLI, YAML, and .env.
- **scraper.py**: All scraping logic (manager links, filings, holdings). Handles HTTP and HTML/JSON parsing.
- **db.py**: Database initialization and deduplication helpers.
- **utils.py**: Logging, ETA formatting, and logger setup.
- **analysis.py**: Query and export data from the database. CLI and interactive menu for built-in SQL analysis.
- **retry_failed_reports.py**: Retry scraping for failed reports.
- **config.yaml**: Default configuration (can be overridden by .env or CLI).
- **requirements.txt**: All dependencies.
- **setup.py**: For pip installation and CLI entry points.

---

## ⚙️ Installation

```bash
# Clone the repo
pip install -r requirements.txt
# (Optional) Install as a package
pip install .
```

---

## 🛠️ Configuration

- **config.yaml**: Main config (BASE_URL, OUT_DIR, MAX_WORKERS, LETTERS)
- **.env**: (Optional) Override any config value with environment variables
- **CLI**: All config values can be overridden via command-line arguments

---

## 🏃 Usage

### Scraping
```bash
python v4/v4.1_scraper.py --threads 8 --out out --letters a-c,0 --log scraper.log
python v4/v4.1_scraper.py --help  # See all options
```

### Analysis & Export
```bash
python v4/analysis.py --query 1 --export-csv reports.csv
python v4/analysis.py --query 2 --export-json holdings.json
python v4/analysis.py --help  # See all options
```

### Retry Failed Reports
```bash
python v4/retry_failed_reports.py
```

---

## 📊 Database Structure

- **summaries**: Summary info for each manager and quarter
- **holdings**: Individual stock holdings per report
- **failed_reports**: Reports that failed scraping
- **scrape_log**: Audit log of scraping activity

---

## 🧑‍💻 Developer Notes

- All modules are documented with docstrings and inline comments.
- Thread safety: Each thread opens its own SQLite connection for DB writes.
- Configuration precedence: CLI > .env > config.yaml
- Extend analysis by adding SQL queries to `QUERIES` in `analysis.py`.
- See each module for detailed documentation and function/class descriptions.

---

## 📂 File Overview

- **v4.1_scraper.py**: Main CLI scraper (see top of file for docstring)
- **scraper.py**: Scraping logic (see top of file for docstring)
- **db.py**: DB helpers (see top of file for docstring)
- **utils.py**: Logging and utilities (see top of file for docstring)
- **analysis.py**: Query/export CLI (see top of file for docstring)
- **retry_failed_reports.py**: Retry helper (see top of file for docstring)
- **config.yaml**: Default config
- **requirements.txt**: Dependencies
- **setup.py**: Packaging

---

## 📝 Example CLI Usage

```bash
# Scrape all managers (default config)
python v4/v4.1_scraper.py

# Scrape only A, B, C, and 0 managers, 8 threads, log to file
python v4/v4.1_scraper.py --threads 8 --letters a,b,c,0 --log scraper.log

# Export total reports scraped to CSV
python v4/analysis.py --query 1 --export-csv reports.csv

# See all CLI options
python v4/v4.1_scraper.py --help
python v4/analysis.py --help
```

---

## 🧩 Extending & Contributing
- Add new SQL queries to `analysis.py` for more insights
- Add new scraping logic to `scraper.py` as needed
- PRs and issues welcome!