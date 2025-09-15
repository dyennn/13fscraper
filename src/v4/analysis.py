"""
analysis.py - Query and export scraped 13f.info data

- Provides CLI and interactive menu for running built-in SQL queries.
- Supports exporting results to CSV/JSON.
"""
import sqlite3
from tabulate import tabulate
import argparse
import csv
import json
import os

DB_FILE = "out/filings.db"

# Predefined analysis queries for reporting and statistics
QUERIES = {
    "1": ("Total Reports Scraped",
          "SELECT COUNT(DISTINCT ReportLink) AS Reports FROM holdings;"),

    "2": ("Total Holdings Saved",
          "SELECT COUNT(*) AS Holdings FROM holdings;"),

    "3": ("Failed Reports Left",
          "SELECT COUNT(*) AS Failed FROM failed_reports;"),

    "4": ("Log Status Summary",
          "SELECT Status, COUNT(*) AS Count FROM scrape_log GROUP BY Status;"),

    "5": ("Top 10 Most Common Assets", """
        SELECT COALESCE(NULLIF(Symbol, ''), NULLIF(IssuerName, ''), 'UNKNOWN_ASSET') AS Asset,
               COUNT(*) AS Count
        FROM holdings
        GROUP BY Asset
        ORDER BY Count DESC
        LIMIT 10;
    """),

    "6": ("Largest Portfolios by Value", """
        SELECT Manager, SUM(CAST(Value AS INT)) AS TotalValue
        FROM holdings
        GROUP BY Manager
        ORDER BY TotalValue DESC
        LIMIT 10;
    """),

    "7": ("Holdings Count per Quarter", """
        SELECT Quarter, COUNT(*) AS Count
        FROM holdings
        GROUP BY Quarter
        ORDER BY CAST(substr(Quarter, instr(Quarter, ' ') + 1) AS INT) * 10 +
                 CAST(substr(Quarter, 2, 1) AS INT) DESC;
    """),

    "8": ("Top 20 Assets Gaining Popularity (Last 4 Quarters)", """
        WITH qtr_counts AS (
            SELECT COALESCE(NULLIF(Symbol, ''), NULLIF(IssuerName, ''), 'UNKNOWN_ASSET') AS Asset,
                   Quarter,
                   COUNT(DISTINCT Manager) AS ManagersHolding,
                   CAST(substr(Quarter, instr(Quarter, ' ') + 1) AS INT) * 10 +
                   CAST(substr(Quarter, 2, 1) AS INT) AS SortKey
            FROM holdings
            GROUP BY Asset, Quarter
        ),
        recent_qtrs AS (
            SELECT DISTINCT Quarter, SortKey
            FROM qtr_counts
            ORDER BY SortKey DESC
            LIMIT 4
        )
        SELECT Asset, Quarter, ManagersHolding
        FROM qtr_counts
        WHERE Quarter IN (SELECT Quarter FROM recent_qtrs)
        ORDER BY ManagersHolding DESC
        LIMIT 20;
    """),

    "9": ("Quarter-over-Quarter Growth (Last 4 Quarters)", """
        WITH qtr_counts AS (
            SELECT COALESCE(NULLIF(Symbol, ''), NULLIF(IssuerName, ''), 'UNKNOWN_ASSET') AS Asset,
                   Quarter,
                   COUNT(DISTINCT Manager) AS ManagersHolding,
                   CAST(substr(Quarter, instr(Quarter, ' ') + 1) AS INT) * 10 +
                   CAST(substr(Quarter, 2, 1) AS INT) AS SortKey
            FROM holdings
            GROUP BY Asset, Quarter
        ),
        recent_qtrs AS (
            SELECT DISTINCT Quarter, SortKey
            FROM qtr_counts
            ORDER BY SortKey DESC
            LIMIT 4
        )
        SELECT a.Asset, a.Quarter AS CurrentQ, a.ManagersHolding,
               (a.ManagersHolding - COALESCE(b.ManagersHolding, 0)) AS QoQ_Growth
        FROM qtr_counts a
        LEFT JOIN qtr_counts b
          ON a.Asset = b.Asset
         AND b.SortKey = (SELECT MAX(SortKey)
                          FROM recent_qtrs
                          WHERE SortKey < a.SortKey)
        WHERE a.Quarter IN (SELECT Quarter FROM recent_qtrs)
        ORDER BY QoQ_Growth DESC
        LIMIT 20;
    """),

    "10": ("Consistently Growing Assets (Last 6 Quarters)", """
        WITH qtr_counts AS (
            SELECT COALESCE(NULLIF(Symbol, ''), NULLIF(IssuerName, ''), 'UNKNOWN_ASSET') AS Asset,
                   Quarter,
                   COUNT(DISTINCT Manager) AS ManagersHolding,
                   CAST(substr(Quarter, instr(Quarter, ' ') + 1) AS INT) * 10 +
                   CAST(substr(Quarter, 2, 1) AS INT) AS SortKey
            FROM holdings
            GROUP BY Asset, Quarter
        ),
        recent_qtrs AS (
            SELECT DISTINCT Quarter, SortKey
            FROM qtr_counts
            ORDER BY SortKey DESC
            LIMIT 6
        ),
        ranked AS (
            SELECT Asset,
                   MIN(Quarter) AS FirstQ,
                   MAX(Quarter) AS LastQ,
                   MIN(ManagersHolding) AS MinManagers,
                   MAX(ManagersHolding) AS MaxManagers
            FROM qtr_counts
            WHERE Quarter IN (SELECT Quarter FROM recent_qtrs)
            GROUP BY Asset
        )
        SELECT Asset, FirstQ, LastQ, MinManagers, MaxManagers,
               (MaxManagers - MinManagers) AS Growth
        FROM ranked
        ORDER BY Growth DESC
        LIMIT 20;
    """),

    "11": ("Institutional Momentum (Top 10 Rising Assets)", """
        WITH this_q AS (
            SELECT COALESCE(NULLIF(Symbol, ''), NULLIF(IssuerName, ''), 'UNKNOWN_ASSET') AS Asset,
                   COUNT(DISTINCT Manager) AS ManagersNow
            FROM holdings
            WHERE Quarter = (SELECT MAX(Quarter) FROM holdings)
            GROUP BY Asset
        ),
        prev_q AS (
            SELECT COALESCE(NULLIF(Symbol, ''), NULLIF(IssuerName, ''), 'UNKNOWN_ASSET') AS Asset,
                   COUNT(DISTINCT Manager) AS ManagersPrev
            FROM holdings
            WHERE Quarter = (SELECT MAX(Quarter) FROM holdings WHERE Quarter < (SELECT MAX(Quarter) FROM holdings))
            GROUP BY Asset
        )
        SELECT this_q.Asset,
               this_q.ManagersNow,
               COALESCE(prev_q.ManagersPrev, 0) AS ManagersPrev,
               (this_q.ManagersNow - COALESCE(prev_q.ManagersPrev, 0)) AS Change
        FROM this_q
        LEFT JOIN prev_q ON this_q.Asset = prev_q.Asset
        ORDER BY Change DESC
        LIMIT 10;
    """)
}


def run_query(choice, export_csv=None, export_json=None):
    """
    Run a selected query, print results, and optionally export to CSV/JSON.
    """
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    title, query = QUERIES[choice]
    print(f"\n=== {title} ===")
    try:
        cur.execute(query)
        rows = cur.fetchall()
        headers = [d[0] for d in cur.description]
        print(tabulate(rows, headers=headers))
        if export_csv:
            with open(export_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)
            print(f"Exported to CSV: {export_csv}")
        if export_json:
            with open(export_json, 'w', encoding='utf-8') as f:
                json.dump([dict(zip(headers, row)) for row in rows], f, indent=2)
            print(f"Exported to JSON: {export_json}")
    except Exception as e:
        print(f"Error running query: {e}")
    con.close()


def menu():
    parser = argparse.ArgumentParser(
        description="""
        13F Analysis: Query and export scraped 13f.info data.
        
        Examples:
          python analysis.py --query 1 --export-csv reports.csv
          python analysis.py --query 2 --export-json holdings.json
          python analysis.py --help
        """,
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('--query', type=str, help='Query number to run (e.g. 1, 2, 3, ...). See menu for options.')
    parser.add_argument('--export-csv', type=str, help='Export result to CSV file (optional)')
    parser.add_argument('--export-json', type=str, help='Export result to JSON file (optional)')
    args = parser.parse_args()

    if args.query:
        if args.query in QUERIES:
            run_query(args.query, export_csv=args.export_csv, export_json=args.export_json)
        else:
            print("Invalid query number. Run without --query to see menu.")
        return

    # Interactive menu for running analysis queries
    while True:
        print("\n=== Query Menu ===")
        for key, (title, _) in QUERIES.items():
            print(f"{key}. {title}")
        print("0. Exit")

        choice = input("\nEnter choice: ").strip()
        if choice == "0":
            print("Goodbye 👋")
            break
        elif choice in QUERIES:
            export_csv = input("Export to CSV? (filename or blank): ").strip() or None
            export_json = input("Export to JSON? (filename or blank): ").strip() or None
            run_query(choice, export_csv=export_csv, export_json=export_json)
        else:
            print("Invalid choice. Try again.")

if __name__ == "__main__":
    menu()
