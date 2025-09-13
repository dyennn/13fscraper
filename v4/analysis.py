import sqlite3  # For database access
from tabulate import tabulate  # For pretty-printing query results

DB_FILE = "out/filings.db"  # Path to SQLite database

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


def run_query(choice):
    # Run a selected query and print results in table format
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    title, query = QUERIES[choice]
    print(f"\n=== {title} ===")
    try:
        cur.execute(query)
        rows = cur.fetchall()
        print(tabulate(rows, headers=[d[0] for d in cur.description]))
    except Exception as e:
        print(f"Error running query: {e}")
    con.close()


def menu():
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
            run_query(choice)
        else:
            print("Invalid choice. Try again.")


if __name__ == "__main__":
    # Entry point: show menu for analysis queries
    menu()
