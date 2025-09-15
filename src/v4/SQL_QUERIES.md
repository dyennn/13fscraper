# Useful SQL Queries for `filings.db`

Run these inside the SQLite shell:
```bash
sqlite3 out/filings.db
````

---

## 🔎 Monitoring Scraping Progress

### Count total reports scraped

```sql
SELECT COUNT(DISTINCT ReportLink) FROM holdings;
```

### Count total holdings saved

```sql
SELECT COUNT(*) FROM holdings;
```

### Count failed reports left

```sql
SELECT COUNT(*) FROM failed_reports;
```

### Count log entries by status

```sql
SELECT Status, COUNT(*) FROM scrape_log GROUP BY Status;
```

---

## 🧾 Scrape Log Analysis

### Show last 20 logs

```sql
SELECT * FROM scrape_log ORDER BY Timestamp DESC LIMIT 20;
```

### Show skipped reports

```sql
SELECT * FROM scrape_log WHERE Status='skipped' LIMIT 50;
```

---

## 📊 Holdings & Managers

### Top 10 most commonly held stocks

```sql
SELECT Symbol, COUNT(*) AS Count
FROM holdings
GROUP BY Symbol
ORDER BY Count DESC
LIMIT 10;
```

### Largest portfolio by reported value

```sql
SELECT Manager, SUM(CAST(Value AS INT)) AS TotalValue
FROM holdings
GROUP BY Manager
ORDER BY TotalValue DESC
LIMIT 10;
```

### Holdings count per quarter

```sql
SELECT Quarter, COUNT(*) AS Count
FROM holdings
GROUP BY Quarter
ORDER BY Quarter DESC;
```

---

## 🔁 Failed Reports Recovery

### List failed reports

```sql
SELECT * FROM failed_reports ORDER BY LastTried DESC LIMIT 20;
```

### Retry summary

```sql
SELECT Error, COUNT(*) FROM failed_reports GROUP BY Error;
```

---

## 🔮 Predictive Analysis Cheat Sheet

### Stocks gaining institutional popularity (last 4 quarters)

```sql
SELECT Symbol, COUNT(DISTINCT Manager) AS ManagersHolding, Quarter
FROM holdings
WHERE Quarter IN (
    SELECT DISTINCT Quarter FROM holdings ORDER BY Quarter DESC LIMIT 4
)
GROUP BY Symbol, Quarter
ORDER BY Symbol, Quarter;
```

### Quarter-over-quarter growth in manager adoption

```sql
WITH qtr_counts AS (
    SELECT Symbol, Quarter, COUNT(DISTINCT Manager) AS ManagersHolding
    FROM holdings
    GROUP BY Symbol, Quarter
)
SELECT a.Symbol, a.Quarter AS CurrentQ, a.ManagersHolding,
       (a.ManagersHolding - b.ManagersHolding) AS QoQ_Growth
FROM qtr_counts a
LEFT JOIN qtr_counts b
  ON a.Symbol = b.Symbol
 AND a.Quarter = (SELECT MAX(Quarter) FROM holdings WHERE Quarter < a.Quarter)
ORDER BY QoQ_Growth DESC
LIMIT 20;
```

### Most consistently growing stocks over last 6 quarters

```sql
WITH qtr_counts AS (
    SELECT Symbol, Quarter, COUNT(DISTINCT Manager) AS ManagersHolding
    FROM holdings
    GROUP BY Symbol, Quarter
),
ranked AS (
    SELECT Symbol,
           MIN(Quarter) AS FirstQ,
           MAX(Quarter) AS LastQ,
           MIN(ManagersHolding) AS MinManagers,
           MAX(ManagersHolding) AS MaxManagers
    FROM qtr_counts
    GROUP BY Symbol
)
SELECT Symbol, FirstQ, LastQ, MinManagers, MaxManagers,
       (MaxManagers - MinManagers) AS Growth
FROM ranked
ORDER BY Growth DESC
LIMIT 20;
```

### Institutional momentum: top 10 rising stocks this quarter

```sql
WITH this_q AS (
    SELECT Symbol, COUNT(DISTINCT Manager) AS ManagersNow
    FROM holdings
    WHERE Quarter = (SELECT MAX(Quarter) FROM holdings)
    GROUP BY Symbol
),
prev_q AS (
    SELECT Symbol, COUNT(DISTINCT Manager) AS ManagersPrev
    FROM holdings
    WHERE Quarter = (SELECT MAX(Quarter) FROM holdings WHERE Quarter < (SELECT MAX(Quarter) FROM holdings))
    GROUP BY Symbol
)
SELECT this_q.Symbol,
       this_q.ManagersNow,
       COALESCE(prev_q.ManagersPrev, 0) AS ManagersPrev,
       (this_q.ManagersNow - COALESCE(prev_q.ManagersPrev, 0)) AS Change
FROM this_q
LEFT JOIN prev_q ON this_q.Symbol = prev_q.Symbol
ORDER BY Change DESC
LIMIT 10;
```

## 📜 `run_queries.py`

A Python helper that executes **all queries** (monitoring + predictive) against `filings.db` and prints the results.
