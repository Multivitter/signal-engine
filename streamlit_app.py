"""
Google Search Collector → PostgreSQL
Тянем органику + форумы + похожие запросы
"""

import os
import requests
import psycopg2
from datetime import datetime
import schedule
import time

API_KEY      = os.getenv("SCRAPINGDOG_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

QUERIES = {
    "crypto": [
        "solana smart money whale wallet",
        "solana token pump signal on-chain",
        "solana new token launch whale buy",
    ],
    "amazon": [
        "amazon FBA best seller rank drop 2026",
        "amazon product ranking algorithm 2026",
        "FBA competitor out of stock opportunity",
    ],
    "geopolitics": [
        "geopolitical risk markets 2026",
        "US China trade war impact crypto",
        "Iran sanctions bitcoin price effect",
    ],
    "ai_tech": [
        "AI crypto trading signal 2026",
        "OpenAI GPT market impact 2026",
        "Nvidia AI chip demand market 2026",
    ],
    "macro": [
        "Federal Reserve interest rates crypto 2026",
        "inflation CPI bitcoin correlation 2026",
        "dollar index DXY crypto market 2026",
    ],
    "regulations": [
        "SEC crypto regulation news 2026",
        "Bitcoin ETF regulatory update 2026",
        "MiCA Europe crypto compliance 2026",
    ],
    "ecommerce": [
        "TikTok Shop vs Amazon seller 2026",
        "Shopify ecommerce trends 2026",
        "dropshipping winning products 2026",
    ],
}

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def create_tables():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS google_results (
            id          SERIAL PRIMARY KEY,
            query       TEXT,
            category    VARCHAR(20),
            result_type VARCHAR(30),
            position    INTEGER,
            title       TEXT,
            url         TEXT,
            snippet     TEXT,
            source      VARCHAR(200),
            scraped_at  TIMESTAMP DEFAULT NOW(),
            UNIQUE(query, url)
        );
        CREATE INDEX IF NOT EXISTS idx_google_category ON google_results(category);
        CREATE INDEX IF NOT EXISTS idx_google_scraped  ON google_results(scraped_at);
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Таблица google_results готова")

def fetch_google(query: str) -> dict:
    params = {
        "api_key": API_KEY,
        "query":   query,
        "country": "us",
        "results": "10",
        "page":    "0",
    }
    try:
        r = requests.get("https://api.scrapingdog.com/google", params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        print(f"⚠️  Google API {r.status_code}: {r.text[:300]}")
        return {}
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return {}

def save_results(data: dict, query: str, category: str) -> int:
    if not data:
        return 0
    conn = get_conn()
    cur  = conn.cursor()
    saved = 0

    def insert(result_type, position, title, url, snippet, source=""):
        nonlocal saved
        try:
            cur.execute("""
                INSERT INTO google_results (query, category, result_type, position, title, url, snippet, source)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (query, url) DO UPDATE SET
                    position=EXCLUDED.position, snippet=EXCLUDED.snippet, scraped_at=NOW()
            """, (query, category, result_type, position, title, url, snippet[:500], source))
            saved += 1
        except Exception as e:
            conn.rollback()

    for i, r in enumerate(data.get("organic_results", []), 1):
        insert("organic", i, r.get("title",""), r.get("link",""), r.get("snippet",""))
    for i, r in enumerate(data.get("discussion_and_forums", []), 1):
        insert("forum", i, r.get("title",""), r.get("link",""), r.get("snippet",""), r.get("source",""))
    for i, r in enumerate(data.get("peopleAlsoAskedFor", []), 1):
        insert("question", i, r.get("question",""), r.get("link",""), r.get("snippet",""))
    for i, r in enumerate(data.get("relatedSearches", []), 1):
        q = r.get("query","") if isinstance(r, dict) else str(r)
        insert("related", i, q, f"related:{q}", q)

    conn.commit()
    cur.close()
    conn.close()
    return saved

def collect_all():
    print(f"\n🚀 Google сбор: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    total = 0
    for category, queries in QUERIES.items():
        for query in queries:
            print(f"  🔍 [{category}] {query[:50]}...")
            data = fetch_google(query)
            n    = save_results(data, query, category)
            print(f"     ✅ {n} результатов")
            total += n
            time.sleep(2)
    print(f"\n✅ Итого: {total} записей")

def check_database():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT category, result_type, COUNT(*) FROM google_results GROUP BY category, result_type ORDER BY category")
    rows = cur.fetchall()
    print(f"\n{'Категория':<12} {'Тип':<12} {'Кол-во':>8}")
    print("-"*35)
    for row in rows:
        print(f"{row[0]:<12} {row[1]:<12} {row[2]:>8}")
    cur.close()
    conn.close()

if __name__ == "__main__":
    import sys
    create_tables()
    if len(sys.argv) > 1 and sys.argv[1] == "check":
        check_database()
    elif len(sys.argv) > 1 and sys.argv[1] == "schedule":
        collect_all()
        schedule.every(6).hours.do(collect_all)
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        collect_all()
        check_database()
