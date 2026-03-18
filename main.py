from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import feedparser, sqlite3
from datetime import datetime
import re

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

RSS_FEEDS = [
    "https://www.investing.com/rss/news.rss",
    "https://www.investing.com/rss/news_25.rss",
    "https://www.investing.com/rss/news_14.rss",
    "https://feeds.marketwatch.com/marketwatch/topstories/",
    "https://feeds.marketwatch.com/marketwatch/marketpulse/",
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "https://www.cnbc.com/id/10000664/device/rss/rss.html",
    "https://www.cnbc.com/id/20910258/device/rss/rss.html",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^GSPC&region=US&lang=en-US",
    "https://www.forexlive.com/feed/news",
    "https://www.fxstreet.com/rss/news",
]

HIGH_KEYWORDS = [
    "fed", "federal reserve", "interest rate", "rate decision", "rate hike", "rate cut",
    "ecb", "european central bank", "boe", "bank of england", "boj", "bank of japan",
    "cpi", "inflation", "nfp", "non-farm", "gdp", "unemployment",
    "war", "attack", "invasion", "sanction", "crisis",
    "opec", "oil shock", "recession", "default", "crash",
    "powell", "lagarde", "yellen", "emergency"
]

MEDIUM_KEYWORDS = [
    "earnings", "gdp", "trade", "deficit", "surplus", "pmi", "retail sales",
    "housing", "jobs", "employment", "manufacturing", "output",
    "central bank", "monetary", "fiscal", "stimulus", "tariff",
    "ipo", "merger", "acquisition", "bankruptcy"
]

ASSET_KEYWORDS = {
    "USD": ["dollar", "usd", "fed", "federal reserve", "us economy", "nfp", "cpi us"],
    "EUR": ["euro", "eur", "ecb", "european", "eurozone", "germany", "france"],
    "GBP": ["pound", "gbp", "boe", "britain", "uk economy", "sterling"],
    "GOLD": ["gold", "xau", "bullion", "safe haven"],
    "OIL": ["oil", "crude", "opec", "brent", "wti", "energy", "petroleum"],
    "SP500": ["s&p", "sp500", "nasdaq", "dow", "wall street", "stocks", "equities"],
    "BTC": ["bitcoin", "btc", "crypto", "ethereum", "digital asset"],
    "JPY": ["yen", "jpy", "boj", "japan", "nikkei"],
}

DB_FILE = "news.db"

conn = sqlite3.connect(DB_FILE)
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS news (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    source TEXT,
    headline_en TEXT,
    headline_pt TEXT,
    summary_en TEXT,
    summary_pt TEXT,
    impact TEXT,
    assets TEXT,
    url TEXT
)''')
conn.commit()
conn.close()

def classify_impact(headline):
    h = headline.lower()
    for kw in HIGH_KEYWORDS:
        if kw in h:
            return "HIGH"
    for kw in MEDIUM_KEYWORDS:
        if kw in h:
            return "MEDIUM"
    return "LOW"

def detect_assets(headline):
    h = headline.lower()
    found = []
    for asset, keywords in ASSET_KEYWORDS.items():
        for kw in keywords:
            if kw in h:
                found.append(asset)
                break
    return ",".join(found) if found else "USD"

def get_source_name(url):
    if "investing.com" in url: return "investing.com"
    if "marketwatch" in url: return "marketwatch"
    if "cnbc.com" in url: return "cnbc"
    if "yahoo" in url: return "yahoo finance"
    if "forexlive" in url: return "forexlive"
    if "fxstreet" in url: return "fxstreet"
    return url

class NewsItem(BaseModel):
    source: str
    headline_en: str
    headline_pt: str
    summary_en: str
    summary_pt: str
    impact: str
    assets: str
    url: str

@app.get("/fetch")
def fetch_news():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    added = 0
    for feed in RSS_FEEDS:
        try:
            d = feedparser.parse(feed)
            for entry in d.entries[:8]:
                c.execute("SELECT id FROM news WHERE url=?", (entry.link,))
                if c.fetchone(): continue
                timestamp = datetime.utcnow().isoformat()
                headline_en = entry.title
                headline_pt = entry.title
                summary_en = entry.summary if 'summary' in entry else ""
                summary_pt = summary_en
                impact = classify_impact(headline_en)
                assets = detect_assets(headline_en)
                url = entry.link
                source = get_source_name(feed)
                c.execute("INSERT INTO news (timestamp, source, headline_en, headline_pt, summary_en, summary_pt, impact, assets, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          (timestamp, source, headline_en, headline_pt, summary_en, summary_pt, impact, assets, url))
                added += 1
        except Exception as e:
            print(f"Error fetching {feed}: {e}")
            continue
    conn.commit()
    conn.close()
    return {"status": "ok", "added": added}

@app.get("/news")
def get_news(limit: int = 50, impact: str = None):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    query = "SELECT timestamp, source, headline_en, headline_pt, impact, assets, url FROM news"
    params = []
    if impact:
        query += " WHERE impact=?"
        params.append(impact)
    query += " ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)
    c.execute(query, tuple(params))
    rows = c.fetchall()
    conn.close()
    return [{"timestamp": r[0], "source": r[1], "headline_en": r[2], "headline_pt": r[3], "impact": r[4], "assets": r[5], "url": r[6]} for r in rows]
