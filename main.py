from fastapi import FastAPI
from pydantic import BaseModel
import feedparser, sqlite3
from datetime import datetime

app = FastAPI()

RSS_FEEDS = [
    "https://www.reuters.com/markets/rss",
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "https://www.bloomberg.com/feed/podcast/etf-report.xml",
    "https://www.investing.com/rss/news.rss"
]

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
        d = feedparser.parse(feed)
        for entry in d.entries[:5]:
            c.execute("SELECT id FROM news WHERE url=?", (entry.link,))
            if c.fetchone(): continue
            timestamp = datetime.utcnow().isoformat()
            headline_en = entry.title
            headline_pt = entry.title
            summary_en = entry.summary if 'summary' in entry else ""
            summary_pt = summary_en
            impact = "MEDIUM"
            assets = "USD"
            url = entry.link
            c.execute("INSERT INTO news (timestamp, source, headline_en, headline_pt, summary_en, summary_pt, impact, assets, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                      (timestamp, feed, headline_en, headline_pt, summary_en, summary_pt, impact, assets, url))
            added += 1
    conn.commit()
    conn.close()
    return {"status": "ok", "added": added}

@app.get("/news")
def get_news(limit: int = 20, impact: str = None):
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
