# scrape_logic.py

# IMPORTS
import os
import time
import pandas as pd
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from textblob import TextBlob
from dotenv import load_dotenv
from supabase import create_client, Client
from playwright.sync_api import sync_playwright
import psycopg2

# INIT
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BINANCE_ENDPOINT = os.getenv("BINANCE_ENDPOINT")
DB_CONN = os.getenv("DB_CONN")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def create_tables_if_not_exists():
    try:
        with psycopg2.connect(DB_CONN) as conn:
            with conn.cursor() as cur: 
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS value (
                        open_time TIMESTAMPTZ PRIMARY KEY,
                        open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume NUMERIC,
                        close_time TIMESTAMPTZ, quote_asset_volume NUMERIC,
                        number_of_trades INTEGER, taker_buy_base_asset_volume NUMERIC,
                        taker_buy_quote_asset_volume NUMERIC, ignore TEXT
                    );
                ''')
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS articles (
                        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        title TEXT, link TEXT, author TEXT,
                        datetime TIMESTAMPTZ, content TEXT, sentiment TEXT
                    );
                ''')
                conn.commit()
    except Exception as e:
        print(f"❌ Error creating tables: {e}")


def scrape_articles():
    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://u.today/search/node?keys=bitcoin", timeout=60000)
        page.wait_for_timeout(3000)  # wait for content to load

        soup = BeautifulSoup(page.content(), 'html.parser')
        articles = soup.find_all('div', class_='news__item')

        for article in articles:
            try:
                title_tag = article.find('div', class_='news__item-title')
                a_tag = title_tag.find_parent('a') if title_tag else None
                href = a_tag['href'] if a_tag and 'href' in a_tag.attrs else None
                link = href if href and href.startswith("http") else f"https://u.today{href}" if href else None
                if not link:
                    continue

                response = requests.get(link)
                if response.status_code != 200:
                    continue

                article_soup = BeautifulSoup(response.text, 'html.parser')
                h1_tag = article_soup.find('h1', class_='article__title')
                article_title = h1_tag.get_text(strip=True) if h1_tag else 'N/A'

                date_tag = article_soup.find('div', class_='article__short-date')
                raw_date = date_tag.get_text(strip=True) if date_tag else None
                article_datetime = pd.to_datetime(raw_date, errors="coerce", dayfirst=True).strftime("%Y-%m-%dT%H:%M:%S") if raw_date else None

                author_tag = article_soup.find('div', class_='author-brief__name')
                author_name = author_tag.get_text(strip=True) if author_tag else 'N/A'
                if author_name.lower().startswith('by'):
                    author_name = author_name[2:].strip()

                p_tags = article_soup.find_all('p', attrs={'dir': 'ltr'})
                article_text = '\n'.join([p.get_text(strip=True) for p in p_tags]) if p_tags else 'N/A'

                sentiment = get_sentiment_label(article_text)

                results.append({
                    'title': article_title,
                    'datetime': article_datetime,
                    'author': author_name,
                    'link': link,
                    'content': article_text,
                    'sentiment': sentiment
                })

            except Exception as e:
                print(f"Error processing article: {e}")

        browser.close()
    return pd.DataFrame(results)

def get_sentiment_label(text):
    try:
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        if polarity > 0.1:
            return 'positive'
        elif polarity < -0.1:
            return 'negative'
        else:
            return 'neutral'
    except Exception:
        return 'unknown'
    
def insert_articles(df_articles):
    def norm_title(t): return t.strip().lower() if isinstance(t, str) else t
    def norm_dt(dt):
        try:
            ts = pd.to_datetime(dt, errors="coerce")
            if pd.isnull(ts):
                ts = pd.to_datetime(dt, errors="coerce", dayfirst=True)
            if not pd.isnull(ts):
                return ts.tz_localize(None).strftime("%Y-%m-%dT%H:%M:%S")
        except:
            return None

    # Deduplicate
    existing = set(
        (norm_title(row["title"]), norm_dt(row["datetime"]))
        for row in supabase.table("articles").select("title", "datetime").execute().data
    )

    df_articles["title_norm"] = df_articles["title"].apply(norm_title)
    df_articles["datetime_norm"] = df_articles["datetime"].apply(norm_dt)

    df_articles_new = df_articles[
        ~df_articles.apply(lambda row: (row["title_norm"], row["datetime_norm"]) in existing, axis=1)
    ].copy()

    df_articles_new = df_articles_new.drop(columns=["title_norm", "datetime_norm"], errors="ignore")

    articles_list = df_articles_new[["title", "link", "author", "datetime", "content", "sentiment"]].to_dict("records")

    for article in articles_list:
        ts = pd.to_datetime(article.get("datetime"), errors="coerce")
        article["datetime"] = ts.strftime("%Y-%m-%dT%H:%M:%S") if not pd.isnull(ts) else None

    if articles_list:
        supabase.table("articles").insert(articles_list).execute()
        print(f"✅ Inserted {len(articles_list)} new articles.")
    else:
        print("✅ No new articles to insert.")


def insert_binance_data():
    latest = supabase.table("value").select("open_time").order("open_time", desc=True).limit(1).execute()
    params = {}
    if latest.data and latest.data[0].get("open_time"):
        ts = pd.to_datetime(latest.data[0]["open_time"])
        params = {"startTime": int(ts.timestamp() * 1000)}

    resp = requests.get(BINANCE_ENDPOINT, params=params)
    columns = [
        "open_time", "open", "high", "low", "close", "volume", "close_time",
        "quote_asset_volume", "number_of_trades",
        "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
    ]
    df = pd.DataFrame(resp.json(), columns=columns)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

    existing = set(pd.to_datetime(
        [row["open_time"] for row in supabase.table("value").select("open_time").execute().data], utc=True
    ))
    df = df[~df["open_time"].isin(existing)]

    if df.empty:
        print("✅ No new Binance data to insert.")
        return

    for col in ["open_time", "close_time"]:
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    value_list = df.where(pd.notnull(df), None).to_dict("records")
    supabase.table("value").insert(value_list).execute()
    print(f"✅ Inserted {len(value_list)} Binance rows.")


def main():
    create_tables_if_not_exists()
    df_articles = scrape_articles()
    insert_articles(df_articles)
    insert_binance_data()
