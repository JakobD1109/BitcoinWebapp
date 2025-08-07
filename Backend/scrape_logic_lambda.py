# Lambda-compatible version of scraping logic
import os
import time
import pandas as pd
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from textblob import TextBlob
from dotenv import load_dotenv
from supabase import create_client, Client
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


def scrape_articles_requests():
    """Simplified version using only requests (no Playwright)"""
    results = []
    try:
        # Use requests with headers to mimic a browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get("https://u.today/search/node?keys=bitcoin", headers=headers)
        if response.status_code != 200:
            print(f"❌ Failed to fetch main page: {response.status_code}")
            return pd.DataFrame(results)
        
        soup = BeautifulSoup(response.text, 'html.parser')
        articles = soup.find_all('div', class_='news__item')
        
        print(f"📰 Found {len(articles)} articles to process")
        
        for i, article in enumerate(articles[:5]):  # Limit to 5 articles for testing
            try:
                title_tag = article.find('div', class_='news__item-title')
                a_tag = title_tag.find_parent('a') if title_tag else None
                href = a_tag['href'] if a_tag and 'href' in a_tag.attrs else None
                link = href if href and href.startswith("http") else f"https://u.today{href}" if href else None
                if not link:
                    continue

                # Get article content
                article_response = requests.get(link, headers=headers)
                if article_response.status_code != 200:
                    continue

                article_soup = BeautifulSoup(article_response.text, 'html.parser')
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

                print(f"✅ Processed article {i+1}: {article_title[:50]}...")

            except Exception as e:
                print(f"❌ Error processing article {i+1}: {e}")

    except Exception as e:
        print(f"❌ Error in scraping: {e}")
    
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
    try:
        if df_articles.empty:
            print("✅ No articles to process.")
            return
            
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

        # Get existing articles for deduplication
        existing_data = supabase.table("articles").select("title", "datetime").execute().data
        existing = set(
            (norm_title(row["title"]), norm_dt(row["datetime"]))
            for row in existing_data
        )
        print(f"📊 Found {len(existing)} existing articles in database")

        # Apply normalization to scraped articles
        df_articles["title_norm"] = df_articles["title"].apply(norm_title)
        df_articles["datetime_norm"] = df_articles["datetime"].apply(norm_dt)

        # Filter out duplicates
        def is_not_duplicate(row):
            key = (row["title_norm"], row["datetime_norm"])
            return key not in existing

        df_articles_new = df_articles[df_articles.apply(is_not_duplicate, axis=1)].copy()
        df_articles_new = df_articles_new.drop(columns=["title_norm", "datetime_norm"], errors="ignore")

        if df_articles_new.empty:
            print("✅ No new articles to insert.")
            return

        articles_list = df_articles_new[["title", "link", "author", "datetime", "content", "sentiment"]].to_dict("records")

        # Clean up datetime format
        for article in articles_list:
            ts = pd.to_datetime(article.get("datetime"), errors="coerce")
            article["datetime"] = ts.strftime("%Y-%m-%dT%H:%M:%S") if not pd.isnull(ts) else None

        # Insert in smaller batches to avoid issues
        batch_size = 10
        total_inserted = 0
        
        for i in range(0, len(articles_list), batch_size):
            batch = articles_list[i:i+batch_size]
            try:
                supabase.table("articles").insert(batch).execute()
                total_inserted += len(batch)
                print(f"✅ Inserted batch of {len(batch)} articles")
            except Exception as batch_error:
                print(f"⚠️ Failed to insert batch {i//batch_size + 1}: {batch_error}")
                continue
        
        print(f"✅ Total articles inserted: {total_inserted}")
            
    except Exception as e:
        print(f"⚠️ Warning: Articles insertion failed: {e}")
        # Don't raise the exception - continue with execution


def insert_binance_data():
    try:
        latest = supabase.table("value").select("open_time").order("open_time", desc=True).limit(1).execute()
        
        # Get data starting from the latest timestamp
        params = {}
        if latest.data and latest.data[0].get("open_time"):
            # Ensure we're working with UTC timezone-aware datetime
            ts = pd.to_datetime(latest.data[0]["open_time"], utc=True)
            # Use startTime to get data from this point forward
            params = {"startTime": int(ts.timestamp() * 1000)}
            print(f"📅 Fetching Binance data from: {ts} (UTC)")
            print(f"📅 Timestamp for API: {int(ts.timestamp() * 1000)}")

        resp = requests.get(BINANCE_ENDPOINT, params=params)
        if resp.status_code != 200:
            print(f"❌ Failed to fetch Binance data: {resp.status_code}")
            return
            
        columns = [
            "open_time", "open", "high", "low", "close", "volume", "close_time",
            "quote_asset_volume", "number_of_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
        ]
        df = pd.DataFrame(resp.json(), columns=columns)
        # Ensure all timestamps are UTC timezone-aware
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
        
        print(f"📈 Fetched {len(df)} Binance records")
        if len(df) > 0:
            print(f"🔍 First fetched record: {df.iloc[0]['open_time']} (UTC)")
            print(f"🔍 Last fetched record: {df.iloc[-1]['open_time']} (UTC)")

        # Filter to exclude the latest timestamp we already have
        if latest.data and latest.data[0].get("open_time"):
            # Ensure both timestamps are UTC timezone-aware for proper comparison
            latest_ts = pd.to_datetime(latest.data[0]["open_time"], utc=True)
            # Only keep records AFTER the latest timestamp (exclude the exact match)
            df_new = df[df["open_time"] > latest_ts]
            print(f"🔍 Latest in DB: {latest_ts} (UTC)")
            print(f"🆕 New records after deduplication: {len(df_new)}")
            
            if len(df_new) > 0:
                print(f"📅 First new record: {df_new.iloc[0]['open_time']} (UTC)")
                print(f"📅 Last new record: {df_new.iloc[-1]['open_time']} (UTC)")
        else:
            df_new = df
            print("🔍 No existing data found, inserting all records")

        if df_new.empty:
            print("✅ No new Binance data to insert.")
            return

        # Format timestamps for insertion (PostgreSQL compatible format with timezone)
        df_formatted = df_new.copy()
        for col in ["open_time", "close_time"]:
            # Keep timezone information for proper storage
            df_formatted[col] = df_formatted[col].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        
        value_list = df_formatted.where(pd.notnull(df_formatted), None).to_dict("records")
        
        if value_list:
            print(f"📊 Inserting {len(value_list)} new Binance records...")
            print(f"🔍 Sample record timestamp: {value_list[0]['open_time']}")
            supabase.table("value").insert(value_list).execute()
            print(f"✅ Successfully inserted {len(value_list)} Binance rows.")
        else:
            print("✅ No new Binance data to insert.")
            
    except Exception as e:
        print(f"⚠️ Warning: Binance data insertion failed: {e}")
        import traceback
        traceback.print_exc()
        # Don't raise the exception - let the lambda continue


def main():
    print("🚀 Starting Lambda scraping function...")
    create_tables_if_not_exists()
    df_articles = scrape_articles_requests()  # Using requests version
    insert_articles(df_articles)
    insert_binance_data()
    print("✅ Lambda function completed successfully!")
