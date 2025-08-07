import streamlit as st
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import timedelta
import os
import plotly.graph_objects as go
import pytz

# CRITICAL: Disable ALL Streamlit caching
st.cache_data.clear()
st.cache_resource.clear()

# Load environment variables
load_dotenv()
url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

st.set_page_config(page_title="Bitcoin Dashboard", layout="wide")

# Define timezone constants
UTC = pytz.UTC
MEZ = pytz.timezone('Europe/Berlin')

# Force fresh data on every page load
if 'data_refresh' not in st.session_state:
    st.session_state.data_refresh = 0

st.session_state.data_refresh += 1

if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.session_state.data_refresh += 1
    st.rerun()

st.title("Bitcoin Dashboard")

# --- Fetch Bitcoin value data (NO CACHING) ---
@st.cache_data(ttl=0)
def fetch_value_data():
    try:
        response = supabase.table("value").select("*").order("open_time", desc=True).limit(None).execute()
        return response.data
    except:
        try:
            response = supabase.table("value").select("*").order("open_time", desc=True).limit(10000).execute()
            return response.data
        except:
            all_data = []
            offset = 0
            batch_size = 1000
            
            while True:
                batch_response = supabase.table("value").select("*").order("open_time", desc=True).range(offset, offset + batch_size - 1).execute()
                batch_data = batch_response.data
                
                if not batch_data:
                    break
                    
                all_data.extend(batch_data)
                
                if len(batch_data) < batch_size:
                    break
                    
                offset += batch_size
            
            return all_data

@st.cache_data(ttl=0)
def fetch_articles_data():
    response = supabase.table("articles").select("*").execute()
    return response.data

# Force fresh data fetch
value_data = fetch_value_data()

if value_data:
    df_value_full = pd.DataFrame(value_data)
    
    # Parse timestamps and convert to MEZ
    df_value_full["open_time"] = pd.to_datetime(df_value_full["open_time"], errors="coerce", utc=True)
    df_value_full["open_time_mez"] = df_value_full["open_time"].dt.tz_convert(MEZ)
    
    # Sort by time (ensure newest data is last)
    df_value_full = df_value_full.sort_values("open_time").reset_index(drop=True)

    # --- Time range buttons - PERFECTLY CENTERED ---
    col1, col2, col3, col4, col5, col6, col7 = st.columns([2, 1, 1, 1, 1, 1, 1.5])
    with col1:
        st.write("")  # Empty space
    with col2:
        range_btn_1d = st.button("1d")
    with col3:
        range_btn_2d = st.button("2d")
    with col4:
        range_btn_3d = st.button("3d")
    with col5:
        range_btn_7d = st.button("7d")
    with col6:
        range_btn_max = st.button("Max")
    with col7:
        st.write("")  # Empty space

    # Time filtering logic - This affects BOTH Bitcoin data AND articles
    now_mez = pd.Timestamp.now(tz=MEZ)
    
    # Initialize with default behavior
    if len(df_value_full) > 1000:
        df_value = df_value_full.tail(1000).reset_index(drop=True)
        # Default timeframe for articles (last 1000 points worth of time)
        article_start_time = df_value["open_time_mez"].min()
        article_end_time = df_value["open_time_mez"].max()
    else:
        df_value = df_value_full.copy()
        article_start_time = df_value["open_time_mez"].min()
        article_end_time = df_value["open_time_mez"].max()
    
    # Override based on button clicks - This affects BOTH chart AND sentiment
    if range_btn_max:
        df_value = df_value_full.copy()
        article_start_time = df_value["open_time_mez"].min()
        article_end_time = df_value["open_time_mez"].max()
    elif range_btn_1d:
        cutoff_time = now_mez - pd.Timedelta(days=1)
        df_value = df_value_full[df_value_full["open_time_mez"] >= cutoff_time].copy()
        article_start_time = cutoff_time
        article_end_time = now_mez
    elif range_btn_2d:
        cutoff_time = now_mez - pd.Timedelta(days=2)
        df_value = df_value_full[df_value_full["open_time_mez"] >= cutoff_time].copy()
        article_start_time = cutoff_time
        article_end_time = now_mez
    elif range_btn_3d:
        cutoff_time = now_mez - pd.Timedelta(days=3)
        df_value = df_value_full[df_value_full["open_time_mez"] >= cutoff_time].copy()
        article_start_time = cutoff_time
        article_end_time = now_mez
    elif range_btn_7d:
        cutoff_time = now_mez - pd.Timedelta(days=7)
        df_value = df_value_full[df_value_full["open_time_mez"] >= cutoff_time].copy()
        article_start_time = cutoff_time
        article_end_time = now_mez

    # Ensure data is sorted
    df_value = df_value.sort_values("open_time_mez").reset_index(drop=True)

    # --- Stats for the FILTERED timeframe - CENTERED AND CONNECTED TO BUTTONS ---
    if not df_value.empty:
        # These stats now change based on button selection
        latest_record_idx = df_value["open_time_mez"].idxmax()
        close_val = df_value.loc[latest_record_idx, "close"]  # Latest close in selected timeframe
        high_val = df_value["high"].max()  # Highest value in selected timeframe
        low_val = df_value["low"].min()   # Lowest value in selected timeframe
        volume_val = df_value["volume"].sum()  # Total volume in selected timeframe

        def euro_style(val):
            return f"${val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        stats_html = f"""
        <div style='display: flex; justify-content: center; gap: 60px; margin-bottom: 24px;'>
        <div style='text-align:center;'>
            <div style='font-size:18px; color:#666;'>Close</div>
            <div style='font-size:32px; font-weight:bold;'>{euro_style(close_val)}</div>
        </div>
        <div style='text-align:center;'>
            <div style='font-size:18px; color:#666;'>High</div>
            <div style='font-size:32px; font-weight:bold;'>{euro_style(high_val)}</div>
        </div>
        <div style='text-align:center;'>
            <div style='font-size:18px; color:#666;'>Low</div>
            <div style='font-size:32px; font-weight:bold;'>{euro_style(low_val)}</div>
        </div>
        <div style='text-align:center;'>
            <div style='font-size:18px; color:#666;'>Volume</div>
            <div style='font-size:32px; font-weight:bold;'>
            {volume_val:,.2f}&nbsp;<span style="font-size:20px; font-weight:normal;">BTC</span>
            </div>
        </div>
        </div>
        """

        st.markdown(stats_html, unsafe_allow_html=True)

        # --- Bitcoin Price Chart with title matching Volume size ---
        st.markdown("<h3 style='margin-bottom: 10px;'>Bitcoin Price Over Time</h3>", unsafe_allow_html=True)
        
        # --- Percentage Change Indicator - SMALLER with colored arrow only ---
        if not df_value.empty:
            start_val = df_value["close"].iloc[0]
            end_val = df_value["close"].iloc[-1]
            pct_change = ((end_val - start_val) / start_val) * 100 if start_val != 0 else 0
            start_date = df_value["open_time_mez"].iloc[0].strftime("%b %d")
            arrow = "▲" if pct_change >= 0 else "▼"
            arrow_color = "green" if pct_change >= 0 else "red"
            st.markdown(
                f"<div style='font-size:14px; margin-bottom:8px;'>"
                f"<span style='color:{arrow_color};'>{arrow}</span> {pct_change:+.2f}% since {start_date} (MEZ)"
                "</div>",
                unsafe_allow_html=True
            )

        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=df_value["open_time_mez"],
            y=df_value["close"],
            mode='lines',
            name='Bitcoin Value',
            line=dict(width=2)
        ))
        
        fig.update_layout(
            yaxis_title="Bitcoin Value (USD)",
            xaxis_title="Time (MEZ)",
            yaxis=dict(tickformat=","),
            height=400,
            margin=dict(l=20, r=20, t=20, b=20),
            showlegend=False
        )

        st.plotly_chart(fig, use_container_width=True)
        
        # --- Volume Chart - REDUCED MARGIN ---
        st.subheader("Volume of Trade")
        
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=df_value["open_time_mez"],
            y=df_value["volume"],
            mode='lines',
            name='Volume'
        ))
        fig2.add_trace(go.Scatter(
            x=df_value["open_time_mez"],
            y=df_value["number_of_trades"],
            mode='lines',
            name='Number of Trades',
            yaxis='y2'
        ))
        fig2.update_layout(
            yaxis_title="Volume",
            yaxis2=dict(title="Number of Trades", overlaying='y', side='right'),
            xaxis_title="Time (MEZ)",
            height=300,
            margin=dict(l=20, r=20, t=20, b=10),
            legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center")
        )
        st.plotly_chart(fig2, use_container_width=True)

        # --- Raw Data Table ---
        with st.expander("Show full raw value table"):
            df_display = df_value.copy()
            df_display["open_time_utc"] = df_display["open_time"]
            df_display = df_display[["open_time_utc", "open_time_mez", "close", "high", "low", "volume", "number_of_trades"]]
            st.dataframe(df_display, use_container_width=True)

        # --- SENTIMENT ANALYSIS SECTION - MOVED HERE ---
        data = fetch_articles_data()
        if data:
            df = pd.DataFrame(data)
            
            def robust_parse_with_timezone(raw_date):
                try:
                    dt = pd.to_datetime(raw_date, errors="coerce", utc=True)
                    if pd.isnull(dt):
                        dt = pd.to_datetime(raw_date, errors="coerce")
                        if not pd.isnull(dt) and dt.tzinfo is None:
                            dt = dt.tz_localize('UTC')
                    return dt
                except Exception:
                    return pd.NaT

            df["datetime_parsed"] = df["datetime"].apply(robust_parse_with_timezone)
            df["datetime_mez"] = df["datetime_parsed"].dt.tz_convert(MEZ)
            df = df.sort_values("datetime_parsed").reset_index(drop=True)

            # --- Sentiment Analysis - NOW USES SAME TIME FILTER AS BITCOIN DATA ---
            st.subheader("Sentiment Analysis of Bitcoin News")

            # Filter articles based on the SAME timeframe as Bitcoin data
            mask = (df["datetime_mez"] >= article_start_time) & (df["datetime_mez"] <= article_end_time)
            filtered_articles = df[mask]

            if not filtered_articles.empty and "sentiment" in filtered_articles.columns:
                total = len(filtered_articles)
                positive_count = (filtered_articles["sentiment"] == "positive").sum()
                neutral_count = (filtered_articles["sentiment"] == "neutral").sum()
                negative_count = (filtered_articles["sentiment"] == "negative").sum()

                percent_positive = 100 * positive_count / total
                percent_neutral = 100 * neutral_count / total
                percent_negative = 100 * negative_count / total

                fig_bar = go.Figure()
                fig_bar.add_trace(go.Bar(
                    y=["Sentiment"],
                    x=[percent_negative],
                    name="Negative",
                    orientation='h',
                    marker=dict(color="#FF5A5F", line=dict(width=0)),
                    width=0.5,
                    offset=0
                ))
                fig_bar.add_trace(go.Bar(
                    y=["Sentiment"],
                    x=[percent_neutral],
                    name="Neutral",
                    orientation='h',
                    marker=dict(color="#FFD700", line=dict(width=0)),
                    width=0.5,
                    offset=0
                ))
                fig_bar.add_trace(go.Bar(
                    y=["Sentiment"],
                    x=[percent_positive],
                    name="Positive",
                    orientation='h',
                    marker=dict(color="#2ECC40", line=dict(width=0)),
                    width=0.5,
                    offset=0
                ))

                fig_bar.update_layout(
                    barmode='stack',
                    height=120,
                    margin=dict(l=20, r=20, t=20, b=20),
                    xaxis=dict(range=[0, 100], title="Percentage", showgrid=False, zeroline=False),
                    yaxis=dict(showticklabels=False, showgrid=False, zeroline=False),
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    showlegend=False
                )

                st.plotly_chart(fig_bar, use_container_width=True, config={"displayModeBar": False})

                st.markdown(
                    f"<div style='text-align:center;font-size:18px; margin-top:12px;'>"
                    f"Negative: <b>{percent_negative:.0f}%</b> &nbsp;|&nbsp; "
                    f"Neutral: <b>{percent_neutral:.0f}%</b> &nbsp;|&nbsp; "
                    f"Positive: <b>{percent_positive:.0f}%</b>"
                    "</div>",
                    unsafe_allow_html=True
                )

                # Show timeframe info
                start_str = article_start_time.strftime("%Y-%m-%d %H:%M")
                end_str = article_end_time.strftime("%Y-%m-%d %H:%M")
                
                with st.expander(f"📰 Articles for selected timeframe ({total} articles from {start_str} to {end_str})"):
                    for _, article in filtered_articles.iterrows():
                        sentiment_color = {"positive": "🟢", "negative": "🔴", "neutral": "🟡"}.get(article["sentiment"], "⚪")
                        article_time_mez = article["datetime_mez"].strftime("%m-%d %H:%M") if pd.notna(article["datetime_mez"]) else "N/A"
                        st.markdown(f"{sentiment_color} **{article_time_mez}** - [{article['title']}]({article['link']})")
            else:
                st.info(f"No articles found for the selected timeframe.")

        # --- Bitcoin/USD Converter - MOVED BELOW SENTIMENT ---
        st.subheader("Bitcoin/USD Converter")
        conversion_mode = st.selectbox("Select input currency:", ["Bitcoin (BTC)", "US Dollar (USD)"])
        
        latest_close = df_value.loc[df_value["open_time_mez"].idxmax(), "close"]

        if conversion_mode == "Bitcoin (BTC)":
            btc_amount = st.number_input("Enter amount in Bitcoin (BTC):", min_value=0.0, value=1.0, step=0.01)
            usd_value = btc_amount * float(latest_close)
            st.success(f"{btc_amount} BTC ≈ ${usd_value:,.2f} USD")
        else:
            usd_amount = st.number_input("Enter amount in US Dollar (USD):", min_value=0.0, value=1000.0, step=1.0)
            btc_value = usd_amount / float(latest_close)
            st.success(f"${usd_amount:,.2f} ≈ {btc_value:.8f} BTC")

        # --- Articles Table - AT THE BOTTOM ---
        if data:
            st.subheader("Bitcoin News")
            df_display = df.sort_values("datetime_parsed", ascending=False).copy()
            if "id" in df_display.columns:
                df_display = df_display.drop(columns=["id"])
            if "datetime_parsed" in df_display.columns:
                df_display = df_display.drop(columns=["datetime_parsed"])
            st.dataframe(df_display)
        else:
            st.warning("No articles found in the database.")

else:
    st.warning("No Bitcoin value data found in the database.")