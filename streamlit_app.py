import re
"""
Signal Engine Dashboard
Unified intelligence dashboard for crypto + Amazon signals
"""

import streamlit as st
import psycopg2
import time
from sqlalchemy import create_engine, text
import psycopg2
import time
from sqlalchemy import create_engine, text
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

# ─── GEMINI AI ───────────────────────────────────────────────────────────────

DATABASE_URL   = os.getenv("DATABASE_URL") or st.secrets.get("DATABASE_URL", "")
def _get_gemini_keys():
    keys = []
    for k in ["GEMINI_API_KEY", "GEMINI_API_KEY_2", "GEMINI_API_KEY_3", "GEMINI_API_KEY_4"]:
        v = os.getenv(k) or st.secrets.get(k, "")
        if v: keys.append(v)
    return keys if keys else [""]

GEMINI_KEYS    = _get_gemini_keys()
GEMINI_API_KEY = GEMINI_KEYS[0]
# Список моделей из Secrets (через запятую) или дефолтный
_models_str = (
    os.getenv("GEMINI_MODELS") or
    st.secrets.get("GEMINI_MODELS", "") or
    "gemini-1.5-flash-8b,gemini-2.0-flash-lite,gemini-2.0-flash,gemini-2.5-flash-lite,gemini-2.5-flash"
)
GEMINI_MODELS_LIST = [m.strip() for m in _models_str.split(",") if m.strip()]
GEMINI_MODEL_DEFAULT = (
    os.getenv("GEMINI_MODEL") or
    st.secrets.get("GEMINI_MODEL", "") or
    GEMINI_MODELS_LIST[0]
)
SYSTEM = "Ты — эксперт по крипто-рынкам и e-commerce аналитик уровня хедж-фонда. Анализируешь социальные сигналы из Reddit. Отвечай структурированно, конкретно, без воды. Не используй markdown символы ## и ** в ответе."

def call_gemini(prompt: str, model_override: str = None) -> str:
    import requests as _req
    # Пробуем выбранную модель, потом остальные из списка
    chosen = model_override or st.session_state.get("gemini_model", GEMINI_MODEL_DEFAULT)
    models_to_try = [chosen] + [m for m in GEMINI_MODELS_LIST if m != chosen]

    for model in models_to_try:
        for key in GEMINI_KEYS:
            try:
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
                r = _req.post(
                    url,
                    headers={"Content-Type": "application/json"},
                    params={"key": key},
                    json={
                        "contents": [{"parts": [{"text": prompt}]}],
                        "generationConfig": {"temperature": 0.7, "maxOutputTokens": 2048}
                    },
                    timeout=30
                )
                if r.status_code == 200:
                    return r.json()["candidates"][0]["content"]["parts"][0]["text"]
                if r.status_code == 429:
                    continue  # следующий ключ или модель
            except Exception:
                continue
    return "⚠️ Все модели и ключи исчерпали лимит 429."


def generate_insights(category=None, lang="RU", days=7, model=None):
    engine = get_engine()
    conn_ins = engine.connect() if engine else None
    if not conn_ins:
        return {"error": "Нет подключения к БД"}

    where = f"WHERE scraped_at >= NOW() - INTERVAL '{days} days'"
    if category:
        where += f" AND category = '{category}'"

    posts_df = pd.read_sql(f"""
        SELECT subreddit, category, title, upvotes,
               sentiment_label, sentiment_score, is_hot,
               COALESCE(ai_summary, '') as ai_summary,
               post_url as url, scraped_at
        FROM reddit_posts {where}
        ORDER BY upvotes DESC LIMIT 40
    """, conn_ins)

    stats_df = pd.read_sql(f"""
        SELECT category,
               COUNT(*) as total,
               AVG(sentiment_score) as avg_sentiment,
               SUM(CASE WHEN is_hot THEN 1 ELSE 0 END) as hot_count,
               SUM(CASE WHEN sentiment_label='positive' THEN 1 ELSE 0 END) as positive,
               SUM(CASE WHEN sentiment_label='negative' THEN 1 ELSE 0 END) as negative
        FROM reddit_posts {where}
        GROUP BY category
    """, conn_ins)
    if conn_ins: conn_ins.close()

    if posts_df.empty:
        return {"error": "Нет данных для анализа" if lang == "RU" else "No data"}

    results = {"generated_at": datetime.now().strftime("%d %b %Y · %H:%M UTC"),
               "posts_analyzed": len(posts_df)}



    def posts_text(cat):
        df = posts_df[posts_df['category'] == cat].head(15)
        return "\n".join([f"[{r['upvotes']}↑ {r['sentiment_label']}] r/{r['subreddit']}: {r['title'][:100]}"
                           for _, r in df.iterrows()])

    def get_stat(cat, col):
        s = stats_df[stats_df['category'] == cat]
        return float(s[col].iloc[0]) if not s.empty and col in s.columns else 0

    # Промпты для каждой категории
    CAT_PROMPTS = {
        "crypto": (
            "КРИПТО | sentiment {sent:+.3f} | горячих {hot}\n{posts}",
            "## 🎯 ГЛАВНЫЙ СИГНАЛ\n## 📊 SENTIMENT\n## 🔥 ТОП НАРРАТИВ\n## ⚡ ТОРГОВОЕ ДЕЙСТВИЕ\n## ⚠️ РИСКИ"
        ),
        "amazon": (
            "AMAZON FBA | sentiment {sent:+.3f} | горячих {hot}\n{posts}",
            "## 🎯 БОЛЬ ПРОДАВЦОВ\n## 📊 РЫНОЧНЫЙ СИГНАЛ\n## 🔥 ВОЗМОЖНОСТЬ\n## ⚡ ДЕЙСТВИЕ\n## ⚠️ АЛЕРТЫ"
        ),
        "geopolitics": (
            "ГЕОПОЛИТИКА | sentiment {sent:+.3f} | горячих {hot}\n{posts}",
            "## 🎯 ГЛАВНОЕ СОБЫТИЕ\n## 📊 РЫНОЧНЫЙ ЭФФЕКТ\n## 🔥 ВЛИЯНИЕ НА КРИПТО\n## ⚡ КАК РЕАГИРОВАТЬ\n## ⚠️ РИСКИ"
        ),
        "ai_tech": (
            "AI / TECH | sentiment {sent:+.3f} | горячих {hot}\n{posts}",
            "## 🎯 ГЛАВНЫЙ ТРЕНД\n## 📊 НАСТРОЕНИЕ РЫНКА\n## 🔥 ВОЗМОЖНОСТЬ\n## ⚡ ДЕЙСТВИЕ\n## ⚠️ РИСКИ"
        ),
        "macro": (
            "МАКРО / ФИНАНСЫ | sentiment {sent:+.3f} | горячих {hot}\n{posts}",
            "## 🎯 МАКРО СИГНАЛ\n## 📊 FED / СТАВКИ\n## 🔥 ВЛИЯНИЕ НА АКТИВЫ\n## ⚡ ПОЗИЦИЯ\n## ⚠️ РИСКИ"
        ),
        "regulations": (
            "РЕГУЛЯЦИИ | sentiment {sent:+.3f} | горячих {hot}\n{posts}",
            "## 🎯 ГЛАВНОЕ ИЗМЕНЕНИЕ\n## 📊 ВЛИЯНИЕ НА РЫНОК\n## 🔥 ЧТО МЕНЯЕТСЯ\n## ⚡ ДЕЙСТВИЕ\n## ⚠️ РИСКИ"
        ),
        "ecommerce": (
            "E-COMMERCE | sentiment {sent:+.3f} | горячих {hot}\n{posts}",
            "## 🎯 ТРЕНД РЫНКА\n## 📊 СИГНАЛ\n## 🔥 ВОЗМОЖНОСТЬ\n## ⚡ ДЕЙСТВИЕ\n## ⚠️ АЛЕРТЫ"
        ),
    }

    # Генерируем инсайт для выбранной категории или всех
    cats_to_analyze = [category] if category else list(CAT_PROMPTS.keys())

    for cat in cats_to_analyze:
        if cat not in CAT_PROMPTS:
            continue
        cat_df = posts_df[posts_df['category'] == cat]
        if cat_df.empty:
            continue
        data_tmpl, structure = CAT_PROMPTS[cat]
        data_str = data_tmpl.format(
            sent=get_stat(cat, 'avg_sentiment'),
            hot=int(get_stat(cat, 'hot_count')),
            posts=posts_text(cat)
        )
        results[cat] = call_gemini(f"""{SYSTEM}

{data_str}

Структура ответа:
{structure}""", model_override=model)

    if category is None:
        hot = posts_df[posts_df['is_hot']==True].head(10)
        hot_text = "\n".join([f"[{r['category'].upper()} {r['upvotes']}↑] {r['title'][:90]}"
                                for _, r in hot.iterrows()])
        crypto_sent = get_stat('crypto','avg_sentiment')
        amazon_sent = get_stat('amazon','avg_sentiment')
        cs = f"{crypto_sent:+.3f}"
        as_ = f"{amazon_sent:+.3f}"
        prompt = (
            f"{SYSTEM}\n\n"
            f"Crypto sentiment: {cs}\n"
            f"Amazon sentiment: {as_}\n\n"
            f"ТОП ГОРЯЧИЕ:\n{hot_text}\n\n"
            "Дай EXECUTIVE SUMMARY:\n"
            "## 🌐 MACRO СИГНАЛ\n"
            "## 💡 ГЛАВНЫЙ ИНСАЙТ ДНЯ\n"
            "## 📈 КРИПТО: ДЕЙСТВИЕ\n"
            "## 📦 AMAZON: ДЕЙСТВИЕ\n"
            "## 🎯 ИТОГ"
        )
        results["summary"] = call_gemini(prompt, model_override=model)

    return results

# ─── PAGE CONFIG ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Signal Engine",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Session state init
if "gemini_model" not in st.session_state:
    st.session_state.gemini_model = GEMINI_MODEL_DEFAULT
if "selected_cat" not in st.session_state:
    st.session_state.selected_cat = None

# ─── CUSTOM CSS ──────────────────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Inter:wght@300;400;500;600&display=swap');

/* Base */
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
    background-color: #0a0a0f;
    color: #e2e8f0;
}

.main { background-color: #0a0a0f; }
.block-container { padding: 1.5rem 2rem; max-width: 1400px; }

/* Header */
.dash-header {
    font-family: 'Space Mono', monospace;
    font-size: 1.8rem;
    font-weight: 700;
    color: #00ff88;
    letter-spacing: -0.02em;
    margin-bottom: 0.2rem;
}
.dash-sub {
    font-size: 0.85rem;
    color: #64748b;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    margin-bottom: 1.5rem;
}

/* Metric cards */
.metric-card {
    background: #111118;
    border: 1px solid #1e1e2e;
    border-radius: 8px;
    padding: 1.2rem 1.4rem;
    position: relative;
    overflow: hidden;
}
.metric-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, #00ff88, #0ea5e9);
}
.metric-value {
    font-family: 'Space Mono', monospace;
    font-size: 2rem;
    font-weight: 700;
    color: #f1f5f9;
    line-height: 1;
}
.metric-label {
    font-size: 0.75rem;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-top: 0.4rem;
}
.metric-delta {
    font-family: 'Space Mono', monospace;
    font-size: 0.8rem;
    color: #00ff88;
    margin-top: 0.3rem;
}
.metric-delta.neg { color: #f43f5e; }

/* Signal badge */
.signal-hot {
    display: inline-block;
    background: rgba(239, 68, 68, 0.15);
    border: 1px solid rgba(239, 68, 68, 0.3);
    color: #f87171;
    font-size: 0.7rem;
    font-family: 'Space Mono', monospace;
    padding: 2px 8px;
    border-radius: 4px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
.signal-pos {
    display: inline-block;
    background: rgba(0, 255, 136, 0.1);
    border: 1px solid rgba(0, 255, 136, 0.25);
    color: #00ff88;
    font-size: 0.7rem;
    font-family: 'Space Mono', monospace;
    padding: 2px 8px;
    border-radius: 4px;
}
.signal-neg {
    display: inline-block;
    background: rgba(244, 63, 94, 0.1);
    border: 1px solid rgba(244, 63, 94, 0.25);
    color: #f43f5e;
    font-size: 0.7rem;
    font-family: 'Space Mono', monospace;
    padding: 2px 8px;
    border-radius: 4px;
}
.signal-neu {
    display: inline-block;
    background: rgba(100, 116, 139, 0.15);
    border: 1px solid rgba(100, 116, 139, 0.25);
    color: #94a3b8;
    font-size: 0.7rem;
    font-family: 'Space Mono', monospace;
    padding: 2px 8px;
    border-radius: 4px;
}

/* Section headers */
.section-title {
    font-family: 'Space Mono', monospace;
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.15em;
    color: #475569;
    border-bottom: 1px solid #1e1e2e;
    padding-bottom: 0.5rem;
    margin-bottom: 1rem;
}

/* Feed items */
.feed-item {
    background: #111118;
    border: 1px solid #1a1a2e;
    border-radius: 6px;
    padding: 0.9rem 1.1rem;
    margin-bottom: 0.6rem;
    transition: border-color 0.2s;
}
.feed-item:hover { border-color: #2d2d4a; }
.feed-title {
    font-size: 0.9rem;
    font-weight: 500;
    color: #e2e8f0;
    margin-bottom: 0.35rem;
    line-height: 1.4;
}
.feed-meta {
    font-size: 0.75rem;
    color: #475569;
    font-family: 'Space Mono', monospace;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background-color: #0d0d15;
    border-right: 1px solid #1a1a2e;
}
section[data-testid="stSidebar"] .block-container {
    padding: 1.5rem 1rem;
}

/* Plotly dark override */
.js-plotly-plot .plotly .modebar { background: transparent !important; }

/* Streamlit overrides */
div[data-testid="stMetric"] {
    background: #111118;
    border: 1px solid #1e1e2e;
    border-radius: 8px;
    padding: 1rem;
}
div[data-testid="stMetricValue"] {
    font-family: 'Space Mono', monospace;
    color: #f1f5f9;
}

/* Tab styling */
.stTabs [data-baseweb="tab-list"] {
    background: transparent;
    gap: 0.5rem;
}
.stTabs [data-baseweb="tab"] {
    background: #111118;
    border: 1px solid #1e1e2e;
    border-radius: 6px;
    color: #64748b;
    font-family: 'Space Mono', monospace;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}
.stTabs [aria-selected="true"] {
    background: #1a2a1a;
    border-color: #00ff88;
    color: #00ff88;
}
/* Light theme */
body.light-theme, .light-theme .stApp {
    background-color: #f8fafc !important;
    color: #0f172a !important;
}
.light-theme .metric-card {
    background: #ffffff !important;
    border-color: #e2e8f0 !important;
    color: #0f172a !important;
}
.light-theme .feed-item {
    background: #ffffff !important;
    border-color: #e2e8f0 !important;
}
.light-theme .feed-title { color: #0f172a !important; }
.light-theme .feed-meta  { color: #64748b !important; }
.light-theme .metric-value { color: #0f172a !important; }
.light-theme .metric-label { color: #64748b !important; }
</style>
""", unsafe_allow_html=True)


# ─── DATABASE ────────────────────────────────────────────────────────────────

@st.cache_resource
@st.cache_resource
def get_engine():
    database_url = (
        os.getenv("DATABASE_URL") or
        st.secrets.get("DATABASE_URL", "") or
        st.secrets.get("database", {}).get("DATABASE_URL", "")
    )
    if not database_url:
        return None
    # SQLAlchemy нужен postgresql:// не postgres://
    url = database_url.replace("postgres://", "postgresql://", 1)
    return create_engine(url, pool_pre_ping=True, pool_size=2, max_overflow=3)

def get_connection():
    return get_engine()


def query(sql, params=None):
    engine = get_engine()
    if engine is None:
        st.error("DB: нет DATABASE_URL")
        return pd.DataFrame()
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params=params)
    except Exception as e:
        st.error(f"DB error: {e}")
        return pd.DataFrame()


# ─── DATA LOADERS ────────────────────────────────────────────────────────────

def load_reddit_stats(category=None, days=7):
    where = f"WHERE scraped_at >= NOW() - INTERVAL '{days} days'"
    if category:
        where += f" AND category = '{category}'"
    return query(f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN is_hot THEN 1 ELSE 0 END) as hot_count,
            AVG(sentiment_score) as avg_sentiment,
            SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) as positive,
            SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) as negative,
            SUM(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END) as neutral
        FROM reddit_posts
        {where}
    """)


def load_reddit_feed(category=None, days=7, limit=20, only_hot=False):
    conditions = [f"scraped_at >= NOW() - INTERVAL '{days} days'"]
    if category:
        conditions.append(f"category = '{category}'")
    if only_hot:
        conditions.append("is_hot = TRUE")
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    return query(f"""
        SELECT
            title, subreddit, upvotes, comments_count,
            sentiment_label, sentiment_score, is_hot,
            keywords_found, scraped_at, post_url as url,
            COALESCE(ai_summary, '') as ai_summary
        FROM reddit_posts
        {where}
        ORDER BY scraped_at DESC, upvotes DESC
        LIMIT {limit}
    """)


def load_sentiment_timeline(category=None, days=14):
    where = f"WHERE scraped_at >= NOW() - INTERVAL '{days} days'"
    if category:
        where += f" AND category = '{category}'"
    return query(f"""
        SELECT
            DATE(scraped_at) as date,
            AVG(sentiment_score) as avg_sentiment,
            COUNT(*) as post_count,
            SUM(CASE WHEN is_hot THEN 1 ELSE 0 END) as hot_count
        FROM reddit_posts
        {where}
        GROUP BY DATE(scraped_at)
        ORDER BY date
    """)


def load_google_stats(category=None, days=7):
    where = f"WHERE scraped_at >= NOW() - INTERVAL '{days} days'"
    if category:
        where += f" AND category = '{category}'"
    return query(f"""
        SELECT
            COUNT(*) as total,
            COUNT(DISTINCT query) as unique_queries,
            COUNT(DISTINCT result_type) as result_types,
            SUM(CASE WHEN result_type = 'forums' THEN 1 ELSE 0 END) as forums,
            SUM(CASE WHEN result_type = 'organic' THEN 1 ELSE 0 END) as organic
        FROM google_results
        {where}
    """)


def load_google_feed(category=None, days=7, limit=20):
    conditions = [f"scraped_at >= NOW() - INTERVAL '{days} days'"]
    if category:
        conditions.append(f"category = '{category}'")
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    return query(f"""
        SELECT
            query, title, url, snippet,
            result_type, position, source, scraped_at
        FROM google_results
        {where}
        ORDER BY scraped_at DESC, position ASC
        LIMIT {limit}
    """)


def load_top_keywords(category=None, days=7, limit=20):
    where = f"WHERE r.scraped_at >= NOW() - INTERVAL '{days} days'"
    if category:
        where += f" AND r.category = '{category}'"
    return query(f"""
        SELECT
            kw.keyword,
            COUNT(*) as mentions,
            AVG(r.sentiment_score) as avg_sentiment,
            SUM(r.upvotes) as total_upvotes
        FROM reddit_posts r,
             LATERAL jsonb_array_elements_text(
               CASE WHEN r.keywords_found IS NOT NULL
                    THEN r.keywords_found
                    ELSE '[]'::jsonb
               END
             ) AS kw(keyword)
        {where}
        GROUP BY kw.keyword
        ORDER BY mentions DESC
        LIMIT {limit}
    """)


def load_subreddit_breakdown(category=None, days=7):
    where = f"WHERE scraped_at >= NOW() - INTERVAL '{days} days'"
    if category:
        where += f" AND category = '{category}'"
    return query(f"""
        SELECT
            subreddit,
            COUNT(*) as posts,
            AVG(sentiment_score) as avg_sentiment,
            SUM(upvotes) as total_upvotes,
            SUM(CASE WHEN is_hot THEN 1 ELSE 0 END) as hot_posts
        FROM reddit_posts
        {where}
        GROUP BY subreddit
        ORDER BY posts DESC
        LIMIT 10
    """)


def load_recent_activity():
    return query("""
        SELECT
            'reddit' as source,
            MAX(scraped_at) as last_update,
            COUNT(*) as records_24h
        FROM reddit_posts
        WHERE scraped_at >= NOW() - INTERVAL '24 hours'
        UNION ALL
        SELECT
            'google' as source,
            MAX(scraped_at) as last_update,
            COUNT(*) as records_24h
        FROM google_results
        WHERE scraped_at >= NOW() - INTERVAL '24 hours'
    """)


# ─── CHART HELPERS ───────────────────────────────────────────────────────────

CHART_THEME = dict(
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    font=dict(family='Space Mono, monospace', color='#64748b', size=11),
    xaxis=dict(gridcolor='#1a1a2e', linecolor='#1a1a2e', tickcolor='#1a1a2e'),
    yaxis=dict(gridcolor='#1a1a2e', linecolor='#1a1a2e', tickcolor='#1a1a2e'),
    margin=dict(l=10, r=10, t=30, b=10),
)


def sentiment_color(val):
    if val > 0.1:
        return '#00ff88'
    elif val < -0.1:
        return '#f43f5e'
    return '#64748b'


def render_metric_card(value, label, delta=None, delta_positive=True):
    delta_html = ""
    if delta is not None:
        cls = "metric-delta" if delta_positive else "metric-delta neg"
        arrow = "↑" if delta_positive else "↓"
        delta_html = f'<div class="{cls}">{arrow} {delta}</div>'

    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{value}</div>
        <div class="metric-label">{label}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


def render_feed_item(title, meta, badge_html="", url=None):
    link = f'<a href="{url}" target="_blank" style="color:#0ea5e9;font-size:0.75rem;text-decoration:none;">↗ open</a>' if url else ''
    st.markdown(f"""
    <div class="feed-item">
        <div class="feed-title">{title}</div>
        <div class="feed-meta">{meta} &nbsp; {badge_html} &nbsp; {link}</div>
    </div>
    """, unsafe_allow_html=True)


def sentiment_badge(label):
    if label == 'positive':
        return '<span class="signal-pos">+ POS</span>'
    elif label == 'negative':
        return '<span class="signal-neg">- NEG</span>'
    return '<span class="signal-neu">~ NEU</span>'


# ─── SIDEBAR ─────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown('<div class="dash-header" style="font-size:1.2rem">⚡ Signal Engine</div>', unsafe_allow_html=True)
    st.markdown('<div class="dash-sub" style="font-size:0.7rem">v1.0 · live data</div>', unsafe_allow_html=True)

    st.markdown("---")

    lang = st.radio("🌐", ["RU", "EN"], horizontal=True, index=0)
    RU = lang == "RU"
    DARK = st.toggle("🌙 Dark mode", value=True, key="theme_dark")

    st.markdown("---")

    category = st.selectbox(
        "КАТЕГОРИЯ" if RU else "CATEGORY",
        options=["all", "crypto", "amazon", "geopolitics", "ai_tech", "macro", "regulations", "ecommerce"],
        format_func=lambda x: {
            "all":         "🌐 Все рынки"       if RU else "🌐 All Markets",
            "crypto":      "₿ Крипто / Solana"  if RU else "₿ Crypto / Solana",
            "amazon":      "📦 Amazon FBA",
            "geopolitics": "🏛 Геополитика"      if RU else "🏛 Geopolitics",
            "ai_tech":     "🤖 AI / Tech",
            "macro":       "📈 Макро / Финансы"  if RU else "📈 Macro / Finance",
            "regulations": "⚖️ Регуляции"        if RU else "⚖️ Regulations",
            "ecommerce":   "🛒 E-commerce",
        }[x]
    )
    cat_filter = None if category == "all" else category
    if st.session_state.get("selected_cat"):
        cat_filter = st.session_state.selected_cat
    # Клик по карточке категории перекрывает sidebar фильтр
    if st.session_state.get("selected_cat"):
        cat_filter = st.session_state.selected_cat

    days = st.select_slider(
        "ПЕРИОД" if RU else "TIME WINDOW",
        options=[1, 3, 7, 14, 30],
        value=7,
        format_func=lambda x: f"{x}д" if RU else f"{x}d"
    )

    st.markdown("---")

    show_hot_only = st.checkbox("🔥 Только горячие" if RU else "🔥 Hot signals only", value=False)
    show_negative = st.checkbox("⚠️ Негативные" if RU else "⚠️ Negative signals", value=False)

    st.markdown("---")

    st.markdown(f'<div class="section-title">{"Статус пайплайна" if RU else "Pipeline Status"}</div>', unsafe_allow_html=True)
    activity = load_recent_activity()
    if not activity.empty:
        for _, row in activity.iterrows():
            last = pd.to_datetime(row['last_update'])
            mins_ago = int((datetime.now() - last.replace(tzinfo=None)).total_seconds() / 60) if pd.notna(last) else 999
            status = "🟢" if mins_ago < 360 else "🟡" if mins_ago < 720 else "🔴"
            ago_label = f"{mins_ago}м назад" if RU else f"{mins_ago}m ago"
            rec_label = "записей" if RU else "records"
            st.markdown(f"""
            <div style="font-family: Space Mono, monospace; font-size: 0.72rem; color: #64748b; margin-bottom: 0.4rem;">
                {status} {row['source'].upper()}<br>
                <span style="color: #334155">{row['records_24h']} {rec_label} · {ago_label}</span>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.markdown('<div style="font-size:0.75rem; color: #f43f5e;">⚠️ No DB connection</div>', unsafe_allow_html=True)

    st.markdown("---")

    if st.button("🔄 " + ("Обновить" if RU else "Refresh"), use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ─── MAIN HEADER ─────────────────────────────────────────────────────────────

# Category emoji map
CAT_EMOJI = {
    "all": "🌐", "crypto": "₿", "amazon": "📦",
    "geopolitics": "🏛", "ai_tech": "🤖", "macro": "📈",
    "regulations": "⚖️", "ecommerce": "🛒",
}
cat_emoji = CAT_EMOJI.get(category, "🌐")


# ── Light theme override ──────────────────────────
if not DARK:
    st.markdown("""<style>
    .stApp, .main, [data-testid="stAppViewContainer"],
    section[data-testid="stSidebar"] {
        background-color: #f1f5f9 !important;
    }
    .metric-card {
        background: #ffffff !important;
        border-color: #cbd5e1 !important;
        box-shadow: 0 1px 4px rgba(0,0,0,0.08) !important;
    }
    .metric-value { color: #0f172a !important; }
    .metric-label { color: #64748b !important; }
    .metric-delta { color: #16a34a !important; }
    .feed-item {
        background: #ffffff !important;
        border-color: #e2e8f0 !important;
    }
    .feed-title  { color: #0f172a !important; }
    .feed-meta   { color: #64748b !important; }
    .section-title {
        color: #334155 !important;
        border-color: #e2e8f0 !important;
    }
    .dash-header { color: #0f172a !important; }
    .dash-sub    { color: #64748b !important; }
    .stMarkdown p, .stMarkdown span,
    .stMarkdown div { color: #0f172a !important; }
    /* Category cards */
    div[style*="background:#0d1117"],
    div[style*="background: #0d1117"],
    div[style*="background:#0f172a"],
    div[style*="background: #0f172a"],
    div[style*="background:#030712"],
    div[style*="background: #030712"] {
        background: #ffffff !important;
    }
    /* All dark inline backgrounds */
    [style*="background:#0"] { background: #ffffff !important; }
    [style*="background: #0"] { background: #f8fafc !important; }
    [style*="color:#94a3b8"] { color: #475569 !important; }
    [style*="color:#64748b"] { color: #475569 !important; }
    [style*="color:#e2e8f0"] { color: #0f172a !important; }
    [style*="color:#f1f5f9"] { color: #0f172a !important; }
    [style*="color:#cbd5e1"] { color: #334155 !important; }
    [style*="color:#475569"] { color: #334155 !important; }
    [style*="color:#1e293b"] { color: #0f172a !important; }
    [style*="color:#0ea5e9"] { color: #0284c7 !important; }
    /* Tabs */
    .stTabs [data-baseweb="tab"] {
        background: #ffffff !important;
        border-color: #e2e8f0 !important;
        color: #475569 !important;
    }
    .stTabs [aria-selected="true"] {
        background: #f0fdf4 !important;
        border-color: #16a34a !important;
        color: #16a34a !important;
    }
    .stTabs [data-baseweb="tab-highlight"] {
        background: #16a34a !important;
    }
    /* Section title text */
    .section-title { color: #334155 !important; }
    /* All dark inline backgrounds → white */
    [style*="background:#0d1117"] { background: #ffffff !important; }
    [style*="background:#111118"] { background: #ffffff !important; }
    [style*="background:#0f172a"] { background: #f8fafc !important; }
    [style*="background:#030712"] { background: #f8fafc !important; }
    /* Dark text colors → readable on white */
    [style*="color:#94a3b8"] { color: #334155 !important; }
    [style*="color:#e2e8f0"] { color: #0f172a !important; }
    [style*="color:#f1f5f9"] { color: #0f172a !important; }
    [style*="color:#cbd5e1"] { color: #334155 !important; }
    [style*="color:#1e293b"] { color: #0f172a !important; }
    /* Feed & card borders */
    [style*="border:1px solid #1e1e2e"] { border-color: #e2e8f0 !important; }
    [style*="border-color:#1e2e"] { border-color: #e2e8f0 !important; }
    </style>""", unsafe_allow_html=True)

st.markdown(f"""
<div class="dash-header">SIGNAL ENGINE</div>
<div class="dash-sub">{cat_emoji} {category.upper()} · {days}{"Д" if RU else "D"} {"ПЕРИОД" if RU else "WINDOW"} · {datetime.now().strftime("%d %b %Y · %H:%M UTC")}</div>
""", unsafe_allow_html=True)


# ─── TOP METRICS ─────────────────────────────────────────────────────────────

reddit_stats = load_reddit_stats(category=cat_filter, days=days)
google_stats = load_google_stats(category=cat_filter, days=days)

# ─── CATEGORY CARDS (only on "all") ──────────────────────────────────────────
if category == "all":
    CAT_INFO = [
        ("₿",  "crypto",      "Крипто"       if RU else "Crypto",      "#00ff88"),
        ("📦", "amazon",      "Amazon FBA",                             "#f59e0b"),
        ("🏛", "geopolitics", "Геополитика"  if RU else "Geopolitics", "#f43f5e"),
        ("🤖", "ai_tech",     "AI / Tech",                              "#a78bfa"),
        ("📈", "macro",       "Макро"        if RU else "Macro",        "#0ea5e9"),
        ("⚖️", "regulations", "Регуляции"    if RU else "Regulations",  "#fb923c"),
        ("🛒", "ecommerce",   "E-commerce",                             "#34d399"),
    ]
    cat_cols = st.columns(7)
    # Theme-aware colors for category cards
    _card_bg    = "#111118" if DARK else "#ffffff"
    _card_brd   = "#1e1e2e" if DARK else "#e2e8f0"
    _label_clr  = "#64748b" if DARK else "#475569"
    _meta_clr   = "#475569" if DARK else "#64748b"

    for i, (emoji, cat_key, cat_label, color) in enumerate(CAT_INFO):
        s = load_reddit_stats(category=cat_key, days=days)
        total = int(s['total'].iloc[0]) if not s.empty and pd.notna(s['total'].iloc[0]) else 0
        sent  = float(s['avg_sentiment'].iloc[0]) if not s.empty and pd.notna(s['avg_sentiment'].iloc[0]) else 0
        hot   = int(s['hot_count'].iloc[0]) if not s.empty and pd.notna(s['hot_count'].iloc[0]) else 0
        arrow = "↑" if sent > 0.05 else "↓" if sent < -0.05 else "→"
        is_selected = st.session_state.selected_cat == cat_key
        border_style = f"border:2px solid {color};" if is_selected else f"border:1px solid {_card_brd}; border-top:2px solid {color};"
        with cat_cols[i]:
            st.markdown(f"""
            <div style="background:{_card_bg}; {border_style}
                        border-radius:8px; padding:0.8rem; text-align:center; margin-bottom:0.2rem;">
                <div style="font-size:1.4rem">{emoji}</div>
                <div style="font-family:Space Mono; font-size:0.65rem; color:{_label_clr}; margin:0.2rem 0">{cat_label.upper()}</div>
                <div style="font-family:Space Mono; font-size:1rem; font-weight:700; color:{color}">{total}</div>
                <div style="font-size:0.7rem; color:{_meta_clr}">{arrow} {sent:+.2f} · 🔥{hot}</div>
            </div>
            """, unsafe_allow_html=True)
            btn_label = "✕ Сброс" if is_selected else cat_label
            if st.button(btn_label, key=f"cat_btn_{cat_key}", use_container_width=True):
                st.session_state.selected_cat = None if is_selected else cat_key
                st.rerun()
    st.markdown("<br>", unsafe_allow_html=True)

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    total = int(reddit_stats['total'].iloc[0]) if not reddit_stats.empty and pd.notna(reddit_stats['total'].iloc[0]) else 0
    render_metric_card(f"{total:,}", "Reddit Posts")

with col2:
    hot = int(reddit_stats['hot_count'].iloc[0]) if not reddit_stats.empty and pd.notna(reddit_stats['hot_count'].iloc[0]) else 0
    render_metric_card(f"{hot}", "🔥 Hot Signals")

with col3:
    sentiment = float(reddit_stats['avg_sentiment'].iloc[0]) if not reddit_stats.empty and pd.notna(reddit_stats['avg_sentiment'].iloc[0]) else 0
    emoji = "📈" if sentiment > 0.05 else "📉" if sentiment < -0.05 else "➡️"
    render_metric_card(f"{sentiment:+.3f}", f"{emoji} Avg Sentiment", delta_positive=sentiment >= 0)

with col4:
    g_total = int(google_stats['total'].iloc[0]) if not google_stats.empty and pd.notna(google_stats['total'].iloc[0]) else 0
    render_metric_card(f"{g_total:,}", "Google Results")

with col5:
    g_queries = int(google_stats['unique_queries'].iloc[0]) if not google_stats.empty and pd.notna(google_stats['unique_queries'].iloc[0]) else 0
    render_metric_card(f"{g_queries}", "Active Queries")

st.markdown("<br>", unsafe_allow_html=True)


# ─── MAIN TABS ───────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📡 " + ("Лента" if RU else "Live Feed"),
    "📊 " + ("Аналитика" if RU else "Analytics"),
    "🔑 " + ("Ключевые слова" if RU else "Keywords"),
    "🔍 " + ("Google Интел" if RU else "Google Intel"),
    "🤖 AI " + ("Инсайты" if RU else "Insights"),
    "📅 " + ("История" if RU else "History"),
    "🐋 " + ("Киты" if RU else "Whales"),
])


# ────────── TAB 1: LIVE FEED ────────────────────────────────────────────────

with tab1:
    col_a, col_b = st.columns([3, 1])

    with col_a:
        st.markdown(f'<div class="section-title">{"Reddit Лента сигналов" if RU else "Reddit Signal Feed"}</div>', unsafe_allow_html=True)

        feed = load_reddit_feed(
            category=cat_filter,
            days=days,
            limit=30,
            only_hot=show_hot_only
        )

        if show_negative and not feed.empty:
            feed = feed[feed['sentiment_label'] == 'negative']

        # ─── ПОИСК ───────────────────────────────────────────
        search_q = st.text_input(
            "🔍 " + ("Поиск по ленте..." if RU else "Search feed..."),
            value="",
            key="feed_search",
            label_visibility="collapsed",
            placeholder="🔍 " + ("Поиск по теме, сабреддиту..." if RU else "Search by topic, subreddit..."),
        )
        if search_q:
            mask = (
                feed['title'].str.contains(search_q, case=False, na=False) |
                feed['subreddit'].str.contains(search_q, case=False, na=False)
            )
            feed = feed[mask]
            st.markdown(
                f'<div style="font-family:Space Mono; font-size:0.7rem; color:#64748b; margin-bottom:0.5rem;">'
                f'{"Найдено" if RU else "Found"}: {len(feed)} {"постов" if RU else "posts"} · "{search_q}"</div>',
                unsafe_allow_html=True
            )

        if feed.empty:
            st.markdown('<div style="color:#475569; font-size:0.85rem; padding: 2rem 0;">' + ('Нет данных. Запустите reddit_collector.py' if RU else 'No data yet. Run reddit_collector.py to populate.') + '</div>', unsafe_allow_html=True)
        else:
            _junk_patterns = [
                r"Вот краткое резюме[^:]*:\s*\n?",
                r"Краткое резюме[^:]*:\s*\n?",
                r"\*\*Краткое резюме\*\*[^:]*:\s*\n?",
                r"\*\*[^*]+\*\*:?\s*",
                r"\*\*(.*?)\*\*",
            ]
            def clean_summary(txt):
                txt = str(txt or '').strip()
                for p in _junk_patterns:
                    txt = re.sub(p, '', txt, flags=re.IGNORECASE).strip()
                return txt

            _sbg = "#0d1117" if DARK else "#f0fdf4"
            _stc = "#94a3b8" if DARK else "#334155"

            for i, (_, row) in enumerate(feed.iterrows()):
                hot_badge = '<span class="signal-hot">🔥 HOT</span> ' if row.get('is_hot') else ''
                s_badge = sentiment_badge(row.get('sentiment_label', 'neutral'))
                meta = f"r/{row['subreddit']} · ↑{row['upvotes']:,} · 💬{row['comments_count']} · {pd.to_datetime(row['scraped_at']).strftime('%d %b %H:%M')}"
                render_feed_item(
                    title=row['title'],
                    meta=meta,
                    badge_html=f"{hot_badge}{s_badge}",
                    url=row.get('url')
                )
                ai_text = clean_summary(row.get('ai_summary', ''))
                # Если резюме нет — генерим на лету для первых 10
                if not ai_text and i < 10:
                    _prompt = (
                        f"2 предложения на русском. Без вступлений, без markdown.\n"
                        f"1) Что произошло. 2) Сигнал для рынка/бизнеса.\n\n"
                        f"Пост: {row['title']}\n"
                        f"r/{row['subreddit']} | {row.get('upvotes',0):,} апвоутов"
                    )
                    _cache_key = f"sum_{hash(row['title'])}"
                    if _cache_key not in st.session_state:
                        time.sleep(0.5)
                        st.session_state[_cache_key] = clean_summary(call_gemini(_prompt))
                    ai_text = st.session_state[_cache_key]
                if ai_text:
                    st.markdown(
                        f'<div style="background:{_sbg};border-left:2px solid #00ff88;'
                        f'border-radius:4px;padding:0.6rem 1rem;margin:-0.3rem 0 0.5rem 0;'
                        f'font-size:0.82rem;color:{_stc};line-height:1.6;">'
                        f'🤖 {ai_text}</div>',
                        unsafe_allow_html=True
                    )

    with col_b:
        st.markdown('<div class="section-title">Sentiment Mix</div>', unsafe_allow_html=True)

        if not reddit_stats.empty:
            pos = int(reddit_stats['positive'].iloc[0] or 0)
            neg = int(reddit_stats['negative'].iloc[0] or 0)
            neu = int(reddit_stats['neutral'].iloc[0] or 0)

            if pos + neg + neu > 0:
                fig = go.Figure(go.Pie(
                    labels=['Positive', 'Negative', 'Neutral'],
                    values=[pos, neg, neu],
                    hole=0.65,
                    marker=dict(
                        colors=['#00ff88', '#f43f5e', '#334155'],
                        line=dict(color='#0a0a0f', width=2)
                    ),
                    textinfo='percent',
                    textfont=dict(family='Space Mono', size=10, color='white'),
                ))
                fig.update_layout(
                    **CHART_THEME,
                    showlegend=True,
                    legend=dict(
                        font=dict(family='Space Mono', size=9, color='#64748b'),
                        bgcolor='rgba(0,0,0,0)',
                    ),
                    height=220,
                )
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

        st.markdown('<div class="section-title" style="margin-top:1rem">Top Subreddits</div>', unsafe_allow_html=True)
        subs = load_subreddit_breakdown(category=cat_filter, days=days)
        if not subs.empty:
            for _, row in subs.head(6).iterrows():
                pct = int(row['avg_sentiment'] * 100) if pd.notna(row['avg_sentiment']) else 0
                color = '#00ff88' if pct > 0 else '#f43f5e' if pct < 0 else '#475569'
                st.markdown(f"""
                <div style="display:flex; justify-content:space-between; align-items:center;
                            padding: 0.4rem 0; border-bottom: 1px solid #1a1a2e;">
                    <span style="font-size:0.8rem; color: #94a3b8;">r/{row['subreddit']}</span>
                    <span style="font-family: Space Mono; font-size: 0.75rem; color: {color};">{pct:+d}%</span>
                </div>
                """, unsafe_allow_html=True)


# ────────── TAB 2: ANALYTICS ────────────────────────────────────────────────

with tab2:
    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown('<div class="section-title">Sentiment Timeline</div>', unsafe_allow_html=True)
        timeline = load_sentiment_timeline(category=cat_filter, days=days * 2)

        if timeline.empty or len(timeline) == 0:
            st.markdown('<div style="color:#475569; font-size:0.85rem; padding: 1rem 0;">No timeline data yet.</div>', unsafe_allow_html=True)
        else:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=timeline['date'],
                y=timeline['avg_sentiment'],
                mode='lines+markers',
                line=dict(color='#00ff88', width=2),
                marker=dict(color='#00ff88', size=5),
                fill='tozeroy',
                fillcolor='rgba(0,255,136,0.05)',
                name='Sentiment',
            ))
            fig.add_hline(y=0, line=dict(color='#1e1e2e', dash='dash', width=1))
            fig.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(family='Space Mono, monospace', color='#64748b', size=11),
                xaxis=dict(gridcolor='#1a1a2e', linecolor='#1a1a2e', tickcolor='#1a1a2e'),
                yaxis=dict(gridcolor='#1a1a2e', linecolor='#1a1a2e', tickcolor='#1a1a2e',
                           tickformat='.3f', range=[-1, 1]),
                margin=dict(l=10, r=10, t=30, b=10),
                height=250,
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

    with col_right:
        st.markdown('<div class="section-title">Post Volume + Hot Signals</div>', unsafe_allow_html=True)

        if not timeline.empty:
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=timeline['date'],
                y=timeline['post_count'],
                marker_color='#1e3a5f',
                name='Total Posts',
            ))
            fig2.add_trace(go.Bar(
                x=timeline['date'],
                y=timeline['hot_count'],
                marker_color='#f43f5e',
                name='🔥 Hot',
            ))
            fig2.update_layout(
                **CHART_THEME,
                height=250,
                barmode='overlay',
                showlegend=True,
                legend=dict(
                    font=dict(family='Space Mono', size=9, color='#64748b'),
                    bgcolor='rgba(0,0,0,0)',
                    orientation='h',
                    x=0, y=1.1,
                ),
            )
            st.plotly_chart(fig2, use_container_width=True, config={'displayModeBar': False})

    # Subreddit heatmap
    st.markdown('<div class="section-title" style="margin-top:1rem">Subreddit Intelligence</div>', unsafe_allow_html=True)
    subs = load_subreddit_breakdown(category=cat_filter, days=days)

    if not subs.empty:
        fig3 = go.Figure(go.Bar(
            x=subs['subreddit'],
            y=subs['posts'],
            marker=dict(
                color=subs['avg_sentiment'],
                colorscale=[[0, '#f43f5e'], [0.5, '#334155'], [1, '#00ff88']],
                cmin=-0.5,
                cmax=0.5,
                colorbar=dict(
                    title=dict(text='Sentiment', font=dict(color='#475569', size=9)),
                    tickfont=dict(color='#475569', size=9),
                    thickness=8,
                ),
                showscale=True,
            ),
            text=subs['posts'],
            textposition='auto',
            textfont=dict(family='Space Mono', size=9, color='white'),
        ))
        fig3.update_layout(
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family='Space Mono, monospace', color='#64748b', size=11),
            xaxis=dict(gridcolor='#1a1a2e', linecolor='#1a1a2e', tickcolor='#1a1a2e', tickangle=-30),
            yaxis=dict(gridcolor='#1a1a2e', linecolor='#1a1a2e', tickcolor='#1a1a2e'),
            margin=dict(l=10, r=10, t=30, b=10),
            height=280,
        )
        st.plotly_chart(fig3, use_container_width=True, config={'displayModeBar': False})


# ────────── TAB 3: KEYWORDS ─────────────────────────────────────────────────

with tab3:
    st.markdown('<div class="section-title">Top Keywords by Mentions</div>', unsafe_allow_html=True)

    kw = load_top_keywords(category=cat_filter, days=days, limit=30)

    if kw.empty:
        st.markdown('<div style="color:#475569; font-size:0.85rem; padding: 1rem 0;">No keyword data yet. Requires reddit_posts with keywords_found populated.</div>', unsafe_allow_html=True)
    else:
        col_kw1, col_kw2 = st.columns([2, 1])

        with col_kw1:
            kw['color'] = kw['avg_sentiment'].apply(
                lambda x: '#00ff88' if x > 0.1 else '#f43f5e' if x < -0.1 else '#475569'
            )
            fig_kw = go.Figure(go.Bar(
                x=kw['keyword'],
                y=kw['mentions'],
                marker=dict(
                    color=kw['avg_sentiment'],
                    colorscale=[[0, '#f43f5e'], [0.5, '#334155'], [1, '#00ff88']],
                    cmin=-0.5,
                    cmax=0.5,
                ),
                text=kw['mentions'],
                textposition='auto',
                textfont=dict(family='Space Mono', size=9, color='white'),
            ))
            fig_kw.update_layout(
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                font=dict(family='Space Mono, monospace', color='#64748b', size=11),
                xaxis=dict(gridcolor='#1a1a2e', linecolor='#1a1a2e', tickcolor='#1a1a2e', tickangle=-40),
                yaxis=dict(gridcolor='#1a1a2e', linecolor='#1a1a2e', tickcolor='#1a1a2e'),
                margin=dict(l=10, r=10, t=30, b=10),
                height=320,
            )
            st.plotly_chart(fig_kw, use_container_width=True, config={'displayModeBar': False})

        with col_kw2:
            st.markdown('<div class="section-title">Keyword Table</div>', unsafe_allow_html=True)
            display_kw = kw[['keyword', 'mentions', 'avg_sentiment', 'total_upvotes']].copy()
            display_kw['avg_sentiment'] = display_kw['avg_sentiment'].round(3)
            display_kw['total_upvotes'] = display_kw['total_upvotes'].fillna(0).astype(int)
            display_kw.columns = ['Keyword', 'Mentions', 'Sentiment', 'Upvotes']
            st.dataframe(
                display_kw,
                use_container_width=True,
                height=320,
                hide_index=True,
            )


# ────────── TAB 4: GOOGLE INTEL ─────────────────────────────────────────────

with tab4:
    col_g1, col_g2 = st.columns([3, 1])

    with col_g1:
        st.markdown('<div class="section-title">Google Search Intelligence</div>', unsafe_allow_html=True)

        gfeed = load_google_feed(category=cat_filter, days=days, limit=30)

        if gfeed.empty:
            st.markdown('<div style="color:#475569; font-size:0.85rem; padding: 1rem 0;">No Google data yet. Run google_collector.py to populate.</div>', unsafe_allow_html=True)
        else:
            for _, row in gfeed.iterrows():
                rtype = row.get('result_type', 'organic')
                type_colors = {
                    'forums': '#f59e0b',
                    'organic': '#0ea5e9',
                    'questions': '#a78bfa',
                    'related_searches': '#34d399',
                    'ads': '#f43f5e',
                }
                color = type_colors.get(rtype, '#475569')
                type_badge = f'<span style="background: rgba(100,116,139,0.1); border: 1px solid {color}33; color: {color}; font-size: 0.68rem; font-family: Space Mono; padding: 1px 7px; border-radius: 3px;">{rtype.upper()}</span>'

                meta = f"#{row.get('position', '?')} · {row.get('query', '')} · {pd.to_datetime(row['scraped_at']).strftime('%d %b %H:%M')}"
                render_feed_item(
                    title=str(row.get('title', row.get('snippet', 'No title')))[:120],
                    meta=meta,
                    badge_html=type_badge,
                    url=row.get('url')
                )

    with col_g2:
        st.markdown('<div class="section-title">Result Types</div>', unsafe_allow_html=True)

        if not google_stats.empty:
            forums = int(google_stats['forums'].iloc[0] or 0)
            organic = int(google_stats['organic'].iloc[0] or 0)
            other = max(0, int(google_stats['total'].iloc[0] or 0) - forums - organic)

            if forums + organic + other > 0:
                fig_gt = go.Figure(go.Pie(
                    labels=['Forums', 'Organic', 'Other'],
                    values=[forums, organic, other],
                    hole=0.6,
                    marker=dict(
                        colors=['#f59e0b', '#0ea5e9', '#475569'],
                        line=dict(color='#0a0a0f', width=2)
                    ),
                    textinfo='percent',
                    textfont=dict(family='Space Mono', size=10, color='white'),
                ))
                fig_gt.update_layout(
                    **CHART_THEME,
                    showlegend=True,
                    legend=dict(
                        font=dict(family='Space Mono', size=9, color='#64748b'),
                        bgcolor='rgba(0,0,0,0)',
                    ),
                    height=220,
                )
                st.plotly_chart(fig_gt, use_container_width=True, config={'displayModeBar': False})

        st.markdown('<div class="section-title" style="margin-top:1rem">Query Stats</div>', unsafe_allow_html=True)
        if not google_stats.empty:
            for label, val in [
                ("Total Results", google_stats['total'].iloc[0]),
                ("Unique Queries", google_stats['unique_queries'].iloc[0]),
                ("Forum Hits", google_stats['forums'].iloc[0]),
            ]:
                st.markdown(f"""
                <div style="display:flex; justify-content:space-between;
                            padding: 0.4rem 0; border-bottom: 1px solid #1a1a2e;">
                    <span style="font-size:0.78rem; color: #64748b;">{label}</span>
                    <span style="font-family: Space Mono; font-size: 0.78rem; color: #94a3b8;">{int(val or 0):,}</span>
                </div>
                """, unsafe_allow_html=True)


# ────────── TAB 5: AI INSIGHTS ─────────────────────────────────────────────

with tab5:
    st.markdown(f'<div class="section-title">{"AI Анализ · Gemini" if RU else "AI Analysis · Gemini"}</div>', unsafe_allow_html=True)

    col_ai1, col_ai2 = st.columns([2, 1])

    with col_ai2:
        _midx = GEMINI_MODELS_LIST.index(st.session_state.gemini_model) if st.session_state.gemini_model in GEMINI_MODELS_LIST else 0
        _sel = st.selectbox(
            "🤖 Модель" if RU else "🤖 Model",
            GEMINI_MODELS_LIST,
            index=_midx,
            key="model_select_ai"
        )
        if _sel != st.session_state.gemini_model:
            st.session_state.gemini_model = _sel

        ai_category = st.selectbox(
            "Анализировать" if RU else "Analyze",
            options=["all", "crypto", "amazon", "geopolitics", "ai_tech", "macro", "regulations", "ecommerce"],
            format_func=lambda x: {
                "all":         "🌐 Всё"         if RU else "🌐 All",
                "crypto":      "₿ Крипто"       if RU else "₿ Crypto",
                "amazon":      "📦 Amazon FBA",
                "geopolitics": "🏛 Геополитика" if RU else "🏛 Geopolitics",
                "ai_tech":     "🤖 AI / Tech",
                "macro":       "📈 Макро"        if RU else "📈 Macro",
                "regulations": "⚖️ Регуляции"   if RU else "⚖️ Regulations",
                "ecommerce":   "🛒 E-commerce",
            }[x],
            key="ai_cat"
        )

        generate_btn = st.button(
            "⚡ " + ("Сгенерировать инсайт" if RU else "Generate Insight"),
            use_container_width=True,
            type="primary"
        )

        st.markdown(f"""
        <div style="font-size:0.72rem; color:#334155; margin-top:1rem; line-height:1.6;">
            {"Анализирует горячие посты за 24ч и генерирует экспертный вывод уровня хедж-фонда." if RU else "Analyzes hot posts from last 24h and generates hedge-fund grade insights."}
        </div>
        """, unsafe_allow_html=True)

    with col_ai1:
        if generate_btn:
            cat_param = None if ai_category == "all" else ai_category
            with st.spinner("🤖 " + ("Анализирую сигналы..." if RU else "Analyzing signals...")):
                insights = generate_insights(category=cat_param, lang=lang, days=days, model=st.session_state.get("gemini_model", GEMINI_MODEL_DEFAULT))

            if "error" in insights:
                st.error(insights["error"])
            else:
                st.markdown(f'<div style="font-size:0.72rem; color:#475569; font-family: Space Mono; margin-bottom:1rem;">'
                    + ("Сгенерировано:" if RU else "Generated:") + f' {insights.get("generated_at","")} · {insights.get("posts_analyzed",0)} ' 
                    + ("постов проанализировано" if RU else "posts analyzed") + '</div>', unsafe_allow_html=True)

                # Color/emoji map for all categories
                CAT_DISPLAY = {
                    "summary":     ("🌐", "#00ff88", "EXECUTIVE SUMMARY"),
                    "crypto":      ("₿",  "#00ff88", "CRYPTO"),
                    "amazon":      ("📦", "#f59e0b", "AMAZON FBA"),
                    "geopolitics": ("🏛", "#f43f5e", "ГЕОПОЛИТИКА"),
                    "ai_tech":     ("🤖", "#a78bfa", "AI / TECH"),
                    "macro":       ("📈", "#0ea5e9", "МАКРО"),
                    "regulations": ("⚖️", "#fb923c", "РЕГУЛЯЦИИ"),
                    "ecommerce":   ("🛒", "#34d399", "E-COMMERCE"),
                }

                for key, (emoji, color, label) in CAT_DISPLAY.items():
                    if key in insights and key not in ("generated_at", "posts_analyzed", "error"):
                        text = insights[key].replace(chr(10), "<br>")
                        st.markdown(f"""
                        <div class="feed-item" style="border-color:{color}33; margin-bottom:1rem;">
                            <div style="font-family:Space Mono; font-size:0.7rem; color:{color}; margin-bottom:0.5rem;">
                                {emoji} {label}
                            </div>
                            <div style="font-size:0.88rem; color:#e2e8f0; line-height:1.8;">
                                {text}
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div style="color:#334155; font-size:0.9rem; padding: 3rem 0; text-align:center; font-family: Space Mono;">
                {"← Нажми кнопку для генерации AI инсайта" if RU else "← Click button to generate AI insight"}
            </div>
            """, unsafe_allow_html=True)


# ─── FOOTER ──────────────────────────────────────────────────────────────────

st.markdown("---")
st.markdown("""
<div style="display:flex; justify-content:space-between; align-items:center;
            font-family: Space Mono, monospace; font-size: 0.68rem; color: #334155; padding: 0.5rem 0;">
    <span>⚡ SIGNAL ENGINE · MR.EQUIPP LIMITED</span>
    <span>CRYPTO + AMAZON INTELLIGENCE LAYER</span>
    <span>BUILT ON POSTGRESQL · STREAMLIT · CLAUDE API</span>
</div>
""", unsafe_allow_html=True)

with tab6:
    st.markdown(f'<div class="section-title">{"📅 История недель" if RU else "📅 Weekly History"}</div>', unsafe_allow_html=True)

    try:
        conn_h = get_engine().connect()
        history_df = pd.read_sql("""
            SELECT week_start, week_end, category, total_posts, hot_posts,
                   avg_sentiment, ai_summary
            FROM weekly_summaries
            ORDER BY week_start DESC, category
        """, conn_h)
        if conn_h: conn_h.close()
    except:
        history_df = pd.DataFrame()

    if history_df.empty:
        st.markdown("""
        <div style="color:#475569; font-size:0.85rem; padding:2rem 0; text-align:center;">
            История пока пуста.<br>
            Запусти <code>python weekly_summary.py</code> чтобы сгенерировать первую сводку.
        </div>
        """, unsafe_allow_html=True)
    else:
        weeks = sorted(history_df['week_start'].unique(), reverse=True)
        CAT_COLORS = {
            "crypto": "#00ff88", "amazon": "#f59e0b", "geopolitics": "#f43f5e",
            "ai_tech": "#a78bfa", "macro": "#0ea5e9", "regulations": "#fb923c",
            "ecommerce": "#34d399",
        }
        CAT_EMOJI = {
            "crypto": "₿", "amazon": "📦", "geopolitics": "🏛",
            "ai_tech": "🤖", "macro": "📈", "regulations": "⚖️", "ecommerce": "🛒",
        }

        for week in weeks:
            week_data = history_df[history_df['week_start'] == week]
            week_dt   = pd.to_datetime(week)
            week_end  = pd.to_datetime(week_data['week_end'].iloc[0])

            st.markdown(f"""
            <div style="font-family:Space Mono; font-size:0.75rem; color:#64748b;
                        border-bottom:1px solid #1e1e2e; padding:0.5rem 0; margin:1rem 0 0.5rem 0;">
                📅 НЕДЕЛЯ: {week_dt.strftime('%d %b')} — {week_end.strftime('%d %b %Y')}
            </div>
            """, unsafe_allow_html=True)

            # Карточки категорий за неделю
            cols = st.columns(len(week_data))
            for i, (_, row) in enumerate(week_data.iterrows()):
                cat   = row['category']
                color = CAT_COLORS.get(cat, '#64748b')
                emoji = CAT_EMOJI.get(cat, '•')
                sent  = float(row['avg_sentiment'] or 0)
                arrow = "↑" if sent > 0.05 else "↓" if sent < -0.05 else "→"
                with cols[i]:
                    _hbg = "#111118" if DARK else "#ffffff"
                    _hbd = "#1e1e2e" if DARK else "#e2e8f0"
                    _hlb = "#64748b" if DARK else "#475569"
                    _hmt = "#475569" if DARK else "#64748b"
                    st.markdown(f"""
                    <div style="background:{_hbg}; border:1px solid {_hbd};
                                border-top:2px solid {color}; border-radius:6px;
                                padding:0.6rem; text-align:center; margin-bottom:0.3rem;">
                        <div style="font-size:1.1rem">{emoji}</div>
                        <div style="font-family:Space Mono; font-size:0.6rem; color:{_hlb}">{cat.upper()}</div>
                        <div style="font-family:Space Mono; font-size:0.9rem; color:{color}; font-weight:700">{int(row['total_posts'])}</div>
                        <div style="font-size:0.65rem; color:{_hmt}">{arrow}{sent:+.2f} 🔥{int(row['hot_posts'])}</div>
                    </div>
                    """, unsafe_allow_html=True)

            # AI сводки за неделю по категориям
            for _, row in week_data.iterrows():
                if not row['ai_summary']:
                    continue
                cat   = row['category']
                color = CAT_COLORS.get(cat, '#64748b')
                emoji = CAT_EMOJI.get(cat, '•')
                cat_label = {"crypto":"Крипто","amazon":"Amazon FBA","geopolitics":"Геополитика",
                              "ai_tech":"AI / Tech","macro":"Макро","regulations":"Регуляции","ecommerce":"E-commerce"}.get(cat, cat.upper())
                with st.expander(f"{emoji} {cat_label} — AI сводка недели"):
                    _stc = "#94a3b8" if DARK else "#334155"
                    st.markdown(f"""
                    <div style="font-size:0.85rem; color:{_stc}; line-height:1.8;">
                        {row['ai_summary'].replace(chr(10), '<br>')}
                    </div>
                    """, unsafe_allow_html=True)

with tab7:
    st.markdown(f'<div class="section-title">{"🐋 Whale Tracker — Solana" if RU else "🐋 Whale Tracker — Solana"}</div>', unsafe_allow_html=True)

    try:
        conn_w = get_engine().connect()
        whales_df = pd.read_sql("""
            SELECT wallet, grade, score, win_rate, profit_factor,
                   net_pnl, trader_type, verdict, source_text,
                   analyzed_at
            FROM whale_wallets
            ORDER BY score DESC
            LIMIT 50
        """, conn_w)
        if conn_w: conn_w.close()
    except:
        whales_df = pd.DataFrame()

    if whales_df.empty:
        st.markdown("""
        <div style="color:#475569; font-size:0.85rem; padding:2rem 0; text-align:center;">
            Данных нет.<br>
            Запусти <code>python whale_tracker.py</code> для поиска китов.
        </div>
        """, unsafe_allow_html=True)
    else:
        # Метрики
        total_w   = len(whales_df)
        grade_ab  = len(whales_df[whales_df['grade'].isin(['A','B'])])
        avg_wr    = whales_df['win_rate'].mean()
        avg_pnl   = whales_df['net_pnl'].mean()

        wc1, wc2, wc3, wc4 = st.columns(4)
        with wc1:
            render_metric_card(str(total_w), "КИТОВ В БАЗЕ")
        with wc2:
            render_metric_card(str(grade_ab), "GRADE A/B", delta=f"{grade_ab/total_w*100:.0f}%", delta_positive=True)
        with wc3:
            render_metric_card(f"{avg_wr:.1f}%", "AVG WIN RATE", delta_positive=avg_wr > 50)
        with wc4:
            render_metric_card(f"${avg_pnl:+,.0f}", "AVG NET PNL", delta_positive=avg_pnl > 0)

        st.markdown("<br>", unsafe_allow_html=True)

        # Фильтр по грейду
        grade_filter = st.select_slider(
            "Минимальный грейд" if RU else "Min Grade",
            options=["F", "C", "B", "A"],
            value="B",
            key="whale_grade_filter"
        )
        grade_order = {"A": 0, "B": 1, "C": 2, "F": 3}
        filtered = whales_df[
            whales_df['grade'].map(grade_order) <= grade_order.get(grade_filter, 3)
        ]

        # ─── AI ОБЩИЙ АНАЛИЗ ─────────────────────────────────────────
        if st.button("🤖 AI анализ всех китов" if RU else "🤖 AI analyze all whales", key="whale_ai"):
            with st.spinner("Gemini анализирует..."):
                top_str = "\n".join([
                    f"  [{r['grade']}] WR:{r['win_rate']:.0f}% PnL:${r['net_pnl']:+,.0f} {r['trader_type']} токен:{r.get('source_symbol','?')}"
                    for _, r in whales_df.head(10).iterrows()
                ])
                prompt = f"""Ты аналитик крипто хедж-фонда. Проанализируй пул Solana китов.

Всего китов: {total_w} | Grade A/B: {grade_ab} | Avg WR: {avg_wr:.1f}% | Avg PnL: ${avg_pnl:+,.0f}

Топ кошельки:
{top_str}

Ответь на русском (4-5 предложений):
## 🎯 ОБЩИЙ СИГНАЛ — что говорит активность китов
## 🔥 ТОП ТОКЕН — в какой токен идёт smart money
## ⚡ ДЕЙСТВИЕ — что делать трейдеру прямо сейчас"""
                result = call_gemini(prompt)
                if result:
                    st.markdown(f"""
                    <div class="feed-item" style="border-color:#00ff8833; margin-bottom:1rem;">
                        <div style="font-family:Space Mono;font-size:0.7rem;color:#00ff88;margin-bottom:0.5rem;">🤖 AI АНАЛИЗ КИТОВ</div>
                        <div style="font-size:0.88rem;color:#e2e8f0;line-height:1.8;">{result.replace(chr(10),"<br>")}</div>
                    </div>
                    """, unsafe_allow_html=True)

        st.markdown(f'<div class="section-title">{"ТОП КОШЕЛЬКИ" if RU else "TOP WALLETS"} · {len(filtered)}</div>', unsafe_allow_html=True)

        for wi, (_, row) in enumerate(filtered.iterrows()):
            grade_color = {"A": "#00ff88", "B": "#f59e0b", "C": "#f43f5e", "F": "#475569"}.get(row['grade'], "#475569")
            grade_emoji = {"A": "🏆", "B": "👍", "C": "⚠️", "F": "🚫"}.get(row['grade'], "❓")
            pnl_color   = "#00ff88" if row['net_pnl'] > 0 else "#f43f5e"
            solscan_url = f"https://solscan.io/account/{row['wallet']}"
            short_wallet = row['wallet'][:8] + "..." + row['wallet'][-6:]

            st.markdown(
                f'<div class="feed-item" style="border-color:{grade_color}33;">'
                f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                f'<span style="font-family:Space Mono;font-size:0.75rem;color:{grade_color};font-weight:700;">{grade_emoji} Grade {row["grade"]} · {int(row["score"])}/100</span>'
                f'<span style="font-family:Space Mono;font-size:0.7rem;color:#64748b;margin-left:1rem;">{row["trader_type"]}</span>'
                f'<a href="{solscan_url}" target="_blank" style="font-family:Space Mono;font-size:0.65rem;color:#0ea5e9;text-decoration:none;">↗ Solscan</a>'
                f'</div>'
                f'<div style="font-family:Space Mono;font-size:0.72rem;color:#94a3b8;margin:0.3rem 0;word-break:break-all;">{row["wallet"]}</div>'
                f'<div style="font-size:0.75rem;color:#64748b;margin-top:0.3rem;">'
                f'WR: <span style="color:#e2e8f0">{row["win_rate"]:.1f}%</span> &nbsp;'
                f'PF: <span style="color:#e2e8f0">{row["profit_factor"]:.2f}x</span> &nbsp;'
                f'PnL: <span style="color:{pnl_color}">${row["net_pnl"]:+,.0f}</span>'
                f'</div>'
                f'<div style="font-size:0.7rem;color:#475569;margin-top:0.3rem;font-style:italic;">{str(row.get("source_symbol","") or "")} · {str(row.get("source_text","") or "")[:80]}</div>'
                f'<div style="font-size:0.75rem;color:#94a3b8;margin-top:0.4rem;font-family:Space Mono;">📅 {str(row["analyzed_at"])[:16] if row.get("analyzed_at") else "—"}</div>'
                f'</div>',
                unsafe_allow_html=True
            )
            # AI анализ кошелька
            whale_btn_key = f"whale_ai_{wi}_{row['wallet'][:8]}"
            if st.button("🤖 Анализ — следить?", key=whale_btn_key):
                with st.spinner("Gemini анализирует кошелёк..."):
                    w_prompt = f"""Ты крипто аналитик. Дай чёткий совет по этому Solana кошельку.

Кошелёк: {row['wallet']}
Grade: {row['grade']} ({int(row['score'])}/100)
Win Rate: {row['win_rate']:.1f}%
Profit Factor: {row['profit_factor']:.2f}x
Net PnL (30д): ${row['net_pnl']:+,.0f}
Тип трейдера: {row['trader_type']}
Токен через который нашли: {str(row.get('source_symbol','')) or 'N/A'}

Ответь структурированно на русском:
1. СЛЕДИТЬ ИЛИ НЕТ — чёткий ответ да/нет и почему
2. СТИЛЬ — как торгует, что покупает
3. РИСК — главные риски копирования
4. РЕКОМЕНДАЦИЯ — конкретное действие"""
                    w_analysis = call_gemini(w_prompt)
                    if w_analysis:
                        txt = w_analysis.strip().replace("\n", "<br>")
                        st.markdown(
                            f'<div style="background:#0d1117;border-left:2px solid {grade_color};'
                            f'border-radius:4px;padding:0.8rem 1rem;margin:0.3rem 0 0.8rem 0;'
                            f'font-size:0.85rem;color:#94a3b8;line-height:1.8;">'
                            f'🤖 {txt}</div>',
                            unsafe_allow_html=True
                        )
