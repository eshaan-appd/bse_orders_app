import requests, pandas as pd, time, re
from datetime import datetime, date
import streamlit as st

# --------------------
# Backend (resilient fetcher)
# --------------------

HOME = "https://www.bseindia.com/"
CORP = "https://www.bseindia.com/corporates/ann.html"

ENDPOINTS = [
    "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w",
    "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w",
]

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": HOME,
    "Origin": "https://www.bseindia.com",
    "X-Requested-With": "XMLHttpRequest",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

def _call_once(s: requests.Session, url: str, params: dict):
    """One guarded call; returns (rows, total, meta)."""
    r = s.get(url, params=params, timeout=30)
    ct = r.headers.get("content-type","")
    if "application/json" not in ct:
        return [], None, {"blocked": True, "ct": ct, "status": r.status_code}
    data = r.json()
    rows = data.get("Table") or []
    total = None
    try:
        total = int((data.get("Table1") or [{}])[0].get("ROWCNT") or 0)
    except Exception:
        pass
    return rows, total, {}

def _fetch_single_range(s, d1: str, d2: str, log):
    """Fetch full date range without chunking."""
    search_opts = ["", "P"]
    seg_opts    = ["C", "E"]
    subcat_opts = ["", "-1"]
    pageno_keys = ["pageno", "Pageno"]
    scrip_keys  = ["strScrip", "strscrip"]

    for ep in ENDPOINTS:
        for strType in seg_opts:
            for strSearch in search_opts:
                for subcategory in subcat_opts:
                    for pageno_key in pageno_keys:
                        for scrip_key in scrip_keys:

                            params = {
                                pageno_key: 1,
                                "strCat": "-1",
                                "strPrevDate": d1,
                                "strToDate": d2,
                                scrip_key: "",
                                "strSearch": strSearch,
                                "strType": strType,
                                "subcategory": subcategory,
                            }

                            log.append(f"Trying {ep} | {pageno_key} | {scrip_key} | Type={strType}")

                            rows_acc = []
                            page = 1

                            while True:
                                rows, total, meta = _call_once(s, ep, params)

                                if meta.get("blocked"):
                                    log.append("Blocked: retry warmup")
                                    try:
                                        s.get(HOME, timeout=10)
                                        s.get(CORP, timeout=10)
                                    except:
                                        pass
                                    rows, total, meta = _call_once(s, ep, params)
                                    if meta.get("blocked"):
                                        break

                                if page == 1 and total == 0 and not rows:
                                    break

                                if not rows:
                                    break

                                rows_acc.extend(rows)
                                params[pageno_key] += 1
                                page += 1

                                if total and len(rows_acc) >= total:
                                    break

                            if rows_acc:
                                return rows_acc

    return []

def fetch_bse_announcements_strict(start_yyyymmdd: str, end_yyyymmdd: str, log=None):
    """Fetch full date range once — NO throttle, NO chunks."""
    if log is None:
        log = []

    s = requests.Session()
    s.headers.update(BASE_HEADERS)

    # warmup
    try:
        s.get(HOME, timeout=15)
        s.get(CORP, timeout=15)
    except:
        pass

    log.append(f"Full fetch: {start_yyyymmdd}..{end_yyyymmdd}")

    all_rows = _fetch_single_range(s, start_yyyymmdd, end_yyyymmdd, log)

    if not all_rows:
        return pd.DataFrame(columns=[
            "SCRIP_CD","SLONGNAME","HEADLINE","NEWSSUB",
            "NEWS_DT","ATTACHMENTNAME","NSURL"
        ])

    base_cols = ["SCRIP_CD","SLONGNAME","HEADLINE","NEWSSUB",
                 "NEWS_DT","ATTACHMENTNAME","NSURL","NEWSID"]

    seen = set(base_cols)
    extra_cols = []

    for r in all_rows:
        for k in r.keys():
            if k not in seen:
                extra_cols.append(k)
                seen.add(k)

    df = pd.DataFrame(all_rows, columns=base_cols + extra_cols)

    keys = ["NSURL", "NEWSID", "ATTACHMENTNAME", "HEADLINE"]
    keys = [k for k in keys if k in df.columns]

    if keys:
        df = df.drop_duplicates(subset=keys)

    if "NEWS_DT" in df.columns:
        df["_NEWS_DT_PARSED"] = pd.to_datetime(df["NEWS_DT"], errors="coerce", dayfirst=True)
        df = df.sort_values("_NEWS_DT_PARSED", ascending=False).drop(columns=["_NEWS_DT_PARSED"])

    return df

# --------------------
# Filters: Orders + Capex
# --------------------

ORDER_KEYWORDS = ["order","contract","bagged","supply","purchase order"]
ORDER_REGEX = re.compile(r"\b(?:" + "|".join(map(re.escape, ORDER_KEYWORDS)) + r")\b", re.IGNORECASE)

CAPEX_KEYWORDS = [
    "capex","capital expenditure","capacity expansion",
    "new plant","manufacturing facility","brownfield","greenfield",
    "setting up a plant","increase in capacity","expansion"
]
CAPEX_REGEX = re.compile("|".join(CAPEX_KEYWORDS), re.IGNORECASE)

def enrich_orders(df):
    if df.empty: return df
    mask = df["HEADLINE"].fillna("").str.contains(ORDER_REGEX)
    out = df.loc[mask, ["SLONGNAME","HEADLINE","NEWS_DT","NSURL"]].copy()
    out.columns = ["Company","Announcement","Date","Link"]
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce", dayfirst=True)
    return out.sort_values("Date", ascending=False)

def enrich_capex(df):
    if df.empty: return df
    combined = (df["HEADLINE"].fillna("") + " " + df["NEWSSUB"].fillna(""))
    mask = combined.str.contains(CAPEX_REGEX, na=False)
    out = df.loc[mask, ["SLONGNAME","HEADLINE","NEWS_DT","NSURL"]].copy()
    out.columns = ["Company","Announcement","Date","Link"]
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce", dayfirst=True)
    return out.sort_values("Date", ascending=False)

# --------------------
# Streamlit UI
# --------------------

st.set_page_config(page_title="BSE Order & Capex Announcements", layout="wide")
st.title("📣 BSE Order & Capex Announcements Finder")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", value=date(2025,1,1))
with col2:
    end_date = st.date_input("End Date", value=date.today())

run = st.button("🔎 Fetch Announcements", use_container_width=True)

if run:
    ds = start_date.strftime("%Y%m%d")
    de = end_date.strftime("%Y%m%d")
    logs = []

    with st.spinner("Fetching..."):
        df = fetch_bse_announcements_strict(ds, de, log=logs)

    orders_df = enrich_orders(df)
    capex_df = enrich_capex(df)

    st.metric("Total Announcements", len(df))
    st.metric("Order Announcements", len(orders_df))
    st.metric("Capex Announcements", len(capex_df))

    tab_orders, tab_capex, tab_all = st.tabs(["📦 Orders", "🏭 Capex", "📄 All"])

    with tab_orders:
        st.dataframe(orders_df, use_container_width=True)

    with tab_capex:
        st.dataframe(capex_df, use_container_width=True)

    with tab_all:
        st.dataframe(df, use_container_width=True)

