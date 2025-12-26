import requests, pandas as pd, time, re
from datetime import datetime, date, timedelta
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
    """Fetch full date range without chunking (not used in the new multi-day loop,
    but kept here in case you still want one-shot behaviour elsewhere)."""
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

def _norm(x):
    """Basic normalisation for text comparison."""
    if x is None:
        return ""
    return str(x).strip()

def _first_col(df: pd.DataFrame, candidates):
    """Return the first existing column from a list of candidate names."""
    for c in candidates:
        if c in df.columns:
            return c
    return None

def fetch_bse_announcements_strict(start_yyyymmdd: str,
                                   end_yyyymmdd: str,
                                   log=None,
                                   verbose: bool = True,
                                   request_timeout: int = 25) -> pd.DataFrame:
    """
    Fetch announcements between start_yyyymmdd and end_yyyymmdd (inclusive),
    but call the BSE API **day-by-day**, because it behaves most reliably when
    strPrevDate == strToDate.

    Logic preserved from your earlier version:
    - Use AnnSubCategoryGetData endpoint.
    - Try multiple (subcategory, strSearch) variants.
    - Build a wide DataFrame from all keys.
    - Filter to Category = 'Company Update'.
    - Further filter to subcategory containing any of:
      Acquisition | Amalgamation / Merger | Scheme of Arrangement | Joint Venture
    """
    assert len(start_yyyymmdd) == 8 and len(end_yyyymmdd) == 8
    assert start_yyyymmdd <= end_yyyymmdd

    base_page = "https://www.bseindia.com/corporates/ann.html"
    url = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"

    # One session for the whole range
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": base_page,
        "X-Requested-With": "XMLHttpRequest",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })

    # Warm-up calls (reduce chance of HTML / redirect responses)
    try:
        s.get(base_page, timeout=15)
    except Exception:
        pass

    variants = [
        {"subcategory": "-1", "strSearch": "P"},
        {"subcategory": "-1", "strSearch": ""},
        {"subcategory": "",   "strSearch": "P"},
        {"subcategory": "",   "strSearch": ""},
    ]

    all_rows: list[dict] = []

    # --- iterate day by day ---
    start_dt = datetime.strptime(start_yyyymmdd, "%Y%m%d").date()
    end_dt   = datetime.strptime(end_yyyymmdd,   "%Y%m%d").date()

    cur = start_dt
    while cur <= end_dt:
        day_str = cur.strftime("%Y%m%d")
        if verbose:
            # On Streamlit, prefer logging via st.write only if you really want to see it.
            # Here we just append to log if provided.
            pass
        if log is not None:
            log.append(f"Fetching BSE announcements for {day_str}...")

        day_rows: list[dict] = []

        for v in variants:
            params = {
                "pageno": 1,
                "strCat": "-1",
                "subcategory": v["subcategory"],
                "strPrevDate": day_str,
                "strToDate": day_str,
                "strSearch": v["strSearch"],
                "strscrip": "",
                "strType": "C",
            }

            rows, total, page = [], None, 1

            while True:
                try:
                    r = s.get(url, params=params, timeout=request_timeout)
                except requests.exceptions.RequestException as e:
                    if log is not None:
                        log.append(f"[{day_str} {v}] request error on page {page}: {e}")
                    rows = []
                    break

                ct = r.headers.get("content-type", "")
                if "application/json" not in ct:
                    if log is not None:
                        log.append(f"[{day_str} {v}] non-JSON on page {page} (ct={ct}).")
                    break

                data = r.json()
                table = data.get("Table") or []
                rows.extend(table)

                if total is None:
                    try:
                        total = int((data.get("Table1") or [{}])[0].get("ROWCNT") or 0)
                    except Exception:
                        total = None

                if not table:
                    break

                params["pageno"] += 1
                page += 1
                time.sleep(0.25)

                if total and len(rows) >= total:
                    break

            if rows:
                # Got data for this day with this variant; no need to try others
                day_rows.extend(rows)
                break

        if day_rows:
            all_rows.extend(day_rows)

        cur += timedelta(days=1)

    # --- no data for entire range ---
    if not all_rows:
        return pd.DataFrame()

    # --- build wide DataFrame from all rows ---
    all_keys = set()
    for r in all_rows:
        all_keys.update(r.keys())
    df = pd.DataFrame(all_rows, columns=list(all_keys))

    # --- filter to Company Update + specific subcategories ---
    def filter_announcements(df_in: pd.DataFrame, category_filter="Company Update") -> pd.DataFrame:
        if df_in.empty:
            return df_in.copy()
        cat_col = _first_col(df_in, [
            "CATEGORYNAME",
            "CATEGORY",
            "NEWS_CAT",
            "NEWSCATEGORY",
            "NEWS_CATEGORY",
        ])
        if not cat_col:
            return df_in.copy()
        df2 = df_in.copy()
        df2["_cat_norm"] = df2[cat_col].map(lambda x: _norm(x).lower())
        return df2.loc[df2["_cat_norm"] == _norm(category_filter).lower()].drop(columns=["_cat_norm"])

    df_filtered = filter_announcements(df, category_filter="Company Update")
    if df_filtered.empty:
        return df_filtered

    df_filtered = df_filtered.loc[
        df_filtered
        .filter(["NEWSSUB", "SUBCATEGORY", "SUBCATEGORYNAME", "NEWS_SUBCATEGORY", "NEWS_SUB"], axis=1)
        .astype(str)
        .apply(
            lambda col: col.str.contains(
                r"(Acquisition|Amalgamation\s*/\s*Merger|Scheme of Arrangement|Joint Venture)",
                case=False,
                na=False,
            )
        )
        .any(axis=1)
    ]

    return df_filtered


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
    if df.empty:
        return df
    mask = df["HEADLINE"].fillna("").str.contains(ORDER_REGEX)
    out = df.loc[mask, ["SLONGNAME","HEADLINE","NEWS_DT","NSURL"]].copy()
    out.columns = ["Company","Announcement","Date","Link"]
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce", dayfirst=True)
    return out.sort_values("Date", ascending=False).reset_index(drop=True)

def enrich_capex(df):
    if df.empty:
        return df
    combined = (df["HEADLINE"].fillna("") + " " + df["NEWSSUB"].fillna(""))
    mask = combined.str.contains(CAPEX_REGEX, na=False)
    out = df.loc[mask, ["SLONGNAME","HEADLINE","NEWS_DT","NSURL"]].copy()
    out.columns = ["Company","Announcement","Date","Link"]
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce", dayfirst=True)
    return out.sort_values("Date", ascending=False).reset_index(drop=True)

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
