import requests, pandas as pd, time, re
from datetime import datetime, timedelta, date
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

def _fetch_chunk_resilient(s, d1: str, d2: str, log):
    """
    Try multiple endpoint/param variants for one small date window.
    Returns list[dict] rows (maybe empty).
    """
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
                            }
                            params["subcategory"] = subcategory

                            log.append(f"[{d1}-{d2}] {ep.split('/api/')[-1]} | strType={strType} | strSearch='{strSearch}' | subcat='{subcategory}' | keys={pageno_key}/{scrip_key}")

                            rows_acc = []
                            page = 1
                            while True:
                                rows, total, meta = _call_once(s, ep, params)
                                if meta.get("blocked"):
                                    log.append(f"   non-JSON ({meta['ct']}, {meta['status']}); re-warm + retry once")
                                    try:
                                        s.get(HOME, timeout=15)
                                        s.get(CORP, timeout=15)
                                    except Exception:
                                        pass
                                    rows, total, meta = _call_once(s, ep, params)
                                    if meta.get("blocked"):
                                        log.append("   still non-JSON; breaking variant.")
                                        break

                                if page == 1 and total == 0 and not rows:
                                    break

                                if not rows:
                                    break

                                rows_acc.extend(rows)
                                params[pageno_key] = params[pageno_key] + 1
                                page += 1
                                time.sleep(0.25)

                                if total and len(rows_acc) >= total:
                                    break

                            if rows_acc:
                                return rows_acc
    return []

def fetch_bse_announcements_strict(start_yyyymmdd: str, end_yyyymmdd: str, chunk_days=5, throttle=0.3, log=None):
    """
    Range-aware, resilient fetcher with multi-endpoint & header hardening.
    Returns DataFrame with deduped, sorted announcements.
    """
    if log is None:
        log = []

    assert len(start_yyyymmdd) == 8 and len(end_yyyymmdd) == 8
    assert start_yyyymmdd <= end_yyyymmdd

    s = requests.Session()
    s.headers.update(BASE_HEADERS)
    try:
        s.get(HOME, timeout=15)
        s.get(CORP, timeout=15)
    except Exception:
        pass

    start_dt = datetime.strptime(start_yyyymmdd, "%Y%m%d").date()
    end_dt   = datetime.strptime(end_yyyymmdd, "%Y%m%d").date()

    all_rows = []
    day = start_dt
    while day <= end_dt:
        chunk_start = day
        chunk_end   = min(day + timedelta(days=chunk_days-1), end_dt)
        ds = chunk_start.strftime("%Y%m%d")
        de = chunk_end.strftime("%Y%m%d")
        log.append(f"Chunk {ds}..{de}")
        rows = _fetch_chunk_resilient(s, ds, de, log)
        if rows:
            all_rows.extend(rows)
        time.sleep(throttle)
        day = chunk_end + timedelta(days=1)

    if not all_rows:
        return pd.DataFrame(columns=["SCRIP_CD","SLONGNAME","HEADLINE","NEWSSUB","NEWS_DT","ATTACHMENTNAME","NSURL"])

    base_cols = ["SCRIP_CD","SLONGNAME","HEADLINE","NEWSSUB","NEWS_DT","ATTACHMENTNAME","NSURL","NEWSID"]
    seen = set(base_cols)
    extra_cols = []
    for r in all_rows:
        for k in r.keys():
            if k not in seen:
                extra_cols.append(k); seen.add(k)

    df = pd.DataFrame(all_rows, columns=base_cols + extra_cols)

    keys = [c for c in ["NSURL","NEWSID","ATTACHMENTNAME","HEADLINE"] if c in df.columns]
    if keys:
        df = df.drop_duplicates(subset=keys)

    if "NEWS_DT" in df.columns:
        df["_NEWS_DT_PARSED"] = pd.to_datetime(df["NEWS_DT"], errors="coerce", dayfirst=True)
        df = df.sort_values("_NEWS_DT_PARSED", ascending=False).drop(columns=["_NEWS_DT_PARSED"])

    return df

# --------------------
# Text filters
# --------------------

# Orders / contracts (as you had)
ORDER_KEYWORDS = ["order","contract","bagged","supply","purchase order"]
ORDER_REGEX = re.compile(r"\b(?:" + "|".join(map(re.escape, ORDER_KEYWORDS)) + r")\b", re.IGNORECASE)

# Capex / expansion indicators
CAPEX_KEYWORDS = [
    "capex",
    "capital expenditure",
    "capital expenditures",
    "capacity expansion",
    "expansion of capacity",
    "expansion project",
    "greenfield project",
    "brownfield project",
    "greenfield expansion",
    "brownfield expansion",
    "new plant",
    "new manufacturing facility",
    "new manufacturing unit",
    "new factory",
    "setting up a plant",
    "setting up new plant",
    "setting up a new unit",
    "setting up manufacturing facility",
    "production capacity",
    "increase in capacity",
    "enhancement of capacity",
    "capital investment",
    "investment of rs",
    "to invest rs",
    "board approves capex",
    "board approves expansion",
    "board approves investment",
]
CAPEX_REGEX = re.compile(r"(?:" + "|".join(map(re.escape, CAPEX_KEYWORDS)) + r")", re.IGNORECASE)

def enrich_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Return a trimmed dataframe with only 'order-like' announcements and a click-through link column."""
    if df.empty:
        return df

    textcol = "HEADLINE" if "HEADLINE" in df.columns else "NEWSSUB"
    mask = df[textcol].fillna("").str.contains(ORDER_REGEX)
    out = df.loc[mask, ["SLONGNAME", "HEADLINE", "NEWS_DT", "NSURL"]].copy()
    out = out.rename(columns={"SLONGNAME":"Company", "HEADLINE":"Announcement", "NEWS_DT":"Date", "NSURL":"Link"})
    out["Company"] = out["Company"].fillna("")
    out["Announcement"] = out["Announcement"].fillna("")
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce", dayfirst=True)
    out = out.sort_values("Date", ascending=False)
    return out

def enrich_capex(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a trimmed dataframe with only capex / expansion / new plant announcements.
    Uses both HEADLINE and NEWSSUB to avoid missing phrasing that appears only in the body.
    """
    if df.empty:
        return df

    # Safely build a combined text series
    headlines = df.get("HEADLINE", pd.Series([""] * len(df))).fillna("")
    subs      = df.get("NEWSSUB", pd.Series([""] * len(df))).fillna("")
    combined  = (headlines + " " + subs).astype(str)

    mask = combined.str.contains(CAPEX_REGEX, na=False)
    out = df.loc[mask, ["SLONGNAME", "HEADLINE", "NEWS_DT", "NSURL"]].copy()
    out = out.rename(columns={"SLONGNAME":"Company", "HEADLINE":"Announcement", "NEWS_DT":"Date", "NSURL":"Link"})
    out["Company"] = out["Company"].fillna("")
    out["Announcement"] = out["Announcement"].fillna("")
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce", dayfirst=True)
    out = out.sort_values("Date", ascending=False)
    return out

# --------------------
# Streamlit App
# --------------------

st.set_page_config(page_title="BSE Order & Capex Announcements", page_icon="📣", layout="wide")

st.markdown(
    """
    <style>
    .title {font-size: 2.1rem; font-weight: 700; margin-bottom: .25rem;}
    .subtitle {color: #5b6b7a; margin-bottom: 1.25rem;}
    .footer {text-align:center; margin-top: 2rem; color: #6b7280;}
    .metric-card {padding: 1rem; background: #0f172a0f; border: 1px solid #0f172a1a; border-radius: 14px;}
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown('<div class="title">📣 BSE Order & Capex Announcements Finder</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="subtitle">Enter a date range to fetch BSE corporate announcements, then filter down to order/contract and capex/expansion related items.</div>',
    unsafe_allow_html=True
)

with st.container():
    col1, col2, col3 = st.columns([1.2,1.2,0.8])
    with col1:
        start_date = st.date_input("Start date", value=date(2025,1,1), min_value=date(2015,1,1))
    with col2:
        end_date = st.date_input("End date", value=date.today())
    with col3:
        chunk_days = st.number_input("Chunk size (days)", min_value=1, max_value=7, value=5, step=1)

advanced = st.expander("Advanced options", expanded=False)
with advanced:
    throttle = st.slider("Request throttle (seconds)", min_value=0.0, max_value=1.0, value=0.3, step=0.05)
    show_logs = st.checkbox("Show fetch logs", value=False)

run = st.button("🔎 Fetch announcements", type="primary", use_container_width=True)

if run:
    if start_date > end_date:
        st.error("Start date cannot be after End date.")
    else:
        ds = start_date.strftime("%Y%m%d")
        de = end_date.strftime("%Y%m%d")
        logs = []
        with st.spinner(f"Fetching announcements from {ds} to {de} ..."):
            df = fetch_bse_announcements_strict(ds, de, chunk_days=int(chunk_days), throttle=float(throttle), log=logs)

        orders_df = enrich_orders(df)
        capex_df  = enrich_capex(df)

        total_rows = len(df)
        order_rows = len(orders_df)
        capex_rows = len(capex_df)

        # Metrics
        mcol1, mcol2, mcol3, mcol4 = st.columns([1,1,1,1])
        with mcol1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Total Announcements", f"{total_rows:,}")
            st.markdown('</div>', unsafe_allow_html=True)
        with mcol2:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Order-like Announcements", f"{order_rows:,}")
            st.markdown('</div>', unsafe_allow_html=True)
        with mcol3:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Capex/Expansion Announcements", f"{capex_rows:,}")
            st.markdown('</div>', unsafe_allow_html=True)
        with mcol4:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Date Range (days)", f"{(end_date - start_date).days + 1:,}")
            st.markdown('</div>', unsafe_allow_html=True)

        st.divider()

        if total_rows == 0:
            st.warning("No announcements found in this window. Try adjusting the date range or chunk size.")
        else:
            tab_orders, tab_capex, tab_all = st.tabs(
                ["📦 Orders / Contracts", "🏭 Capex / Expansion", "🧾 All Announcements"]
            )

            with tab_orders:
                if order_rows == 0:
                    st.warning("No order-like announcements found in this window.")
                else:
                    st.subheader("Order / Contract Announcements")
                    st.dataframe(
                        orders_df,
                        use_container_width=True,
                        column_config={
                            "Link": st.column_config.LinkColumn("Announcement Link", display_text="Open"),
                            "Date": st.column_config.DatetimeColumn(format="DD MMM YYYY, HH:mm"),
                        },
                        hide_index=True,
                    )

                    csv_orders = orders_df.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        "⬇️ Download order results (CSV)",
                        csv_orders,
                        file_name=f"bse_orders_{ds}_{de}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )

            with tab_capex:
                if capex_rows == 0:
                    st.warning("No capex/expansion announcements found in this window.")
                else:
                    st.subheader("Capex / Expansion Announcements")
                    st.dataframe(
                        capex_df,
                        use_container_width=True,
                        column_config={
                            "Link": st.column_config.LinkColumn("Announcement Link", display_text="Open"),
                            "Date": st.column_config.DatetimeColumn(format="DD MMM YYYY, HH:mm"),
                        },
                        hide_index=True,
                    )

                    csv_capex = capex_df.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        "⬇️ Download capex results (CSV)",
                        csv_capex,
                        file_name=f"bse_capex_{ds}_{de}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )

            with tab_all:
                st.subheader("All Announcements (raw)")
                st.dataframe(df, use_container_width=True)

        if show_logs:
            st.divider()
            st.subheader("Fetch logs")
            st.code("\n".join(logs) if logs else "No logs.")
