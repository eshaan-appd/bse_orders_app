import re
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests
import streamlit as st
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================================================
# BSE CONFIG
# =========================================================
HOME = "https://www.bseindia.com/"
CORP = "https://www.bseindia.com/corporates/ann.html"
API_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
PDF_BASE = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/154.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": CORP,
    "Connection": "keep-alive",
}

# BSE can return a lot of pages. Company Update filtering is the main speed-up.
CATEGORY = "Company Update"
SUBCATEGORY = "-1"
CHUNK_DAYS = 7
PAGE_PAUSE_SECONDS = 0.15


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=5, pool_maxsize=5)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def warm_up(s: requests.Session) -> None:
    """Create cookies/session before calling the API."""
    for url in (HOME, CORP):
        try:
            s.get(url, timeout=12)
        except requests.RequestException:
            pass


def api_get(s: requests.Session, params: dict) -> dict:
    """Single resilient BSE API request."""
    try:
        r = s.get(API_URL, params=params, timeout=(8, 30))
    except requests.RequestException as exc:
        raise RuntimeError(f"BSE request failed: {exc}") from exc

    # If BSE rejects the current session, refresh cookies once.
    if r.status_code in (401, 403):
        warm_up(s)
        try:
            r = s.get(API_URL, params=params, timeout=(8, 30))
        except requests.RequestException as exc:
            raise RuntimeError(f"BSE retry failed: {exc}") from exc

    if not r.ok:
        raise RuntimeError(
            f"BSE API returned HTTP {r.status_code}. "
            f"Response: {r.text[:250]!r}"
        )

    # Do not reject only because Content-Type is unusual; attempt JSON parsing.
    try:
        return r.json()
    except ValueError as exc:
        raise RuntimeError(
            f"BSE returned a non-JSON response. "
            f"Content-Type={r.headers.get('content-type')!r}; "
            f"Body={r.text[:250]!r}"
        ) from exc


def fetch_one_range(
    s: requests.Session,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
    log: list[str] | None = None,
) -> list[dict]:
    """Fetch all pages for one BSE date range using one known request shape."""
    if log is None:
        log = []

    rows_all: list[dict] = []
    page = 1
    total_rows = None

    while True:
        params = {
            "pageno": page,
            "strCat": CATEGORY,
            "subcategory": SUBCATEGORY,
            "strPrevDate": start_yyyymmdd,
            "strToDate": end_yyyymmdd,
            "strSearch": "P",
            "strscrip": "",       # correct key used by current endpoint
            "strType": "C",      # equity
        }

        data = api_get(s, params)
        rows = data.get("Table") or []

        if page == 1:
            try:
                total_rows = int((data.get("Table1") or [{}])[0].get("ROWCNT") or 0)
            except (TypeError, ValueError, IndexError):
                total_rows = None

            log.append(
                f"{start_yyyymmdd}..{end_yyyymmdd}: "
                f"reported rows={total_rows if total_rows is not None else 'unknown'}"
            )

        if not rows:
            break

        rows_all.extend(rows)

        if total_rows is not None and total_rows > 0 and len(rows_all) >= total_rows:
            break

        page += 1

        # Safety guard against accidental endless pagination.
        if page > 5000:
            raise RuntimeError("Pagination safety limit reached.")

        time.sleep(PAGE_PAUSE_SECONDS)

    return rows_all


def iter_date_chunks(start_dt: date, end_dt: date, chunk_days: int = CHUNK_DAYS):
    current = start_dt
    while current <= end_dt:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_dt)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def build_pdf_link(value) -> str | None:
    if value is None or pd.isna(value):
        return None

    value = str(value).strip()
    if not value:
        return None

    if value.lower().startswith(("http://", "https://")):
        return value

    return PDF_BASE + value.lstrip("/")


def fetch_bse_announcements(
    start_dt: date,
    end_dt: date,
    log: list[str] | None = None,
) -> pd.DataFrame:
    if log is None:
        log = []

    if start_dt > end_dt:
        raise ValueError("Start date cannot be after end date.")

    s = make_session()
    warm_up(s)

    all_rows: list[dict] = []

    for chunk_start, chunk_end in iter_date_chunks(start_dt, end_dt):
        d1 = chunk_start.strftime("%Y%m%d")
        d2 = chunk_end.strftime("%Y%m%d")
        log.append(f"Fetching {d1}..{d2}")
        all_rows.extend(fetch_one_range(s, d1, d2, log))

    if not all_rows:
        return pd.DataFrame(
            columns=[
                "SCRIP_CD", "SLONGNAME", "HEADLINE", "NEWSSUB",
                "NEWS_DT", "ATTACHMENTNAME", "NSURL", "NEWSID",
                "CATEGORYNAME", "SUBCATNAME", "PDF_LINK",
            ]
        )

    df = pd.DataFrame(all_rows)

    # Prefer the unique announcement ID. Fallback keys are only for old/missing records.
    if "NEWSID" in df.columns:
        with_id = df["NEWSID"].fillna("").astype(str).str.strip().ne("")
        df_with_id = df.loc[with_id].drop_duplicates(subset=["NEWSID"], keep="first")
        df_without_id = df.loc[~with_id].copy()
        fallback = [c for c in ["SCRIP_CD", "NEWS_DT", "ATTACHMENTNAME", "HEADLINE"] if c in df.columns]
        if fallback:
            df_without_id = df_without_id.drop_duplicates(subset=fallback, keep="first")
        df = pd.concat([df_with_id, df_without_id], ignore_index=True)
    else:
        fallback = [c for c in ["SCRIP_CD", "NEWS_DT", "ATTACHMENTNAME", "HEADLINE"] if c in df.columns]
        if fallback:
            df = df.drop_duplicates(subset=fallback, keep="first")

    if "ATTACHMENTNAME" in df.columns:
        df["PDF_LINK"] = df["ATTACHMENTNAME"].apply(build_pdf_link)
    else:
        df["PDF_LINK"] = None

    if "NEWS_DT" in df.columns:
        df["_NEWS_DT_PARSED"] = pd.to_datetime(df["NEWS_DT"], errors="coerce")
        df = (
            df.sort_values("_NEWS_DT_PARSED", ascending=False, na_position="last")
              .drop(columns="_NEWS_DT_PARSED")
              .reset_index(drop=True)
        )

    return df


# =========================================================
# FILTERS: ORDERS + CAPEX
# =========================================================
ORDER_REGEX = re.compile(
    r"(?:"
    r"award\s+of\s+order|receipt\s+of\s+order|"
    r"purchase\s+order|work\s+order|letter\s+of\s+award|"
    r"contract\s+awarded|awarded\s+(?:a\s+)?contract|"
    r"bagged\s+(?:an?\s+)?order|secured\s+(?:an?\s+)?order|"
    r"order\s+win|new\s+order|order\s+received"
    r")",
    re.IGNORECASE,
)

ORDER_EXCLUDE_REGEX = re.compile(
    r"(?:"
    r"court\s+order|high\s+court|supreme\s+court|tribunal|"
    r"gst\s+order|tax\s+order|assessment\s+order|adjudication\s+order|"
    r"penalty\s+order|regulatory\s+order|exchange\s+order"
    r")",
    re.IGNORECASE,
)

CAPEX_REGEX = re.compile(
    r"(?:"
    r"capex|capital\s+expenditure|capacity\s+expansion|"
    r"expand(?:ing|ed|s)?\s+(?:the\s+)?capacity|"
    r"new\s+plant|new\s+facility|manufacturing\s+facility|"
    r"brownfield|greenfield|setting\s+up\s+(?:a\s+)?(?:new\s+)?plant|"
    r"increase\s+in\s+capacity|capacity\s+addition|"
    r"commission(?:ing|ed)?\s+(?:of\s+)?(?:a\s+)?(?:new\s+)?(?:plant|facility|line)|"
    r"new\s+manufacturing\s+line"
    r")",
    re.IGNORECASE,
)


def combined_text(df: pd.DataFrame) -> pd.Series:
    parts = []
    for col in ["HEADLINE", "NEWSSUB", "SUBCATNAME"]:
        if col in df.columns:
            parts.append(df[col].fillna("").astype(str))
    if not parts:
        return pd.Series("", index=df.index)

    text = parts[0]
    for part in parts[1:]:
        text = text + " " + part
    return text


def output_table(df: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    cols = [c for c in ["SLONGNAME", "HEADLINE", "NEWS_DT", "PDF_LINK"] if c in df.columns]
    out = df.loc[mask, cols].copy()

    rename = {
        "SLONGNAME": "Company",
        "HEADLINE": "Announcement",
        "NEWS_DT": "Date",
        "PDF_LINK": "Link",
    }
    out = out.rename(columns=rename)

    if "Date" in out.columns:
        out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
        out = out.sort_values("Date", ascending=False, na_position="last")

    return out.reset_index(drop=True)


def enrich_orders(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["Company", "Announcement", "Date", "Link"])

    text = combined_text(df)

    # BSE's dedicated subcategory is useful, but exclusions remove obvious court/tax false positives.
    subcat_match = pd.Series(False, index=df.index)
    if "SUBCATNAME" in df.columns:
        subcat_match = df["SUBCATNAME"].fillna("").str.contains(
            r"Award of Order\s*/\s*Receipt of Order",
            case=False,
            regex=True,
        )

    positive = text.str.contains(ORDER_REGEX, na=False) | subcat_match
    negative = text.str.contains(ORDER_EXCLUDE_REGEX, na=False)

    return output_table(df, positive & ~negative)


def enrich_capex(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["Company", "Announcement", "Date", "Link"])

    text = combined_text(df)
    mask = text.str.contains(CAPEX_REGEX, na=False)
    return output_table(df, mask)


# =========================================================
# STREAMLIT UI
# =========================================================
st.set_page_config(page_title="BSE Order & Capex Announcements", layout="wide")
st.title("BSE Order & Capex Announcements Finder")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", value=date.today() - timedelta(days=30))
with col2:
    end_date = st.date_input("End Date", value=date.today())

if start_date > end_date:
    st.error("Start Date cannot be after End Date.")
    st.stop()

range_days = (end_date - start_date).days + 1
if range_days > 180:
    st.warning(
        "Large date range selected. BSE announcements are paginated; "
        "for faster runs, fetch shorter periods and combine the results."
    )

run = st.button("Fetch Announcements", use_container_width=True)

if run:
    logs: list[str] = []

    try:
        with st.spinner("Fetching BSE Company Update announcements..."):
            df = fetch_bse_announcements(start_date, end_date, log=logs)

        orders_df = enrich_orders(df)
        capex_df = enrich_capex(df)

        m1, m2, m3 = st.columns(3)
        m1.metric("Company Update Announcements", len(df))
        m2.metric("Order Announcements", len(orders_df))
        m3.metric("Capex Announcements", len(capex_df))

        tab_orders, tab_capex, tab_all, tab_log = st.tabs(
            ["Orders", "Capex", "All Company Updates", "Debug Log"]
        )

        link_config = {
            "Link": st.column_config.LinkColumn("Link", display_text="Open PDF")
        }

        with tab_orders:
            st.dataframe(
                orders_df,
                use_container_width=True,
                hide_index=True,
                column_config=link_config,
            )

        with tab_capex:
            st.dataframe(
                capex_df,
                use_container_width=True,
                hide_index=True,
                column_config=link_config,
            )

        with tab_all:
            display_cols = [
                c for c in [
                    "SCRIP_CD", "SLONGNAME", "HEADLINE", "NEWS_DT",
                    "CATEGORYNAME", "SUBCATNAME", "PDF_LINK"
                ] if c in df.columns
            ]
            all_display = df[display_cols].rename(columns={"PDF_LINK": "Link"})
            st.dataframe(
                all_display,
                use_container_width=True,
                hide_index=True,
                column_config=link_config,
            )

        with tab_log:
            st.code("\n".join(logs) if logs else "No log entries.")

    except Exception as exc:
        st.error(f"Fetch failed: {exc}")
        with st.expander("Debug log"):
            st.code("\n".join(logs) if logs else "No log entries.")
