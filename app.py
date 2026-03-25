"""
마케팅 일별 리포트 대시보드 (SQLite + Streamlit)
실행: streamlit run app.py
"""
from __future__ import annotations

import hashlib
import hmac
import sqlite3
import time
from pathlib import Path

import pandas as pd
import streamlit as st

_ROOT = Path(__file__).resolve().parent
DB_PATH = _ROOT / "marketing.db"

ADMIN_ID = "admin"
# SHA-256("admin1234") — 평문 비밀번호는 코드에 두지 않음
ADMIN_PASSWORD_SHA256 = (
    "ac9689e2272427085e35b9d3e3e8bed88cb3434828b43b86fc0596cad4c6e270"
)

MAX_FAILED_ATTEMPTS = 3
LOCKOUT_SECONDS = 300


def _pw_hash(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def verify_credentials(user_id: str, password: str) -> bool:
    if user_id != ADMIN_ID:
        return False
    return hmac.compare_digest(_pw_hash(password), ADMIN_PASSWORD_SHA256)


def init_auth_state() -> None:
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "failed_attempts" not in st.session_state:
        st.session_state.failed_attempts = 0
    if "lockout_until" not in st.session_state:
        st.session_state.lockout_until = None


def is_locked_out() -> bool:
    until = st.session_state.lockout_until
    if until is None:
        return False
    now = time.time()
    if now >= until:
        st.session_state.lockout_until = None
        st.session_state.failed_attempts = 0
        return False
    return True


def lockout_remaining_sec() -> int:
    return max(0, int(st.session_state.lockout_until - time.time()))


def register_failed_login() -> None:
    st.session_state.failed_attempts += 1
    if st.session_state.failed_attempts >= MAX_FAILED_ATTEMPTS:
        st.session_state.lockout_until = time.time() + LOCKOUT_SECONDS
        st.session_state.failed_attempts = 0


def load_dataframe() -> pd.DataFrame:
    if not DB_PATH.is_file():
        raise FileNotFoundError(str(DB_PATH))
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query("SELECT * FROM daily_report ORDER BY date, channel, campaign", conn)
    finally:
        conn.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df


def format_krw(n: float) -> str:
    return f"{int(round(n)):,}원"


def login_page() -> None:
    st.title("로그인")
    init_auth_state()

    if is_locked_out():
        rem = lockout_remaining_sec()
        m, s = rem // 60, rem % 60
        st.error(f"로그인 시도가 제한되었습니다. {m}분 {s}초 후 다시 시도하세요.")
        st.caption("3회 이상 틀리면 5분간 로그인할 수 없습니다. 남은 시간은 페이지를 새로고침하면 갱신됩니다.")
        return

    left, _ = st.columns([1, 2])
    with left:
        with st.form("login_form", clear_on_submit=False):
            uid = st.text_input("아이디", autocomplete="username")
            pwd = st.text_input("비밀번호", type="password", autocomplete="current-password")
            submitted = st.form_submit_button("로그인")

        if submitted:
            if verify_credentials(uid.strip(), pwd):
                st.session_state.authenticated = True
                st.session_state.failed_attempts = 0
                st.session_state.lockout_until = None
                st.rerun()
            else:
                register_failed_login()
                left_attempts = MAX_FAILED_ATTEMPTS - st.session_state.failed_attempts
                if st.session_state.lockout_until:
                    st.error("3회 이상 실패하여 5분간 로그인이 제한됩니다.")
                elif left_attempts > 0:
                    st.warning(f"아이디 또는 비밀번호가 올바르지 않습니다. ({left_attempts}회 남음)")
                else:
                    st.error("아이디 또는 비밀번호가 올바르지 않습니다.")


def dashboard() -> None:
    try:
        df_all = load_dataframe()
    except FileNotFoundError:
        st.error(f"DB 파일을 찾을 수 없습니다: `{DB_PATH}` — `setup_data.py`를 먼저 실행하세요.")
        if st.sidebar.button("로그아웃"):
            st.session_state.authenticated = False
            st.rerun()
        return

    if df_all.empty:
        st.warning("데이터가 없습니다. `setup_data.py`로 데이터를 생성하세요.")
        if st.sidebar.button("로그아웃"):
            st.session_state.authenticated = False
            st.rerun()
        return

    with st.sidebar:
        st.header("필터")
        d_min = df_all["date"].min().date()
        d_max = df_all["date"].max().date()
        c1, c2 = st.columns(2)
        with c1:
            date_from = st.date_input("시작일", value=d_min, min_value=d_min, max_value=d_max)
        with c2:
            date_to = st.date_input("종료일", value=d_max, min_value=d_min, max_value=d_max)

        if date_from > date_to:
            st.error("시작일이 종료일보다 늦을 수 없습니다.")
            st.stop()

        channels = sorted(df_all["channel"].unique())
        sel_channels = st.multiselect("채널", channels, default=channels)

        camp_pool = df_all[df_all["channel"].isin(sel_channels)]["campaign"].unique()
        campaigns = sorted(camp_pool)
        sel_campaigns = st.multiselect("캠페인", campaigns, default=campaigns)

        st.divider()
        if st.button("로그아웃", type="secondary"):
            st.session_state.authenticated = False
            st.rerun()

    mask = (
        (df_all["date"].dt.date >= date_from)
        & (df_all["date"].dt.date <= date_to)
        & df_all["channel"].isin(sel_channels)
        & df_all["campaign"].isin(sel_campaigns)
    )
    df = df_all.loc[mask].copy()

    st.title("마케팅 성과 대시보드")
    st.caption("필터는 왼쪽 사이드바에서 조정할 수 있습니다.")

    if df.empty:
        st.info("선택한 조건에 맞는 데이터가 없습니다.")
        return

    total_imp = int(df["impressions"].sum())
    total_clk = int(df["clicks"].sum())
    total_cost = float(df["cost"].sum())
    total_conv = int(df["conversions"].sum())
    total_rev = float(df["revenue"].sum())
    roas = total_rev / total_cost if total_cost else 0.0
    ctr = (total_clk / total_imp * 100) if total_imp else 0.0
    cpc = total_cost / total_clk if total_clk else 0.0

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("노출", f"{total_imp:,}")
    m2.metric("클릭", f"{total_clk:,}")
    m3.metric("비용", format_krw(total_cost))
    m4.metric("전환", f"{total_conv:,}")
    m5.metric("매출", format_krw(total_rev))
    m6.metric("ROAS", f"{roas:.2f}x")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("총 클릭수", f"{total_clk:,}")
    k2.metric("총 노출수", f"{total_imp:,}")
    k3.metric("평균 CTR", f"{ctr:.2f}%")
    k4.metric("평균 CPC", format_krw(cpc))

    row1, row2 = st.columns(2)
    daily = (
        df.assign(_day=df["date"].dt.strftime("%Y-%m-%d"))
        .groupby("_day", as_index=False)
        .agg(cost=("cost", "sum"), revenue=("revenue", "sum"))
        .rename(columns={"_day": "date"})
        .sort_values("date")
    )
    with row1:
        st.subheader("일별 비용·매출")
        st.line_chart(daily.set_index("date")[["cost", "revenue"]])

    by_ch = df.groupby("channel")[["cost", "revenue", "conversions"]].sum().sort_values("revenue", ascending=False)
    with row2:
        st.subheader("채널별 매출")
        st.bar_chart(by_ch["revenue"])

    st.subheader("채널별 요약")
    summary = by_ch.reset_index()
    summary["ROAS"] = summary.apply(
        lambda r: (r["revenue"] / r["cost"]) if r["cost"] else 0.0, axis=1
    )
    display = summary.rename(
        columns={
            "channel": "채널",
            "cost": "비용",
            "revenue": "매출",
            "conversions": "전환",
            "ROAS": "ROAS",
        }
    )
    st.dataframe(
        display.style.format(
            {"비용": "{:,.0f}", "매출": "{:,.0f}", "전환": "{:,.0f}", "ROAS": "{:.2f}"}
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.subheader("상세 데이터")
    detail = df[
        ["date", "channel", "campaign", "impressions", "clicks", "cost", "conversions", "revenue"]
    ].copy()
    detail["date"] = detail["date"].dt.strftime("%Y-%m-%d")
    detail = detail.rename(
        columns={
            "date": "일자",
            "channel": "채널",
            "campaign": "캠페인",
            "impressions": "노출",
            "clicks": "클릭",
            "cost": "비용",
            "conversions": "전환",
            "revenue": "매출",
        }
    )
    st.dataframe(
        detail.style.format({"비용": "{:,.0f}", "매출": "{:,.0f}"}),
        use_container_width=True,
        hide_index=True,
    )


def main() -> None:
    st.set_page_config(
        page_title="마케팅 대시보드",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    init_auth_state()
    if not st.session_state.authenticated:
        login_page()
    else:
        dashboard()


if __name__ == "__main__":
    main()
