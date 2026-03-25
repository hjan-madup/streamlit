"""
우리팀 광고 대시보드 (SQLite + Streamlit)
실행: streamlit run app.py
"""
from __future__ import annotations

import hashlib
import hmac
import sqlite3
import time
from datetime import date, datetime
from io import BytesIO
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

_ROOT = Path(__file__).resolve().parent
DB_PATH = _ROOT / "marketing.db"

ADMIN_ID = "admin"
ADMIN_PASSWORD_SHA256 = (
    "ac9689e2272427085e35b9d3e3e8bed88cb3434828b43b86fc0596cad4c6e270"
)

MAX_FAILED_ATTEMPTS = 3
LOCKOUT_SECONDS = 300

PAGE_TITLE = "우리팀 광고 대시보드"
PAGE_ICON = "🎯"


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


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def load_dataframe() -> pd.DataFrame:
    if not DB_PATH.is_file():
        raise FileNotFoundError(str(DB_PATH))
    conn = get_connection()
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


def chart_cost_vs_revenue(daily: pd.DataFrame) -> None:
    color_scale = alt.Scale(
        domain=["광고비", "매출"],
        range=["#4834d4", "#f0932b"],
    )
    legend = alt.Legend(
        orient="top-right",
        title=None,
        direction="horizontal",
    )

    bar = (
        alt.Chart(daily.assign(지표="광고비"))
        .mark_bar()
        .encode(
            x=alt.X("date:O", title="일자", sort=None),
            y=alt.Y("cost:Q", title="금액(원)"),
            color=alt.Color("지표:N", scale=color_scale, legend=legend),
            tooltip=[
                alt.Tooltip("date:O", title="일자"),
                alt.Tooltip("cost:Q", title="광고비", format=","),
            ],
        )
    )
    line = (
        alt.Chart(daily.assign(지표="매출"))
        .mark_line(point=True, strokeWidth=2)
        .encode(
            x=alt.X("date:O", sort=None),
            y=alt.Y("revenue:Q"),
            color=alt.Color("지표:N", scale=color_scale, legend=None),
            tooltip=[
                alt.Tooltip("date:O", title="일자"),
                alt.Tooltip("revenue:Q", title="매출", format=","),
            ],
        )
    )
    chart = (
        (bar + line)
        .resolve_scale(y="shared", color="shared")
        .properties(height=360)
        .configure_axis(labelLimit=200)
    )
    st.altair_chart(chart, use_container_width=True)


def pct_change(curr: float, prev: float) -> float:
    if prev == 0:
        return 100.0 if curr > 0 else 0.0
    return (curr - prev) / prev * 100.0


def render_weekly_channel_comparison(df_all: pd.DataFrame, sel_channels: list[str]) -> None:
    st.subheader("주간 성과 비교 (채널별)")
    st.caption("최근 7일 vs 그 직전 7일 · 사이드바에서 선택한 채널만 표시합니다.")

    if df_all.empty:
        st.info("비교할 데이터가 없습니다.")
        return

    end_ts = df_all["date"].max().normalize()
    this_start = end_ts - pd.Timedelta(days=6)
    prev_end = this_start - pd.Timedelta(days=1)
    prev_start = prev_end - pd.Timedelta(days=6)

    span_days = (end_ts - df_all["date"].min().normalize()).days
    if span_days < 13:
        st.warning("전주 대비 비교를 하기에는 데이터 기간이 2주 미만일 수 있습니다.")

    m_this = (df_all["date"] >= this_start) & (df_all["date"] <= end_ts)
    m_prev = (df_all["date"] >= prev_start) & (df_all["date"] <= prev_end)
    ch_set = set(sel_channels)

    def agg_by_channel(mask: pd.Series) -> pd.DataFrame:
        sub = df_all.loc[mask & df_all["channel"].isin(ch_set)]
        if sub.empty:
            return pd.DataFrame(columns=["channel", "cost", "revenue", "conversions", "roas"])
        g = (
            sub.groupby("channel", as_index=False)
            .agg(cost=("cost", "sum"), revenue=("revenue", "sum"), conversions=("conversions", "sum"))
        )
        g["roas"] = g.apply(lambda r: (r["revenue"] / r["cost"]) if r["cost"] else 0.0, axis=1)
        return g

    this_w = agg_by_channel(m_this).set_index("channel")
    prev_w = agg_by_channel(m_prev).set_index("channel")

    all_ch = sorted(set(this_w.index) | set(prev_w.index))
    if not all_ch:
        st.info("선택한 채널·기간에 해당하는 주간 데이터가 없습니다.")
        return

    rows = []
    for ch in all_ch:
        t = this_w.loc[ch] if ch in this_w.index else pd.Series({"cost": 0, "revenue": 0, "conversions": 0, "roas": 0.0})
        p = prev_w.loc[ch] if ch in prev_w.index else pd.Series({"cost": 0, "revenue": 0, "conversions": 0, "roas": 0.0})
        rows.append(
            {
                "채널": ch,
                "광고비(이번)": float(t["cost"]),
                "광고비(전주)": float(p["cost"]),
                "광고비 증감(%)": pct_change(float(t["cost"]), float(p["cost"])),
                "매출(이번)": float(t["revenue"]),
                "매출(전주)": float(p["revenue"]),
                "매출 증감(%)": pct_change(float(t["revenue"]), float(p["revenue"])),
                "ROAS(이번)": float(t["roas"]),
                "ROAS(전주)": float(p["roas"]),
                "ROAS 증감(%)": pct_change(float(t["roas"]), float(p["roas"])),
                "전환(이번)": int(t["conversions"]),
                "전환(전주)": int(p["conversions"]),
                "전환 증감(%)": pct_change(float(t["conversions"]), float(p["conversions"])),
            }
        )

    cmp_df = pd.DataFrame(rows)

    def _style_delta(val: float) -> str:
        if val > 0:
            return "color: #2ecc71; font-weight: 600"
        if val < 0:
            return "color: #e74c3c; font-weight: 600"
        return ""

    styler = cmp_df.style.format(
        {
            "광고비(이번)": "{:,.0f}",
            "광고비(전주)": "{:,.0f}",
            "광고비 증감(%)": "{:+.2f}%",
            "매출(이번)": "{:,.0f}",
            "매출(전주)": "{:,.0f}",
            "매출 증감(%)": "{:+.2f}%",
            "ROAS(이번)": "{:.2f}",
            "ROAS(전주)": "{:.2f}",
            "ROAS 증감(%)": "{:+.2f}%",
            "전환(이번)": "{:,}",
            "전환(전주)": "{:,}",
            "전환 증감(%)": "{:+.2f}%",
        }
    )
    delta_cols = ["광고비 증감(%)", "매출 증감(%)", "ROAS 증감(%)", "전환 증감(%)"]
    if hasattr(styler, "map"):
        styler = styler.map(lambda v: _style_delta(v), subset=delta_cols)
    else:
        styler = styler.applymap(lambda v: _style_delta(v), subset=delta_cols)

    st.dataframe(styler, use_container_width=True, hide_index=True)
    st.caption(
        f"이번 주 구간: {this_start.date()} ~ {end_ts.date()} · 전주 구간: {prev_start.date()} ~ {prev_end.date()}"
    )


def save_uploaded_csv_to_db(df: pd.DataFrame, filename: str) -> None:
    out = df.copy()
    out["_imported_at"] = datetime.now().isoformat(timespec="seconds")
    out["_source_file"] = filename
    conn = get_connection()
    try:
        out.to_sql("csv_user_uploads", conn, if_exists="append", index=False)
        conn.commit()
    finally:
        conn.close()


def tab_csv_upload() -> None:
    st.subheader("CSV 업로드")
    st.caption("CSV를 올려 미리보기·간단 차트 후 SQLite(`csv_user_uploads` 테이블)에 저장할 수 있습니다.")

    up = st.file_uploader("CSV 파일 선택", type=["csv"], help="UTF-8 인코딩을 권장합니다.")
    if not up:
        return

    raw = up.getvalue()
    preview_df = None
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            preview_df = pd.read_csv(BytesIO(raw), encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    if preview_df is None:
        try:
            preview_df = pd.read_csv(BytesIO(raw))
        except Exception as e:
            st.error(f"CSV를 읽는 데 실패했습니다: {e}")
            return

    st.markdown("**미리보기**")
    st.dataframe(preview_df.head(50), use_container_width=True)

    cols = list(preview_df.columns)
    if len(cols) < 2:
        st.warning("차트를 만들려면 최소 2개 열이 필요합니다.")
        return

    c1, c2 = st.columns(2)
    with c1:
        x_col = st.selectbox("X축", cols, key="csv_x")
    with c2:
        y_candidates = [c for c in cols if c != x_col]
        y_col = st.selectbox("Y축", y_candidates, key="csv_y")

    chart_df = preview_df[[x_col, y_col]].copy()
    chart_df[y_col] = pd.to_numeric(chart_df[y_col], errors="coerce")
    chart_df = chart_df.dropna(subset=[y_col])
    if chart_df.empty:
        st.warning("Y축으로 사용할 수 있는 숫자 값이 없습니다.")
    else:
        st.markdown("**바 차트**")
        st.bar_chart(chart_df.set_index(x_col)[y_col])

    if st.button("DB에 저장", type="primary", key="csv_save"):
        try:
            save_uploaded_csv_to_db(preview_df, up.name)
            st.success(f"`csv_user_uploads` 테이블에 저장했습니다. ({up.name})")
        except Exception as e:
            st.error(f"저장 실패: {e}")


def tab_data_query() -> None:
    t_csv, t_note = st.tabs(["CSV 업로드", "가이드"])
    with t_csv:
        tab_csv_upload()
    with t_note:
        st.markdown(
            """
- CSV는 **UTF-8**을 권장합니다.
- 저장 시 `marketing.db`의 **`csv_user_uploads`** 테이블에 행이 추가됩니다.
- 첫 저장 시 스키마가 생성되며, 이후 업로드는 **동일한 열 구성**이면 이어 붙입니다. 열이 다르면 SQLite 오류가 날 수 있습니다.
            """
        )


def login_page() -> None:
    st.title(PAGE_TITLE)
    st.markdown("**마케팅팀 전용 로그인**")
    st.caption("허가된 계정만 이용할 수 있습니다. 계정이 필요하면 팀 관리자에게 문의하세요.")
    init_auth_state()

    if is_locked_out():
        rem = lockout_remaining_sec()
        m, s = rem // 60, rem % 60
        st.error(f"로그인이 일시적으로 제한되었습니다. {m}분 {s}초 후 다시 시도하세요.")
        st.caption("비밀번호를 3회 이상 틀리면 5분간 로그인할 수 없습니다. 남은 시간은 새로고침 시 갱신됩니다.")
        return

    left, _ = st.columns([1, 2])
    with left:
        with st.form("login_form", clear_on_submit=False):
            uid = st.text_input("아이디", autocomplete="username", placeholder="admin")
            pwd = st.text_input("비밀번호", type="password", autocomplete="current-password")
            submitted = st.form_submit_button("대시보드 로그인")

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


def render_dashboard_main(
    df: pd.DataFrame,
    df_all: pd.DataFrame,
    sel_channels: list[str],
) -> None:
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
        st.subheader("광고비 vs 매출")
        chart_cost_vs_revenue(daily)

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

    render_weekly_channel_comparison(df_all, sel_channels)


def dashboard() -> None:
    try:
        df_all = load_dataframe()
    except FileNotFoundError:
        st.error(f"DB 파일을 찾을 수 없습니다: `{DB_PATH}` — `setup_data.py`를 먼저 실행하세요.")
        with st.sidebar:
            st.markdown("### 마케팅팀 v1.0")
            st.caption(date.today().strftime("%Y년 %m월 %d일"))
            if st.button("로그아웃", type="secondary"):
                st.session_state.authenticated = False
                st.rerun()
        return

    with st.sidebar:
        st.markdown("### 마케팅팀 v1.0")
        st.caption(date.today().strftime("%Y년 %m월 %d일"))
        st.divider()

        if df_all.empty:
            st.warning("데이터가 없습니다. `setup_data.py`로 데이터를 생성하세요.")
            if st.button("로그아웃", type="secondary"):
                st.session_state.authenticated = False
                st.rerun()
            return

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

    st.title(PAGE_TITLE)

    tab_dash, tab_query = st.tabs(["대시보드", "데이터 조회"])
    with tab_dash:
        render_dashboard_main(df, df_all, sel_channels)
    with tab_query:
        tab_data_query()


def main() -> None:
    st.set_page_config(
        page_title=PAGE_TITLE,
        page_icon=PAGE_ICON,
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
