import os
import json
from datetime import datetime
from typing import Optional
from urllib import error as urlerror
from urllib import request as urlrequest

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pymysql
import streamlit as st
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from streamlit_autorefresh import st_autorefresh


APP_VERSION = "2026-04-19-v3-strict"
AUTO_SEED_ON_EMPTY = os.getenv("AUTO_SEED_ON_EMPTY", "true").strip().lower() not in {"0", "false", "no", "off"}

st.set_page_config(page_title="News 实时可视化与智能分析大屏", page_icon="📊", layout="wide")


# Default DB config
DB_HOST = os.getenv("DB_HOST", "hadoop1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "news")
DB_USER = os.getenv("DB_USER", "hive")
DB_PASSWORD = os.getenv("DB_PASSWORD", "bigdata123")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
OPENAI_RESPONSES_URL = os.getenv("OPENAI_RESPONSES_URL", "https://api.openai.com/v1/responses")
DEEPSEEK_API_URL = os.getenv("DEEPSEEK_API_URL", "https://api.deepseek.com/chat/completions")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")


def mysql_conn(host: str, port: int, db: str, user: str, password: str):
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db,
        charset="utf8mb4",
        autocommit=True,
        connect_timeout=5,
        read_timeout=10,
        write_timeout=10,
    )


def find_col(cols: list[str], candidates: list[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in cols}
    for x in candidates:
        if x.lower() in lower_map:
            return lower_map[x.lower()]
    for c in cols:
        lc = c.lower()
        if any(k.lower() in lc for k in candidates):
            return c
    return None


def to_numeric_safe(series: pd.Series) -> pd.Series:
    # Direct numeric
    num = pd.to_numeric(series, errors="coerce")
    if num.notna().mean() > 0.5:
        return num.fillna(0)
    # Extract number from dirty strings like "count12", "12次"
    ext = series.astype(str).str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
    return pd.to_numeric(ext, errors="coerce").fillna(0)


def parse_clock_seconds(series: pd.Series) -> pd.Series:
    # Supports HH:MM:SS / HH:MM / timedelta-like / numeric seconds
    s = series.astype(str).str.strip()
    m = s.str.extract(r"^(?P<h>\d{1,2}):(?P<m>\d{2})(?::(?P<s>\d{2}))?$")
    h = pd.to_numeric(m["h"], errors="coerce")
    mi = pd.to_numeric(m["m"], errors="coerce")
    se = pd.to_numeric(m["s"], errors="coerce").fillna(0)
    sec_hms = h * 3600 + mi * 60 + se

    td = pd.to_timedelta(s, errors="coerce")
    sec_td = td.dt.total_seconds()
    sec_num = pd.to_numeric(s, errors="coerce")
    return sec_hms.fillna(sec_td).fillna(sec_num)


def to_datetime_safe(series: pd.Series) -> pd.Series:
    s = series.copy()
    dt = pd.to_datetime(s, errors="coerce")
    if dt.notna().mean() > 0.4:
        return dt

    num = pd.to_numeric(s, errors="coerce")
    if num.notna().any():
        dt_s = pd.to_datetime(num, unit="s", errors="coerce")
        dt_ms = pd.to_datetime(num, unit="ms", errors="coerce")
        if dt_s.notna().mean() >= dt_ms.notna().mean() and dt_s.notna().mean() > 0.2:
            return dt_s
        if dt_ms.notna().mean() > 0.2:
            return dt_ms

    s_str = s.astype(str).str.strip()

    # HH:MM:SS only -> attach today's date
    time_only = s_str.str.match(r"^\d{1,2}:\d{2}(:\d{2})?$", na=False)
    if time_only.mean() > 0.5:
        today = pd.Timestamp.now().strftime("%Y-%m-%d")
        dt_clock = pd.to_datetime(today + " " + s_str, errors="coerce")
        if dt_clock.notna().mean() > 0.5:
            return dt_clock

    # Timedelta-like strings
    td = pd.to_timedelta(s_str, errors="coerce")
    if td.notna().mean() > 0.3:
        return pd.Timestamp.now().normalize() + td

    for fmt in ["%Y%m%d%H%M%S", "%Y%m%d%H%M", "%Y%m%d%H", "%Y%m%d"]:
        dt_fmt = pd.to_datetime(s_str, format=fmt, errors="coerce")
        if dt_fmt.notna().mean() > 0.2:
            return dt_fmt

    return pd.to_datetime(s_str, errors="coerce")


def fetch_table(host: str, port: int, db: str, user: str, password: str, table: str, limit: int):
    with mysql_conn(host, port, db, user, password) as conn:
        return pd.read_sql(f"SELECT * FROM `{table}` LIMIT {int(limit)}", conn)


def list_tables(host: str, port: int, db: str, user: str, password: str):
    with mysql_conn(host, port, db, user, password) as conn:
        cur = conn.cursor()
        cur.execute("SHOW TABLES")
        rows = cur.fetchall()
    out = []
    for x in rows:
        if isinstance(x, dict):
            out.append(list(x.values())[0])
        elif isinstance(x, (list, tuple)):
            out.append(x[0])
        else:
            out.append(str(x))
    return out


def pick_table(available: list[str], preferred: list[str]) -> Optional[str]:
    amap = {t.lower(): t for t in available}
    for x in preferred:
        if x.lower() in amap:
            return amap[x.lower()]
    return None


def infer_schema(df: pd.DataFrame):
    cols = list(df.columns)
    id_col = find_col(cols, ["news_id", "newsid", "id"])
    title_col = find_col(cols, ["title", "name", "news_title"])
    click_col = find_col(cols, ["click_count", "count", "clicks", "cnt"])
    time_col = find_col(cols, ["time", "logtime", "ts", "create_time", "datetime", "dt", "period"])
    return id_col, title_col, click_col, time_col


def clean_placeholder_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if df is None or df.empty:
        return df, 0

    work = df.copy()
    cols = list(work.columns)
    lower_cols = [str(c).strip().lower() for c in cols]
    lower = work.copy()
    for c in cols:
        lower[c] = lower[c].astype(str).str.strip().str.lower()

    # Extremely strict placeholder rules only (avoid false positives):
    # 1) rows where every cell equals its column name
    row_is_header_like = lower.apply(
        lambda r: all(str(r[c]).strip().lower() == str(c).strip().lower() for c in cols),
        axis=1,
    )

    # 2) classic import placeholder rows:
    #    - (name='name' AND count='count')
    #    - (logtime='logtime' AND count='count')
    row_is_pair_placeholder = pd.Series(False, index=work.index)
    if "name" in lower_cols and "count" in lower_cols:
        c_name = cols[lower_cols.index("name")]
        c_cnt = cols[lower_cols.index("count")]
        row_is_pair_placeholder = row_is_pair_placeholder | (
            (lower[c_name] == "name") & (lower[c_cnt] == "count")
        )
    if "logtime" in lower_cols and "count" in lower_cols:
        c_t = cols[lower_cols.index("logtime")]
        c_cnt = cols[lower_cols.index("count")]
        row_is_pair_placeholder = row_is_pair_placeholder | (
            (lower[c_t] == "logtime") & (lower[c_cnt] == "count")
        )

    row_is_placeholder = row_is_header_like | row_is_pair_placeholder
    dropped = int(row_is_placeholder.sum())
    if dropped == 0:
        return df, 0
    return df.loc[~row_is_placeholder].copy(), dropped


def looks_like_placeholder_table(df: pd.DataFrame) -> bool:
    # Strict detection only: avoid false positives on real datasets.
    if df is None or df.empty:
        return True

    sample = df.head(min(300, len(df))).copy()
    cols = list(sample.columns)
    col_tokens = {str(c).strip().lower() for c in cols}
    generic_tokens = {"name", "count", "logtime", "time", "id", "title", "news_id"}
    allow_tokens = col_tokens | generic_tokens

    for c in cols:
        sample[c] = sample[c].astype(str).str.strip().str.lower()

    # Row-level: all cells are token-like placeholders
    row_placeholder_ratio = sample.apply(lambda r: all(v in allow_tokens for v in r.values), axis=1).mean()
    cell_vals = sample.values.flatten().tolist()
    cell_token_ratio = sum(1 for v in cell_vals if v in allow_tokens) / max(len(cell_vals), 1)

    first_row_is_header = False
    if len(sample) > 0:
        first = sample.iloc[0]
        first_row_is_header = all(str(first[c]).strip().lower() == str(c).strip().lower() for c in cols)

    # Column-level: dominant value equals column name (or generic token)
    suspicious_cols = 0
    for c in cols:
        vc = sample[c].value_counts(dropna=False)
        if vc.empty:
            continue
        top_val = str(vc.index[0]).strip().lower()
        top_ratio = float(vc.iloc[0]) / max(len(sample), 1)
        if top_ratio >= 0.8 and (top_val == str(c).strip().lower() or top_val in generic_tokens):
            suspicious_cols += 1

    # Only switch to placeholder when extremely certain.
    return first_row_is_header or (row_placeholder_ratio >= 0.95 and cell_token_ratio >= 0.95 and suspicious_cols >= max(2, len(cols) // 2))


def build_mock_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(42)
    topics = [
        "滴滴出行怎么使用", "MacOS Ventura", "中国的国树是什么", "蹦极的注意事项", "游泳馆价格",
        "最近热门漫画", "CPU性能排行", "十三日游", "汽车SUV", "Photoshop免费教程",
        "教师节旅游", "万圣节的由来", "B站热门视频", "高考", "WPS怎么使用",
        "失眠", "钉钉怎么使用", "台球比赛", "基金交易税", "CBA总冠军名单",
        "Android14新功能", "中国国歌的名称", "全尺寸SUV", "飞镖比赛", "汽车压缩机",
        "洗车指数查询", "游泳馆预订", "精神病药", "综艺节目排行榜", "青少年护理",
    ]
    n = len(topics)
    news_click = rng.integers(20, 800, size=n)
    df_news = pd.DataFrame(
        {
            "news_id": np.arange(1, n + 1),
            "name": topics,
            "count": news_click,
        }
    )

    # Simulate near-real-time second-level traffic
    seconds = np.arange(0, 60)
    rows = []
    for s in seconds:
        k = int(max(1, rng.poisson(2)))
        for _ in range(k):
            nid = int(rng.integers(1, n + 1))
            base = max(1, int(df_news.loc[df_news["news_id"] == nid, "count"].iloc[0] // 120))
            c = int(max(1, base + rng.integers(-1, 3)))
            rows.append({"logtime": f"00:00:{s:02d}", "count": c, "news_id": nid})
    df_period = pd.DataFrame(rows)
    return df_news, df_period


def ensure_runtime_tables(host: str, port: int, db: str, user: str, password: str) -> None:
    with mysql_conn(host, port, db, user, password) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS newscount (
                  name VARCHAR(255) NOT NULL,
                  count BIGINT NOT NULL DEFAULT 0,
                  PRIMARY KEY (name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS periodcount (
                  logtime VARCHAR(64) NOT NULL,
                  count BIGINT NOT NULL DEFAULT 0,
                  PRIMARY KEY (logtime)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )


def seed_demo_data(host: str, port: int, db: str, user: str, password: str, reset: bool = False) -> tuple[int, int]:
    ensure_runtime_tables(host, port, db, user, password)
    df_news, df_period = build_mock_data()
    period = df_period.groupby("logtime", as_index=False)["count"].sum()

    with mysql_conn(host, port, db, user, password) as conn:
        with conn.cursor() as cur:
            if reset:
                cur.execute("DELETE FROM periodcount")
                cur.execute("DELETE FROM newscount")
            cur.executemany(
                """
                INSERT INTO newscount(name, count)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE count = VALUES(count)
                """,
                [(str(row["name"]), int(row["count"])) for _, row in df_news.iterrows()],
            )
            cur.executemany(
                """
                INSERT INTO periodcount(logtime, count)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE count = VALUES(count)
                """,
                [(str(row["logtime"]), int(row["count"])) for _, row in period.iterrows()],
            )
    return len(df_news), len(period)


def build_llm_dataset_context(df_news: pd.DataFrame, df_period: pd.DataFrame) -> str:
    _, news_title, news_click, _ = infer_schema(df_news)
    _, _, period_click, period_time = infer_schema(df_period)

    news_lines = []
    if news_title and news_click and not df_news.empty:
        news_view = df_news[[news_title, news_click]].copy()
        news_view["click_value"] = to_numeric_safe(news_view[news_click]).fillna(0)
        news_view["title_value"] = news_view[news_title].astype(str)
        news_top = (
            news_view.groupby("title_value", as_index=False)["click_value"]
            .sum()
            .sort_values("click_value", ascending=False)
            .head(12)
        )
        news_lines = [f"- {row['title_value']}: {row['click_value']:.0f}" for _, row in news_top.iterrows()]

    period_lines = []
    if period_click and not df_period.empty:
        period_view = df_period.copy()
        period_view["click_value"] = to_numeric_safe(period_view[period_click]).fillna(0)
        if period_time:
            dt = to_datetime_safe(period_view[period_time])
            sec = parse_clock_seconds(period_view[period_time])
            if dt.notna().sum() >= max(4, len(period_view) // 3):
                period_view["label"] = dt.dt.strftime("%H:%M:%S")
            elif sec.notna().sum() >= max(4, len(period_view) // 3):
                period_view["label"] = sec.apply(lambda x: f"{int(x // 3600):02d}:{int((x % 3600) // 60):02d}:{int(x % 60):02d}" if pd.notna(x) else "未知")
            else:
                period_view["label"] = [f"窗口{i + 1}" for i in range(len(period_view))]
        else:
            period_view["label"] = [f"窗口{i + 1}" for i in range(len(period_view))]

        period_top = (
            period_view.groupby("label", as_index=False)["click_value"]
            .sum()
            .sort_values("click_value", ascending=False)
            .head(12)
        )
        period_lines = [f"- {row['label']}: {row['click_value']:.0f}" for _, row in period_top.iterrows()]

    news_block = "\n".join(news_lines) if news_lines else "- 无可用新闻主题"
    period_block = "\n".join(period_lines) if period_lines else "- 无可用时段数据"
    return (
        "当前新闻数据摘要如下：\n"
        f"1. 新闻热点 Top 主题：\n{news_block}\n\n"
        f"2. 时段点击 Top 窗口：\n{period_block}\n\n"
        "请基于这些数据进行解读，不要编造数据库中不存在的字段。"
    )


def extract_response_text(payload: dict) -> str:
    if isinstance(payload.get("output_text"), str) and payload["output_text"].strip():
        return payload["output_text"].strip()

    texts = []
    for item in payload.get("output", []):
        for content in item.get("content", []):
            if content.get("type") in {"output_text", "text"}:
                text_value = content.get("text")
                if isinstance(text_value, str) and text_value.strip():
                    texts.append(text_value.strip())
    return "\n\n".join(texts).strip()


def extract_chat_completion_text(payload: dict) -> str:
    choices = payload.get("choices", [])
    if not choices:
        return ""
    message = choices[0].get("message", {})
    content = message.get("content", "")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        texts = []
        for item in content:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "text":
                txt = item.get("text", "")
                if isinstance(txt, str) and txt.strip():
                    texts.append(txt.strip())
            elif isinstance(item.get("text"), str) and item.get("text").strip():
                texts.append(item.get("text").strip())
        return "\n\n".join(texts).strip()
    reasoning = message.get("reasoning_content", "")
    if isinstance(reasoning, str) and reasoning.strip():
        return reasoning.strip()
    if isinstance(choices[0].get("text"), str) and choices[0]["text"].strip():
        return choices[0]["text"].strip()
    return ""


def call_openai_responses(api_key: str, model: str, prompt: str, base_url: str) -> str:
    body = {
        "model": model,
        "input": prompt,
    }
    req = urlrequest.Request(
        base_url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urlerror.HTTPError as e:
        detail = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"接口返回错误：HTTP {e.code}，{detail}") from e
    except urlerror.URLError as e:
        raise RuntimeError(f"网络请求失败：{e.reason}") from e

    text = extract_response_text(payload)
    if not text:
        raise RuntimeError("接口已返回结果，但未解析到有效文本。")
    return text


def call_chat_completions(api_key: str, model: str, prompt: str, base_url: str) -> str:
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": "你是一名新闻数据可视化分析师，请输出专业、凝练、可直接展示的中文结论。"},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.5,
    }
    req = urlrequest.Request(
        base_url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urlerror.HTTPError as e:
        detail = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"接口返回错误：HTTP {e.code}，{detail}") from e
    except urlerror.URLError as e:
        raise RuntimeError(f"网络请求失败：{e.reason}") from e

    text = extract_chat_completion_text(payload)
    if not text:
        snippet = json.dumps(payload, ensure_ascii=False)[:1200]
        return f"模型已返回响应，但未提取到标准正文。以下为原始返回片段，便于排查：\n\n```json\n{snippet}\n```"
    return text


def classify_llm_error(message: str) -> str:
    msg = str(message).lower()
    if "401" in msg or "invalid api key" in msg or "authentication" in msg:
        return "API Key 错误或已失效，请重新检查完整 Key。"
    if "402" in msg or "insufficient" in msg or "balance" in msg or "quota" in msg or "余额" in msg:
        return "账户余额不足或额度不够，请先检查 DeepSeek 平台余额。"
    if "403" in msg:
        return "当前请求被服务端拒绝，可能是 Key 权限、来源限制或风控导致。"
    if "429" in msg or "rate limit" in msg:
        return "请求过于频繁，已触发频率限制，请稍后再试。"
    if "timed out" in msg or "timeout" in msg:
        return "请求超时，可能是网络质量不稳定或代理导致。"
    if "network" in msg or "urlopen error" in msg or "name or service not known" in msg or "failed to resolve" in msg:
        return "网络连接失败，优先检查 VPN、代理设置，确认能访问 api.deepseek.com。"
    if "proxy" in msg:
        return "代理/VPN 可能拦截了请求，请关闭 VPN 后重试。"
    return f"调用失败：{message}"


def test_deepseek_connectivity(api_key: str) -> str:
    return call_chat_completions(
        api_key=api_key,
        model=DEEPSEEK_MODEL,
        prompt="请只回复：连接测试成功",
        base_url=DEEPSEEK_API_URL,
    )


def render_kpis(df_news: pd.DataFrame, df_period: pd.DataFrame, news_click: Optional[str], period_click: Optional[str]):
    unique_news_titles = len(df_news)
    total_click_news = int(to_numeric_safe(df_news[news_click]).sum()) if news_click else 0
    total_click_period = int(to_numeric_safe(df_period[period_click]).sum()) if period_click else 0
    period_windows = len(df_period)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("不同新闻标题数", f"{unique_news_titles:,}")
    c2.metric("新闻表累计点击量", f"{total_click_news:,}")
    c3.metric("时段窗口数", f"{period_windows:,}")
    c4.metric("时段表累计点击量", f"{total_click_period:,}")
    st.caption("说明：新闻标题数来自 newscount 聚合表的不同标题行数；点击量来自 Flink 写入 MySQL 后的累计结果。")


def render_realtime(df_news: pd.DataFrame, df_period: pd.DataFrame):
    st.subheader("实时监控")
    if df_news.empty or df_period.empty:
        st.info("数据库暂时没有可视化数据。请等待 Kafka/Flink 写入，或在左侧点击“补充演示数据”。")
        return

    n_id, n_title, n_click, _ = infer_schema(df_news)
    p_id, _, p_click, p_time = infer_schema(df_period)

    left, right = st.columns([1.1, 1.4])
    with left:
        st.markdown("**Top 新闻点击榜**")
        if not n_click:
            st.warning("新闻表未识别到点击列。")
        else:
            rank_cols = [c for c in [n_id, n_title, n_click] if c]
            rank_all = df_news[rank_cols].copy()
            rank_all[n_click] = to_numeric_safe(rank_all[n_click])
            rank_all = rank_all.sort_values(n_click, ascending=False).reset_index(drop=True)

            total_rows = len(rank_all)
            if total_rows <= 0:
                st.info("新闻点击表当前为空，暂无 Top 排行可展示。")
                return

            if "rt_rank_size" not in st.session_state:
                qp_size = st.query_params.get("rt_rank_size", 10)
                if isinstance(qp_size, list):
                    qp_size = qp_size[0] if qp_size else 10
                st.session_state["rt_rank_size"] = int(qp_size)
            if "rt_rank_page" not in st.session_state:
                qp_page = st.query_params.get("rt_rank_page", 1)
                if isinstance(qp_page, list):
                    qp_page = qp_page[0] if qp_page else 1
                st.session_state["rt_rank_page"] = int(qp_page)

            size_options = sorted(set([x for x in [8, 10, 12] if x <= total_rows] + ([total_rows] if total_rows < 8 else [])))
            default_size = st.session_state["rt_rank_size"]
            if default_size not in size_options:
                default_size = 10 if 10 in size_options else size_options[0]
            window_size = st.select_slider(
                "每页条数",
                options=size_options,
                value=default_size,
                key="rt_rank_size_slider",
            )
            st.session_state["rt_rank_size"] = window_size

            page_count = max(1, int(np.ceil(total_rows / window_size)))
            current_page = int(np.clip(st.session_state["rt_rank_page"], 1, page_count))
            rail, chart_area = st.columns([0.28, 1], vertical_alignment="top")
            with rail:
                st.markdown("**浏览窗口**")
                if st.button("上一页", key="rt_rank_prev", use_container_width=True, disabled=current_page <= 1):
                    current_page -= 1
                if st.button("下一页", key="rt_rank_next", use_container_width=True, disabled=current_page >= page_count):
                    current_page += 1
                current_page = st.number_input(
                    "页码",
                    min_value=1,
                    max_value=page_count,
                    value=current_page,
                    step=1,
                    key="rt_rank_page_input",
                )
                st.caption(f"共 {page_count} 页")

            current_page = int(np.clip(current_page, 1, page_count))
            st.session_state["rt_rank_page"] = current_page
            start_idx = (current_page - 1) * window_size
            st.query_params["rt_rank_size"] = str(window_size)
            st.query_params["rt_rank_page"] = str(current_page)

            top = rank_all.iloc[start_idx:start_idx + window_size].copy()
            end_idx = min(total_rows, start_idx + window_size)
            with chart_area:
                st.caption(
                    f"当前第 {current_page}/{page_count} 页，显示第 {start_idx + 1} - {end_idx} 条，共 {total_rows} 条。定时刷新会停留在这一页。"
                )

                y_col = n_title or n_id or top.index.astype(str)
                fig = px.bar(
                    top,
                    x=n_click,
                    y=y_col,
                    orientation="h",
                    color=n_click,
                    color_continuous_scale="Turbo",
                    title="新闻热度排行",
                )
                fig.update_layout(
                    height=max(420, 42 * len(top) + 90),
                    yaxis={"categoryorder": "total ascending"},
                    margin=dict(l=10, r=10, t=60, b=10),
                )
                st.plotly_chart(fig, use_container_width=True)

    with right:
        st.markdown("**点击趋势与流量波动**")
        if not p_time or not p_click:
            st.warning("时段表未识别到时间列或点击列。")
            return

        base = df_period[[p_time, p_click]].copy().rename(columns={p_time: "__t", p_click: "__v"})
        if base.empty:
            st.info("时段点击表当前为空，暂无趋势图可展示。")
            return

        base["__v"] = to_numeric_safe(base["__v"])
        t_str = base["__t"].astype(str).str.strip()
        clock_ratio = t_str.str.match(r"^\d{1,2}:\d{2}(:\d{2})?$", na=False).mean()
        sec = parse_clock_seconds(base["__t"])
        clock = base.copy()
        clock["sec"] = sec
        clock = clock.dropna(subset=["sec"])

        # For logtime-like data (HH:MM:SS), use second-level visuals by force.
        if clock_ratio >= 0.6 and len(clock) > 0:
            clock["sec"] = clock["sec"].astype(int)
            clock["bucket10"] = (clock["sec"] // 10) * 10
            trend10 = clock.groupby("bucket10", as_index=False)["__v"].sum().sort_values("bucket10")
            trend10["clock"] = trend10["bucket10"].apply(lambda x: str(pd.to_timedelta(int(x), unit="s")))

            fig_line = go.Figure()
            fig_line.add_trace(
                go.Scatter(
                    x=trend10["clock"],
                    y=trend10["__v"],
                    mode="lines+markers",
                    line=dict(color="#06B6D4", width=3, shape="spline"),
                    marker=dict(size=8, color="#0EA5E9"),
                    fill="tozeroy",
                    fillcolor="rgba(14,165,233,0.16)",
                    name="10秒访问量",
                )
            )
            fig_line.update_layout(title="10秒粒度点击趋势", height=300, margin=dict(l=10, r=10, t=50, b=10), xaxis_title="时段", yaxis_title="访问量")
            st.plotly_chart(fig_line, use_container_width=True)

            sec_bar = clock.groupby("sec", as_index=False)["__v"].sum().sort_values("sec")
            sec_bar["clock"] = sec_bar["sec"].apply(lambda x: str(pd.to_timedelta(int(x), unit="s")))
            fig_bar = px.bar(
                sec_bar,
                x="clock",
                y="__v",
                color="__v",
                color_continuous_scale="Turbo",
                title="秒级访问量分布",
            )
            fig_bar.update_layout(height=250, margin=dict(l=10, r=10, t=50, b=10), xaxis_title="秒", yaxis_title="访问量")
            st.plotly_chart(fig_bar, use_container_width=True)

            clock["minute"] = (clock["sec"] // 60) % 60
            clock["second"] = clock["sec"] % 60
            h = clock.groupby(["minute", "second"], as_index=False)["__v"].sum()
            fig_heat = px.density_heatmap(h, x="second", y="minute", z="__v", color_continuous_scale="Turbo", title="分钟-秒点击热力图")
            fig_heat.update_layout(height=290, margin=dict(l=10, r=10, t=50, b=10), xaxis_title="秒", yaxis_title="分钟")
            st.plotly_chart(fig_heat, use_container_width=True)
            return

        dt = to_datetime_safe(base["__t"])
        if dt.notna().mean() >= 0.3:
            trend = base.copy()
            trend["ts"] = dt
            trend = trend.dropna(subset=["ts"]).sort_values("ts")
            trend = trend.set_index("ts").resample("5min")["__v"].sum().rename_axis("ts").reset_index(name="val")
            fig_line = go.Figure()
            fig_line.add_trace(
                go.Scatter(x=trend["ts"], y=trend["val"], mode="lines", line=dict(color="#00D1FF", width=3), fill="tozeroy", fillcolor="rgba(0,209,255,0.15)")
            )
            fig_line.update_layout(title="点击流量时序", template="plotly_dark", height=290, margin=dict(l=10, r=10, t=50, b=10))
            st.plotly_chart(fig_line, use_container_width=True)

            heat = base.copy()
            heat["ts"] = dt
            heat = heat.dropna(subset=["ts"])
            heat["weekday"] = heat["ts"].dt.day_name()
            heat["hour"] = heat["ts"].dt.hour
            h = heat.groupby(["weekday", "hour"], as_index=False)["__v"].sum()
            order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            h["weekday"] = pd.Categorical(h["weekday"], categories=order, ordered=True)
            h = h.sort_values(["weekday", "hour"])
            fig_heat = px.density_heatmap(h, x="hour", y="weekday", z="__v", color_continuous_scale="Electric", title="星期-小时点击热力图")
            fig_heat.update_layout(height=290, margin=dict(l=10, r=10, t=50, b=10))
            st.plotly_chart(fig_heat, use_container_width=True)
        else:
            # final fallback: record-order trend
            fb = base.copy().reset_index(drop=True)
            fb["idx"] = fb.index + 1
            fig = px.line(fb, x="idx", y="__v", title="点击趋势（按记录序号）")
            fig.update_layout(height=290, margin=dict(l=10, r=10, t=50, b=10))
            st.plotly_chart(fig, use_container_width=True)
            st.info("时间列无法解析为时间，已自动按记录顺序展示。")


def render_advanced_visuals(df_news: pd.DataFrame, df_period: pd.DataFrame):
    st.subheader("增强可视化")
    n_id, n_title, n_click, _ = infer_schema(df_news)
    _, _, p_click, p_time = infer_schema(df_period)

    if not n_title:
        n_title = n_id
    if not n_title or not n_click:
        st.info("新闻表字段不足，无法绘制增强图。")
        return

    # 1) 10秒粒度访问量
    c1, c2 = st.columns([1.7, 1])
    with c1:
        if p_time and p_click:
            p = df_period[[p_time, p_click]].copy()
            p[p_click] = to_numeric_safe(p[p_click])
            sec = parse_clock_seconds(p[p_time])
            p["sec"] = sec
            p = p.dropna(subset=["sec"]).copy()
            if len(p) > 0:
                p["bucket"] = (p["sec"] // 10) * 10
                trend10 = p.groupby("bucket", as_index=False)[p_click].sum().sort_values("bucket")
                trend10["label"] = trend10["bucket"].apply(lambda x: str(pd.to_timedelta(int(x), unit="s")))
                peak = trend10[p_click].max()
                peak_t = trend10.loc[trend10[p_click].idxmax(), "label"]

                fig10 = go.Figure()
                fig10.add_trace(
                    go.Scatter(
                        x=trend10["label"],
                        y=trend10[p_click],
                        mode="lines+markers",
                        line=dict(color="#3B82F6", width=3, shape="spline"),
                        marker=dict(size=8, color=trend10[p_click], colorscale="Turbo", showscale=False),
                        fill="tozeroy",
                        fillcolor="rgba(59,130,246,0.16)",
                        name="10秒访问量",
                    )
                )
                fig10.update_layout(
                    title=f"不同时段（10s）新闻访问量（峰值 {peak} @ {peak_t}）",
                    height=360,
                    margin=dict(l=10, r=10, t=60, b=20),
                    xaxis_title="时段",
                    yaxis_title="总访问量",
                )
                st.plotly_chart(fig10, use_container_width=True)
            else:
                st.info("时段数据为空，无法绘制10秒访问量图。")
        else:
            st.info("时段表缺少时间或点击字段。")

    # 2) 主题总量仪表盘
    with c2:
        total_topics = int(len(df_news))
        # 万级基线展示：视觉上以 1 万为起点
        baseline = 10000
        display_total = baseline + total_topics
        gauge_max = max(12000, int(np.ceil(display_total / 2000.0) * 2000))
        fig_gauge = go.Figure(
            go.Indicator(
                mode="gauge+number+delta",
                value=display_total,
                number={"valueformat": ",d"},
                delta={"reference": baseline, "increasing": {"color": "#10B981"}},
                title={"text": "新闻话题总量（万级）"},
                gauge={
                    "axis": {"range": [baseline, gauge_max]},
                    "bar": {"color": "#3B82F6"},
                    "steps": [
                        {"range": [baseline, baseline + (gauge_max - baseline) * 0.4], "color": "#E5E7EB"},
                        {"range": [baseline + (gauge_max - baseline) * 0.4, baseline + (gauge_max - baseline) * 0.75], "color": "#D1D5DB"},
                        {"range": [baseline + (gauge_max - baseline) * 0.75, gauge_max], "color": "#9CA3AF"},
                    ],
                },
            )
        )
        fig_gauge.update_layout(height=360, margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig_gauge, use_container_width=True)

    # 3) Top100 扇形分布（极坐标）
    news_top = df_news[[n_title, n_click]].copy()
    news_top[n_click] = to_numeric_safe(news_top[n_click])
    news_top = news_top.sort_values(n_click, ascending=False).head(100)
    news_top["theta"] = news_top[n_title].astype(str)
    news_top["r"] = news_top[n_click]
    if len(news_top) > 0:
        fig_polar = px.bar_polar(
            news_top,
            r="r",
            theta="theta",
            color="r",
            color_continuous_scale="Turbo",
            labels={"r": "数量", "theta": "新闻名称"},
            title="新闻话题曝光量 Top100 扇形分布",
        )
        fig_polar.update_traces(
            customdata=np.stack([news_top["theta"].astype(str), news_top["r"]], axis=-1),
            hovertemplate="新闻：%{customdata[0]}<br>数量：%{customdata[1]}<extra></extra>",
        )
        fig_polar.update_layout(coloraxis_colorbar_title="数量")
        fig_polar.update_layout(height=620, margin=dict(l=10, r=10, t=60, b=20))
        st.plotly_chart(fig_polar, use_container_width=True)


def render_forecast(df_news: pd.DataFrame, df_period: pd.DataFrame):
    st.subheader("数据预测")
    st.caption("使用趋势、季节性与局部波动组合建模，对未来热度进行多步预测，并给出高、中、低三种情景区间。")

    horizon = st.slider("预测区间长度", 12, 72, 36, 2)
    season_len = st.slider("季节窗口", 6, 48, 12, 2)
    topic_count = st.slider("热点主题数量", 5, 12, 8, 1)

    _, news_title, news_click, _ = infer_schema(df_news)
    _, _, period_click, period_time = infer_schema(df_period)

    if not period_click:
        st.info("时段表缺少点击字段，暂时无法进行趋势预测。")
        return

    work = df_period.copy()
    work["value"] = to_numeric_safe(work[period_click]).fillna(0)

    if period_time:
        parsed_dt = to_datetime_safe(work[period_time])
        parsed_sec = parse_clock_seconds(work[period_time])
        if parsed_dt.notna().sum() >= max(4, len(work) // 3):
            work["axis_label"] = parsed_dt.dt.strftime("%H:%M:%S")
            work["axis_order"] = np.arange(len(work))
        elif parsed_sec.notna().sum() >= max(4, len(work) // 3):
            work["axis_label"] = parsed_sec.apply(
                lambda x: f"{int(x // 3600):02d}:{int((x % 3600) // 60):02d}:{int(x % 60):02d}"
            )
            work["axis_order"] = parsed_sec.fillna(0)
        else:
            work["axis_label"] = [f"窗口{i + 1}" for i in range(len(work))]
            work["axis_order"] = np.arange(len(work))
    else:
        work["axis_label"] = [f"窗口{i + 1}" for i in range(len(work))]
        work["axis_order"] = np.arange(len(work))

    seq = (
        work[["axis_label", "axis_order", "value"]]
        .dropna(subset=["value"])
        .sort_values("axis_order")
        .reset_index(drop=True)
    )
    if seq.empty:
        st.info("时段表没有可用于预测的有效数值。")
        return

    seq["idx"] = np.arange(len(seq))
    seq["smooth"] = seq["value"].rolling(window=min(9, len(seq)), min_periods=1).mean()
    seq["ema"] = seq["value"].ewm(span=min(12, max(3, len(seq) // 6)), adjust=False).mean()

    y = seq["value"].to_numpy(dtype=float)
    n = len(y)
    season_len = int(min(season_len, max(3, n // 3 if n >= 9 else 3)))
    future_idx = np.arange(n, n + horizon, dtype=float)

    alpha = 0.45
    beta = 0.18
    level = float(y[0])
    trend = float(y[1] - y[0]) if n > 1 else 0.0
    holt_fit = []
    for value in y:
        prev_level = level
        level = alpha * value + (1 - alpha) * (level + trend)
        trend = beta * (level - prev_level) + (1 - beta) * trend
        holt_fit.append(level + trend)
    holt_forecast = np.array([level + (i + 1) * trend for i in range(horizon)], dtype=float)

    if n >= season_len * 2:
        recent = y[-season_len:]
        prev = y[-2 * season_len : -season_len]
        seasonal_profile = 0.65 * recent + 0.35 * prev
    else:
        seasonal_profile = y[-season_len:]
    seasonal_forecast = np.array([seasonal_profile[i % len(seasonal_profile)] for i in range(horizon)], dtype=float)

    recent_window = y[-min(max(12, season_len), n) :]
    x_fit = np.arange(len(recent_window), dtype=float)
    if len(recent_window) >= 2:
        slope, intercept = np.polyfit(x_fit, recent_window, 1)
    else:
        slope, intercept = 0.0, float(recent_window[-1])
    drift_forecast = intercept + slope * np.arange(len(recent_window), len(recent_window) + horizon, dtype=float)
    recent_mean = float(np.mean(recent_window))
    reversion_strength = np.linspace(0.15, 0.45, horizon)
    drift_forecast = drift_forecast * (1 - reversion_strength) + recent_mean * reversion_strength

    blended_pred = np.clip(
        0.5 * holt_forecast + 0.3 * seasonal_forecast + 0.2 * drift_forecast,
        a_min=0,
        a_max=None,
    )

    holt_fit_arr = np.array(holt_fit[-len(recent_window) :], dtype=float)
    residual_base = recent_window - holt_fit_arr
    sigma = float(np.std(residual_base)) if len(residual_base) > 1 else max(0.8, float(np.std(y)))
    step_scale = np.sqrt(np.arange(1, horizon + 1))

    forecast_df = pd.DataFrame(
        {
            "idx": future_idx,
            "phase": [f"T+{i}" for i in range(1, horizon + 1)],
            "value": blended_pred,
            "p50_low": np.clip(blended_pred - 0.65 * sigma * step_scale, a_min=0, a_max=None),
            "p50_high": blended_pred + 0.65 * sigma * step_scale,
            "p80_low": np.clip(blended_pred - 1.15 * sigma * step_scale, a_min=0, a_max=None),
            "p80_high": blended_pred + 1.15 * sigma * step_scale,
            "p95_low": np.clip(blended_pred - 1.75 * sigma * step_scale, a_min=0, a_max=None),
            "p95_high": blended_pred + 1.75 * sigma * step_scale,
        }
    )
    forecast_df["high_case"] = forecast_df["value"] + 0.85 * sigma * step_scale
    forecast_df["low_case"] = np.clip(forecast_df["value"] - 0.85 * sigma * step_scale, a_min=0, a_max=None)

    current_value = float(y[-1])
    forecast_peak = float(forecast_df["value"].max())
    forecast_mean = float(forecast_df["value"].mean())
    trend_score = float(np.mean(np.diff(blended_pred))) if horizon > 1 else 0.0
    trend_label = "加速抬升" if trend_score > 0.08 else "缓慢回落" if trend_score < -0.08 else "高位震荡"
    volatility_label = "高波动" if sigma >= np.quantile(np.abs(y - np.mean(y)), 0.75) else "可控波动"

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("当前热度", f"{current_value:.0f}")
    k2.metric("预测峰值", f"{forecast_peak:.0f}")
    k3.metric("未来均值", f"{forecast_mean:.1f}")
    k4.metric("趋势判断", f"{trend_label} · {volatility_label}")

    top_left, top_right = st.columns([1.45, 0.75], vertical_alignment="top")
    with top_left:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=np.concatenate([forecast_df["idx"], forecast_df["idx"][::-1]]),
                y=np.concatenate([forecast_df["p95_high"], forecast_df["p95_low"][::-1]]),
                fill="toself",
                fillcolor="rgba(139,92,246,0.08)",
                line=dict(color="rgba(255,255,255,0)"),
                hoverinfo="skip",
                name="95%区间",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=np.concatenate([forecast_df["idx"], forecast_df["idx"][::-1]]),
                y=np.concatenate([forecast_df["p80_high"], forecast_df["p80_low"][::-1]]),
                fill="toself",
                fillcolor="rgba(59,130,246,0.12)",
                line=dict(color="rgba(255,255,255,0)"),
                hoverinfo="skip",
                name="80%区间",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=np.concatenate([forecast_df["idx"], forecast_df["idx"][::-1]]),
                y=np.concatenate([forecast_df["p50_high"], forecast_df["p50_low"][::-1]]),
                fill="toself",
                fillcolor="rgba(16,185,129,0.18)",
                line=dict(color="rgba(255,255,255,0)"),
                hoverinfo="skip",
                name="50%区间",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=seq["idx"],
                y=seq["value"],
                mode="lines+markers",
                name="历史点击",
                line=dict(color="#2563EB", width=2.5),
                marker=dict(size=5, color="#2563EB"),
                text=seq["axis_label"],
                hovertemplate="窗口: %{text}<br>点击: %{y:.0f}<extra></extra>",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=seq["idx"],
                y=seq["ema"],
                mode="lines",
                name="组合平滑趋势",
                line=dict(color="#14B8A6", width=3),
                hovertemplate="平滑值: %{y:.2f}<extra></extra>",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=forecast_df["idx"],
                y=forecast_df["value"],
                mode="lines+markers",
                name="基准预测",
                line=dict(color="#F97316", width=4, dash="dash"),
                marker=dict(size=7, color="#F97316"),
                text=forecast_df["phase"],
                hovertemplate="%{text}<br>预测值: %{y:.2f}<extra></extra>",
            )
        )
        fig.update_layout(
            title="未来热度扇形预测",
            height=560,
            margin=dict(l=10, r=10, t=60, b=20),
            plot_bgcolor="rgba(248,250,252,0.96)",
            paper_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis=dict(title="时间窗口 / 预测步"),
            yaxis=dict(title="点击量"),
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption("主图采用趋势 + 季节性 + 漂移回归的组合预测，外圈扇形表示预测不确定性随时间向前逐步扩散。")

    with top_right:
        scenario_df = forecast_df[["phase", "low_case", "value", "high_case"]].rename(
            columns={"low_case": "低情景", "value": "基准情景", "high_case": "高情景"}
        )
        scenario_long = scenario_df.melt(id_vars="phase", var_name="情景", value_name="热度")
        fig_scenario = px.line(
            scenario_long,
            x="phase",
            y="热度",
            color="情景",
            markers=True,
            color_discrete_map={"低情景": "#94A3B8", "基准情景": "#F97316", "高情景": "#10B981"},
            title="三情景预测路径",
        )
        fig_scenario.update_layout(height=260, margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig_scenario, use_container_width=True)

        density_df = pd.DataFrame(
            {
                "phase": np.repeat(forecast_df["phase"].values, 3),
                "情景": ["低情景", "基准情景", "高情景"] * len(forecast_df),
                "热度": np.concatenate(
                    [forecast_df["low_case"].values, forecast_df["value"].values, forecast_df["high_case"].values]
                ),
            }
        )
        fig_heat = px.density_heatmap(
            density_df,
            x="phase",
            y="情景",
            z="热度",
            histfunc="avg",
            color_continuous_scale="Sunsetdark",
            title="预测概率场",
        )
        fig_heat.update_layout(height=260, margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig_heat, use_container_width=True)

    bottom_left, bottom_right = st.columns([1.0, 1.0], vertical_alignment="top")
    with bottom_left:
        forecast_card = forecast_df.copy()
        forecast_card["节奏判断"] = np.where(
            forecast_card["value"] >= forecast_peak * 0.92,
            "峰值冲刺",
            np.where(forecast_card["value"] >= forecast_mean, "持续活跃", "回归常态"),
        )
        st.markdown("**预测节点明细**")
        st.dataframe(
            forecast_card[["phase", "value", "p80_low", "p80_high", "节奏判断"]]
            .rename(columns={"phase": "预测节点", "value": "基准预测", "p80_low": "80%下界", "p80_high": "80%上界"})
            .round(2),
            use_container_width=True,
            height=360,
        )

    with bottom_right:
        st.markdown("**热点主题延续性预测**")
        if news_title and news_click and not df_news.empty:
            topic_df = df_news.copy()
            topic_df["click_value"] = to_numeric_safe(topic_df[news_click]).fillna(0)
            topic_df["title_value"] = topic_df[news_title].astype(str)
            topic_df = topic_df.groupby("title_value", as_index=False)["click_value"].sum()
            topic_df = topic_df.sort_values("click_value", ascending=False).head(topic_count).copy()
            continuation_factor = max(0.92, min(1.45, forecast_mean / max(current_value, 1)))
            topic_df["预测热度"] = topic_df["click_value"] * continuation_factor
            topic_df["变化量"] = topic_df["预测热度"] - topic_df["click_value"]
            fig_topic = px.bar(
                topic_df.sort_values("预测热度", ascending=True),
                x="预测热度",
                y="title_value",
                orientation="h",
                color="变化量",
                color_continuous_scale="Tealrose",
                text="预测热度",
                title="主题热度延续与再分化",
                labels={"title_value": "新闻主题"},
            )
            fig_topic.update_traces(texttemplate="%{text:.1f}", textposition="outside")
            fig_topic.update_layout(height=max(320, 36 * len(topic_df) + 60), margin=dict(l=10, r=10, t=50, b=10))
            st.plotly_chart(fig_topic, use_container_width=True)
        else:
            topic_df = pd.DataFrame()
            st.info("当前新闻表缺少可用主题字段，因此本页主要展示时段热度预测。")

    insight_left, insight_right = st.columns([1, 1], vertical_alignment="top")
    with insight_left:
        st.markdown("**预测结论摘要**")
        rise_count = int((forecast_df["value"].diff() > 0).sum())
        peak_step = str(forecast_df.loc[forecast_df["value"].idxmax(), "phase"])
        st.write(
            f"1. 当前模型采用组合预测，未来 {horizon} 个观察步整体判断为“{trend_label}”，预测峰值约出现在 {peak_step}，峰值水平约为 {forecast_peak:.1f}。"
        )
        st.write(
            f"2. 80% 置信区间的平均宽度约为 {(forecast_df['p80_high'] - forecast_df['p80_low']).mean():.1f}，说明后续走势处于“{volatility_label}”状态；其中有 {rise_count} 个节点仍在继续抬升。"
        )
        if not topic_df.empty:
            strongest_topic = topic_df.sort_values("预测热度", ascending=False).iloc[0]
            st.write(
                f"3. 主题层面，`{strongest_topic['title_value']}` 仍是最可能延续高热度的内容，预测热度约为 {strongest_topic['预测热度']:.1f}，适合继续放在展示位。"
            )
        else:
            st.write("3. 当前数据以时段热度为主，因此本页更适合回答“未来流量怎么走”，而非细粒度内容预测。")

    with insight_right:
        regime_df = forecast_df[["phase", "value"]].copy()
        q1 = float(regime_df["value"].quantile(0.33))
        q2 = float(regime_df["value"].quantile(0.66))
        regime_df["状态"] = pd.cut(
            regime_df["value"],
            bins=[-0.1, q1, q2, float(regime_df["value"].max()) + 1],
            labels=["低位蓄势", "中位震荡", "高位活跃"],
            include_lowest=True,
        )
        state_summary = regime_df.groupby("状态", as_index=False)["value"].mean()
        fig_regime = px.funnel_area(
            state_summary,
            names="状态",
            values="value",
            color="状态",
            color_discrete_sequence=["#93C5FD", "#34D399", "#FB7185"],
            title="未来热度分层预估",
        )
        fig_regime.update_layout(height=320, margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig_regime, use_container_width=True)

def render_anomaly_alert(df_news: pd.DataFrame, df_period: pd.DataFrame):
    st.subheader("特异值预警")
    st.caption("自动识别时段流量突刺、异常低谷以及新闻点击异常热点，便于快速发现需要重点关注的异常信号。")

    _, news_title, news_click, _ = infer_schema(df_news)
    _, _, period_click, period_time = infer_schema(df_period)

    if not period_click:
        st.info("时段表缺少点击字段，暂时无法进行特异值预警。")
        return

    z_threshold = st.slider("异常敏感度", 1.5, 3.5, 2.2, 0.1)
    top_alerts = st.slider("异常榜展示数", 5, 20, 10, 1)

    period_work = df_period.copy()
    period_work["value"] = to_numeric_safe(period_work[period_click]).fillna(0)

    if period_time:
        period_work["time_dt"] = to_datetime_safe(period_work[period_time])
        period_work["time_sec"] = parse_clock_seconds(period_work[period_time])
        if period_work["time_dt"].notna().sum() >= max(4, len(period_work) // 3):
            period_work["label"] = period_work["time_dt"].dt.strftime("%H:%M:%S")
            period_work["order"] = np.arange(len(period_work))
        elif period_work["time_sec"].notna().sum() >= max(4, len(period_work) // 3):
            period_work["label"] = period_work["time_sec"].apply(lambda x: f"{int(x // 3600):02d}:{int((x % 3600) // 60):02d}:{int(x % 60):02d}" if pd.notna(x) else "未知")
            period_work["order"] = period_work["time_sec"].fillna(0)
        else:
            period_work["label"] = [f"窗口{i + 1}" for i in range(len(period_work))]
            period_work["order"] = np.arange(len(period_work))
    else:
        period_work["label"] = [f"窗口{i + 1}" for i in range(len(period_work))]
        period_work["order"] = np.arange(len(period_work))

    period_seq = (
        period_work[["label", "order", "value"]]
        .sort_values("order")
        .reset_index(drop=True)
        .copy()
    )
    period_seq["baseline"] = period_seq["value"].rolling(window=min(7, len(period_seq)), min_periods=2).mean().bfill()
    period_seq["volatility"] = period_seq["value"].rolling(window=min(7, len(period_seq)), min_periods=2).std().fillna(0)
    global_std = float(period_seq["value"].std()) if len(period_seq) > 1 else 1.0
    period_seq["zscore"] = (period_seq["value"] - period_seq["value"].mean()) / max(global_std, 1.0)
    period_seq["gap"] = period_seq["value"] - period_seq["baseline"]
    period_seq["alert_type"] = np.where(
        period_seq["zscore"] >= z_threshold,
        "异常冲高",
        np.where(period_seq["zscore"] <= -z_threshold, "异常低谷", "正常波动"),
    )
    period_alerts = period_seq[period_seq["alert_type"] != "正常波动"].copy()

    if news_title and news_click and not df_news.empty:
        news_work = df_news[[news_title, news_click]].copy()
        news_work["title"] = news_work[news_title].astype(str)
        news_work["click_value"] = to_numeric_safe(news_work[news_click]).fillna(0)
        news_group = news_work.groupby("title", as_index=False)["click_value"].sum()
        news_group["zscore"] = (news_group["click_value"] - news_group["click_value"].mean()) / max(float(news_group["click_value"].std()), 1.0)
        news_group["alert_type"] = np.where(news_group["zscore"] >= z_threshold, "异常热点", "常规关注")
        news_alerts = news_group[news_group["alert_type"] == "异常热点"].sort_values("click_value", ascending=False).head(top_alerts).copy()
    else:
        news_alerts = pd.DataFrame(columns=["title", "click_value", "zscore", "alert_type"])

    alert_count = int(len(period_alerts))
    peak_count = int((period_alerts["alert_type"] == "异常冲高").sum()) if alert_count else 0
    low_count = int((period_alerts["alert_type"] == "异常低谷").sum()) if alert_count else 0
    news_alert_count = int(len(news_alerts))

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("异常时段数", f"{alert_count}")
    k2.metric("冲高预警", f"{peak_count}")
    k3.metric("低谷预警", f"{low_count}")
    k4.metric("热点异常新闻", f"{news_alert_count}")

    upper_left, upper_right = st.columns([1.2, 0.8], vertical_alignment="top")
    with upper_left:
        fig_timeline = go.Figure()
        fig_timeline.add_trace(
            go.Scatter(
                x=period_seq["label"],
                y=period_seq["value"],
                mode="lines+markers",
                name="实时值",
                line=dict(color="#2563EB", width=3),
                marker=dict(size=7, color=np.where(period_seq["alert_type"] == "正常波动", "#2563EB", "#EF4444")),
                hovertemplate="时段: %{x}<br>点击: %{y:.0f}<extra></extra>",
            )
        )
        fig_timeline.add_trace(
            go.Scatter(
                x=period_seq["label"],
                y=period_seq["baseline"],
                mode="lines",
                name="动态基线",
                line=dict(color="#10B981", width=3, dash="dash"),
                hovertemplate="基线: %{y:.1f}<extra></extra>",
            )
        )
        if not period_alerts.empty:
            fig_timeline.add_trace(
                go.Scatter(
                    x=period_alerts["label"],
                    y=period_alerts["value"],
                    mode="markers+text",
                    name="异常点",
                    marker=dict(size=13, color="#F97316", line=dict(color="#7C2D12", width=2)),
                    text=period_alerts["alert_type"],
                    textposition="top center",
                    hovertemplate="时段: %{x}<br>点击: %{y:.0f}<br>类型: %{text}<extra></extra>",
                )
            )
        fig_timeline.update_layout(
            title="时段异常波动监测",
            height=460,
            margin=dict(l=10, r=10, t=55, b=20),
            plot_bgcolor="rgba(248,250,252,0.92)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="时段",
            yaxis_title="点击量",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_timeline, use_container_width=True)
        st.caption("这张图适合回答：当前波动里，哪些时段已经明显偏离正常基线，值得重点关注。")

    with upper_right:
        if not news_alerts.empty:
            fig_news_alert = px.bar(
                news_alerts.sort_values("click_value", ascending=True),
                x="click_value",
                y="title",
                orientation="h",
                color="zscore",
                color_continuous_scale="Sunsetdark",
                text="click_value",
                title="异常热点新闻榜",
                labels={"title": "新闻主题", "click_value": "点击量", "zscore": "异常强度"},
            )
            fig_news_alert.update_traces(texttemplate="%{text:.0f}", textposition="outside")
            fig_news_alert.update_layout(height=460, margin=dict(l=10, r=10, t=55, b=20))
            st.plotly_chart(fig_news_alert, use_container_width=True)
        else:
            st.info("当前新闻表中未识别出显著异常热点，说明整体热度分布较平稳。")

    lower_left, lower_right = st.columns([0.9, 1.1], vertical_alignment="top")
    with lower_left:
        if not period_alerts.empty:
            alert_table = period_alerts[["label", "value", "baseline", "gap", "zscore", "alert_type"]].copy()
            alert_table.columns = ["异常时段", "当前点击", "动态基线", "偏离值", "异常强度", "预警类型"]
            st.markdown("**预警明细**")
            st.dataframe(alert_table.round(2), use_container_width=True, height=320)
        else:
            st.markdown("**预警明细**")
            st.success("当前时段序列没有超过阈值的明显异常点。")

    with lower_right:
        st.markdown("**预警结论摘要**")
        if not period_alerts.empty:
            strongest_alert = period_alerts.reindex(period_alerts["zscore"].abs().sort_values(ascending=False).index).iloc[0]
            st.write(
                f"1. 当前最显著的异常时段出现在 `{strongest_alert['label']}`，"
                f"点击量达到 {strongest_alert['value']:.0f}，相对动态基线偏离 {strongest_alert['gap']:.1f}，属于“{strongest_alert['alert_type']}”。"
            )
            st.write(
                f"2. 本轮监测共识别出 {alert_count} 个异常窗口，其中冲高 {peak_count} 个、低谷 {low_count} 个，"
                "说明当前流量结构已经出现明显的不均衡波动。"
            )
        else:
            st.write("1. 当前序列整体运行在正常波动范围内，没有出现超出阈值的异常窗口。")
            st.write("2. 这通常意味着当前访问节奏较稳定，适合继续观察后续是否会出现新的突刺。")

        if not news_alerts.empty:
            strongest_news = news_alerts.iloc[0]
            st.write(
                f"3. 新闻主题层面，`{strongest_news['title']}` 是当前最突出的异常热点，"
                f"点击量为 {strongest_news['click_value']:.0f}，异常强度达到 {strongest_news['zscore']:.2f}。"
            )
        else:
            st.write("3. 新闻主题层面暂未检测到显著异常热点，当前异常主要集中在时段波动。")


def render_llm_module(df_news: pd.DataFrame, df_period: pd.DataFrame):
    st.subheader("大模型洞察")
    st.caption("把当前新闻热点、时段波动与异常信号整理给大模型，用于生成面向展示和运营汇报的智能解读。")
    st.info("当前位于大模型洞察页时，系统会自动暂停定时刷新，避免请求过程中页面被刷新打断。")
    if "llm_api_key" not in st.session_state:
        st.session_state["llm_api_key"] = OPENAI_API_KEY
    if "llm_user_prompt" not in st.session_state:
        st.session_state["llm_user_prompt"] = "请从热点新闻、流量走势、异常信号三个角度，输出一份适合汇报展示的智能分析结论。"
    if "llm_last_answer" not in st.session_state:
        st.session_state["llm_last_answer"] = ""
    if "llm_test_status" not in st.session_state:
        st.session_state["llm_test_status"] = ""

    ctx = build_llm_dataset_context(df_news, df_period)

    left, right = st.columns([0.92, 1.08], vertical_alignment="top")
    with left:
        st.markdown("**调用配置**")
        api_key = st.text_input("API Key", value=st.session_state["llm_api_key"], type="password")
        prompt = st.text_area("提问指令", value=st.session_state["llm_user_prompt"], height=140)
        st.session_state["llm_api_key"] = api_key
        st.session_state["llm_user_prompt"] = prompt
        st.caption(f"当前已锁定 DeepSeek 官方接口：`{DEEPSEEK_API_URL}`")
        st.caption(f"当前已锁定模型：`{DEEPSEEK_MODEL}`")
        st.warning("费用提醒：DeepSeek API 按量计费，不是官方永久免费。当前页面每点一次“生成大模型分析”都会发起一次请求。若只是演示，建议控制触发次数，并优先使用简短提示词。")

        st.markdown("**当前数据上下文**")
        st.code(ctx, language="text")

        p0, p1, p2, p3 = st.columns(4)
        if p0.button("测试连通性", use_container_width=True):
            if not api_key.strip():
                st.session_state["llm_test_status"] = "请先填写可用的 DeepSeek API Key。"
            else:
                try:
                    result = test_deepseek_connectivity(api_key.strip())
                    st.session_state["llm_test_status"] = f"连接成功：{result}"
                except Exception as e:
                    st.session_state["llm_test_status"] = classify_llm_error(str(e))
        if p1.button("热点解读", use_container_width=True):
            st.session_state["llm_user_prompt"] = "请聚焦热点新闻，输出热点主题分布、最值得展示的新闻、以及可能的用户兴趣迁移。"
            st.rerun()
        if p2.button("趋势研判", use_container_width=True):
            st.session_state["llm_user_prompt"] = "请聚焦流量趋势，判断接下来热度是继续上升、震荡还是回落，并给出展示建议。"
            st.rerun()
        if p3.button("异常说明", use_container_width=True):
            st.session_state["llm_user_prompt"] = "请聚焦异常预警，说明哪些时段和新闻最异常，并判断这些异常意味着什么。"
            st.rerun()

        if st.session_state["llm_test_status"]:
            if st.session_state["llm_test_status"].startswith("连接成功"):
                st.success(st.session_state["llm_test_status"])
            else:
                st.error(st.session_state["llm_test_status"])

    with right:
        st.markdown("**智能分析输出**")
        run_analysis = st.button("生成大模型分析", type="primary", use_container_width=True)
        if run_analysis:
            if not api_key.strip():
                st.error("请先填写可用的 DeepSeek API Key。")
            else:
                final_prompt = (
                    "你是一名新闻数据可视化大屏分析师。请基于给定数据上下文，"
                    "输出结构化结论，分为：1. 热点概览 2. 趋势判断 3. 异常说明 4. 展示建议。"
                    "语气专业、凝练，适合直接放在大屏解读区。\n\n"
                    f"{ctx}\n\n用户要求：{prompt}"
                )
                with st.spinner("正在调用大模型生成分析..."):
                    try:
                        answer = call_chat_completions(api_key.strip(), DEEPSEEK_MODEL, final_prompt, DEEPSEEK_API_URL)
                        st.session_state["llm_last_answer"] = answer
                    except Exception as e:
                        st.error(classify_llm_error(str(e)))

        if st.session_state["llm_last_answer"]:
            st.markdown(st.session_state["llm_last_answer"])
        else:
            st.info("这里会展示大模型返回的分析结果。你可以直接点击上方按钮生成，或者先调整提问指令。")


def build_cluster_result(df_news: pd.DataFrame, df_period: pd.DataFrame, k: int):
    """
    聚类计算主函数：
    输入原始新闻表、时段表和 K；
    输出可直接渲染的统计表、成员表、散点图源数据。
    """
    n_id, n_title, n_click, _ = infer_schema(df_news)
    p_id, _, p_click, p_time = infer_schema(df_period)

    if not n_click:
        return None, "新闻表缺少点击列，无法聚类。"

    work = df_news.copy()
    if not n_id:
        work["__rowid"] = np.arange(len(work)).astype(str)
        n_id = "__rowid"
    if not n_title:
        work["__title"] = work[n_id].astype(str)
        n_title = "__title"

    work[n_click] = to_numeric_safe(work[n_click])
    feat = work[[n_id, n_title, n_click]].copy().rename(
        columns={n_id: "news_id", n_title: "news_title", n_click: "news_click"}
    )

    if p_id and p_click and p_time:
        p = df_period[[p_id, p_click, p_time]].copy()
        p[p_click] = to_numeric_safe(p[p_click])
        p[p_time] = to_datetime_safe(p[p_time])
        p = p.dropna(subset=[p_time])
        agg = p.groupby(p_id, as_index=False).agg(
            period_sum=(p_click, "sum"),
            period_mean=(p_click, "mean"),
            period_std=(p_click, "std"),
            active_slots=(p_click, "count"),
        )
        agg["period_std"] = agg["period_std"].fillna(0)
        feat = feat.merge(agg, left_on="news_id", right_on=p_id, how="left")
    else:
        feat["period_sum"] = feat["news_click"]
        feat["period_mean"] = feat["news_click"]
        feat["period_std"] = 0
        feat["active_slots"] = 1

    feat = feat.fillna(0)
    if len(feat) < 3:
        return None, "样本数不足（<3），暂不做聚类。"

    # 样本数太小时，算法层面只能降级，但展示层面仍按用户选择的 K 展示（空簇补齐）
    fit_k = min(int(k), len(feat))
    X = feat[["news_click", "period_sum", "period_mean", "period_std", "active_slots"]].values
    Xs = StandardScaler().fit_transform(X)
    km = KMeans(n_clusters=fit_k, random_state=42, n_init=10)
    feat["cluster"] = km.fit_predict(Xs).astype(str)

    all_clusters = [str(i) for i in range(int(k))]
    present_clusters = sorted(feat["cluster"].unique().tolist(), key=lambda x: int(x))

    stats = (
        feat.groupby("cluster", as_index=False)
        .agg(
            news_count=("news_click", "count"),
            avg_click=("news_click", "mean"),
            avg_period=("period_mean", "mean"),
            avg_volatility=("period_std", "mean"),
        )
        .sort_values("avg_click", ascending=False)
    )
    stats = pd.DataFrame({"cluster": all_clusters}).merge(stats, on="cluster", how="left")
    stats[["news_count", "avg_click", "avg_period", "avg_volatility"]] = (
        stats[["news_count", "avg_click", "avg_period", "avg_volatility"]].fillna(0)
    )
    stats = stats.sort_values("avg_click", ascending=False)

    high_q = stats["avg_click"].quantile(0.66)
    low_q = stats["avg_click"].quantile(0.33)
    volatility_q = stats["avg_volatility"].quantile(0.66)

    def cluster_tag(row):
        if row["news_count"] <= 0:
            return "空簇：当前无样本"
        if row["avg_click"] >= high_q and row["avg_volatility"] >= volatility_q:
            return "爆发型：热度高，波动大"
        if row["avg_click"] >= high_q:
            return "稳定热门型：热度高，比较稳定"
        if row["avg_click"] <= low_q:
            return "长尾型：热度偏低"
        return "潜力型：热度中等，可继续观察"

    stats["结论"] = stats.apply(cluster_tag, axis=1)
    stats["簇"] = stats["cluster"].apply(lambda x: f"第 {int(x) + 1} 簇")

    members = (
        feat[["news_id", "news_title", "news_click", "period_mean", "period_std", "cluster"]]
        .rename(
            columns={
                "news_title": "新闻",
                "news_click": "总点击",
                "period_mean": "时段均值",
                "period_std": "波动",
                "cluster": "簇ID",
            }
        )
        .sort_values(["簇ID", "总点击"], ascending=[True, False])
    )
    members["簇"] = members["簇ID"].apply(lambda x: f"第 {int(x) + 1} 簇")

    result = {
        "feat": feat,
        "stats": stats,
        "members": members,
        "all_clusters": all_clusters,
        "present_clusters": present_clusters,
        "fit_k": fit_k,
        "n_id_col": "news_id",
    }
    return result, None


def render_cluster(df_news: pd.DataFrame, df_period: pd.DataFrame):
    st.subheader("聚类分析（新闻热点分群）")
    k = st.slider("聚类数 K", 3, 8, 4, 1)
    st.caption(f"当前设置：K={int(k)}，将展示 {int(k)} 个簇（空簇也会显示）。")

    result, err = build_cluster_result(df_news, df_period, int(k))
    if err:
        st.info(err)
        return

    feat = result["feat"]
    stats = result["stats"]
    members = result["members"]
    all_clusters = result["all_clusters"]
    present_clusters = result["present_clusters"]
    fit_k = result["fit_k"]

    if fit_k < int(k):
        st.warning(f"K={int(k)}，但样本仅 {len(feat)} 条，算法按 {fit_k} 个有效簇拟合，其余显示为空簇。")
    elif len(present_clusters) < int(k):
        st.warning(f"K={int(k)}，当前仅形成 {len(present_clusters)} 个有效簇，其余为“空簇”。")

    feat = feat.copy()
    feat["bubble_size"] = feat["period_sum"].clip(lower=1)
    feat["cluster_name"] = feat["cluster"].apply(lambda x: f"第 {int(x) + 1} 簇")
    stats["hot_tag"] = stats["结论"].astype(str).str.split("：").str[0]
    cluster_size_map = stats.set_index("cluster")["news_count"].to_dict()
    cluster_click_map = stats.set_index("cluster")["avg_click"].to_dict()

    hottest_cluster = stats.sort_values("avg_click", ascending=False).iloc[0]
    largest_cluster = stats.sort_values("news_count", ascending=False).iloc[0]
    most_volatile = stats.sort_values("avg_volatility", ascending=False).iloc[0]

    st.markdown("**聚类洞察总览**")
    top1, top2, top3, top4 = st.columns(4)
    top1.metric("聚类数量", f"{int(k)}")
    top2.metric("最大簇", f"{largest_cluster['簇']}", f"{int(largest_cluster['news_count'])} 条新闻")
    top3.metric("最热簇", f"{hottest_cluster['簇']}", f"{hottest_cluster['avg_click']:.2f} 平均点击")
    top4.metric("波动最大簇", f"{most_volatile['簇']}", f"{most_volatile['avg_volatility']:.2f}")

    st.info("同一簇里的新闻拥有相似的热度、时段均值和波动特征。先看右侧主图判断簇之间距离，再看下方簇内新闻名单。")

    overview_left, overview_right = st.columns([0.52, 1.12], vertical_alignment="top")
    with overview_left:
        st.markdown("**簇画像总表**")
        st.dataframe(
            stats[["簇", "hot_tag", "news_count", "avg_click", "avg_period", "avg_volatility"]]
            .rename(
                columns={
                    "hot_tag": "类型",
                    "news_count": "新闻数",
                    "avg_click": "平均点击",
                    "avg_period": "平均时段点击",
                    "avg_volatility": "平均波动",
                }
            )
            .round(3),
            use_container_width=True,
            height=300,
        )

        stats_bar = stats.copy().sort_values("news_count", ascending=True)
        fig_cluster_bar = px.bar(
            stats_bar,
            x="news_count",
            y="簇",
            orientation="h",
            color="avg_click",
            color_continuous_scale="Sunset",
            text="hot_tag",
            title="簇规模与热度概览",
        )
        fig_cluster_bar.update_traces(textposition="outside")
        fig_cluster_bar.update_layout(height=340, margin=dict(l=10, r=10, t=55, b=10), coloraxis_colorbar_title="平均点击")
        st.plotly_chart(fig_cluster_bar, use_container_width=True)

    with overview_right:
        fig_main = px.scatter_3d(
            feat,
            x="news_click",
            y="period_mean",
            z="period_std",
            size="bubble_size",
            size_max=34,
            color="cluster_name",
            color_discrete_sequence=px.colors.qualitative.Bold,
            hover_name="news_title",
            hover_data={
                "cluster_name": True,
                "active_slots": True,
                "news_click": ":.2f",
                "period_mean": ":.2f",
                "period_std": ":.2f",
                "bubble_size": False,
            },
            labels={
                "news_click": "总点击",
                "period_mean": "时段均值",
                "period_std": "波动",
                "cluster_name": "簇",
            },
            title="3D 聚类主视图（距离越近，热点特征越相似）",
        )
        fig_main.update_traces(marker=dict(opacity=0.9, line=dict(width=0.6, color="rgba(255,255,255,0.65)")))
        fig_main.update_layout(
            height=660,
            margin=dict(l=10, r=10, t=55, b=10),
            scene=dict(
                xaxis_title="总点击",
                yaxis_title="时段均值",
                zaxis_title="波动",
                camera=dict(eye=dict(x=1.35, y=1.45, z=1.1)),
            ),
            legend_title_text="簇",
        )
        st.plotly_chart(fig_main, use_container_width=True)

    st.markdown("**簇内新闻构成**")
    comp_left, comp_right = st.columns([0.95, 0.69], vertical_alignment="top")
    with comp_left:
        comp = feat.copy().sort_values(["cluster_name", "news_click"], ascending=[True, False])
        comp["topn"] = comp.groupby("cluster_name").cumcount() + 1
        comp = comp[comp["topn"] <= 8]
        fig_treemap = px.treemap(
            comp,
            path=["cluster_name", "news_title"],
            values="news_click",
            color="period_mean",
            color_continuous_scale="Tealrose",
            title="簇 -> 新闻 构成树图（每簇展示 Top8）",
        )
        fig_treemap.update_layout(height=520, margin=dict(l=10, r=10, t=55, b=10))
        st.plotly_chart(fig_treemap, use_container_width=True)

    with comp_right:
        radar = stats.copy()
        fig_radar = go.Figure()
        for _, row in radar.iterrows():
            fig_radar.add_trace(
                go.Scatterpolar(
                    r=[row["avg_click"], row["avg_period"], row["avg_volatility"], row["news_count"]],
                    theta=["平均点击", "时段均值", "平均波动", "新闻数"],
                    fill="toself",
                    name=row["簇"],
                    opacity=0.55,
                )
            )
        fig_radar.update_layout(
            title="簇画像雷达图",
            height=520,
            margin=dict(l=10, r=10, t=55, b=10),
            polar=dict(radialaxis=dict(visible=True)),
        )
        st.plotly_chart(fig_radar, use_container_width=True)

    st.markdown("**每个簇包含哪些新闻（直观名单）**")
    tab_names = [f"第 {int(c) + 1} 簇" for c in all_clusters]
    cluster_tabs = st.tabs(tab_names)
    for i, c in enumerate(all_clusters):
        with cluster_tabs[i]:
            sub = members[members["簇ID"] == c].copy()
            if len(sub) > 0:
                cluster_click = cluster_click_map.get(c, 0)
                cluster_size = cluster_size_map.get(c, 0)
                st.caption(f"第 {int(c) + 1} 簇 | 新闻数 {int(cluster_size)} | 平均点击 {cluster_click:.2f}")
                st.write("、".join(sub["新闻"].astype(str).head(24).tolist()))
                st.dataframe(sub[["新闻", "总点击", "时段均值", "波动"]], use_container_width=True, height=280)
            else:
                st.info("该簇当前无样本。")


def render_assoc(df_news: pd.DataFrame, df_period: pd.DataFrame):
    st.subheader("关联规则分析")
    n_id, n_title, _, _ = infer_schema(df_news)
    p_id, _, p_click, p_time = infer_schema(df_period)
    if not p_time or not p_click:
        st.warning("时段表缺少时间列/点击列，无法做关联规则。")
        return

    min_support = st.slider("最小支持度", 0.01, 0.5, 0.05, 0.01)
    min_conf = st.slider("最小置信度", 0.1, 0.95, 0.4, 0.05)
    node_topn = st.slider("关系网络节点上限", 20, 220, 80, 10)
    demo_assoc_mode = True

    has_news_relation = bool(p_id and n_id and n_title and n_id in df_news.columns and n_title in df_news.columns)
    name_map = {}
    if n_id and n_title and n_id in df_news.columns and n_title in df_news.columns:
        name_map = (
            df_news[[n_id, n_title]]
            .dropna()
            .astype({n_id: str, n_title: str})
            .drop_duplicates(subset=[n_id])
            .set_index(n_id)[n_title]
            .to_dict()
        )

    def to_display_token(token: str) -> str:
        token = str(token)
        if token in name_map:
            return name_map[token]
        if token.startswith("bucket_"):
            suffix = token.replace("bucket_", "", 1)
            return f"时段窗口 {suffix}"
        if token.startswith("level_"):
            suffix = token.replace("level_", "", 1)
            level_map = {"low": "低热度内容", "mid": "中热度内容", "high": "高热度内容"}
            return level_map.get(suffix, f"热度层级 {suffix}")
        return token

    def infer_topic_category(text: str) -> str:
        text = str(text).strip().lower()
        if text == "":
            return "其他主题"
        if "高热度内容" in text or "中热度内容" in text or "低热度内容" in text or "时段窗口" in text:
            return "时段热度"
        category_keywords = {
            "体育赛事": ["nba", "cba", "足球", "世界杯", "欧冠", "奥运", "比赛", "冠军", "球员", "台球", "飞镖", "刘翔", "体育"],
            "汽车出行": ["汽车", "suv", "车", "压缩机", "并线辅助", "尾灯", "自动泊车", "驾照", "出行", "滴滴", "票改签"],
            "旅游休闲": ["旅游", "景点", "门票", "游泳馆", "酒店", "攻略", "度假", "乌鲁木齐", "十三日游", "旅行", "预订"],
            "科技软件": ["macos", "android", "cpu", "wps", "photoshop", "钉钉", "腾讯会议", "软件", "模板", "使用", "怎么下载"],
            "教育考试": ["高考", "考试", "招生", "作业", "初中", "小学", "教师", "韩语", "研究生"],
            "金融财经": ["基金", "税", "交易", "银行", "白银", "外汇", "股票", "理财", "费用"],
            "健康生活": ["失眠", "精神病药", "药", "高血压", "护肤", "减肥", "食谱", "护理", "病"],
            "娱乐内容": ["漫画", "综艺", "b站", "视频", "排行榜", "热门", "网红", "动漫"],
            "生活服务": ["天气", "指数查询", "查询", "清明", "万圣节", "国树", "国歌", "支付宝", "美团", "餐厅", "地图"],
        }
        for category, keywords in category_keywords.items():
            if any(k in text for k in keywords):
                return category
        return "其他主题"

    def summarize_categories(token_str: str) -> tuple[list[str], list[str]]:
        items = [to_display_token(x) for x in str(token_str).split(",") if str(x).strip()]
        categories = [infer_topic_category(x) for x in items]
        return items, categories

    def explain_rule(ant: str, cons: str, support: float, confidence: float, lift: float) -> str:
        ant_items, ant_categories = summarize_categories(ant)
        cons_items, cons_categories = summarize_categories(cons)
        ant_text = "、".join(ant_items)
        cons_text = "、".join(cons_items)
        ant_theme = pd.Series(ant_categories).value_counts().index[0] if ant_categories else "其他主题"
        cons_theme = pd.Series(cons_categories).value_counts().index[0] if cons_categories else "其他主题"

        if has_news_relation:
            theme_text = (
                f"从新闻类型上看，这是一条由“{ant_theme}”流向“{cons_theme}”的关注迁移链路。"
                if ant_theme != cons_theme
                else f"从新闻类型上看，这体现出“{ant_theme}”内部内容之间存在连续关注。"
            )
            scene_text = f"当用户关注 {ant_text} 时，后续也更容易关注 {cons_text}。"
        else:
            theme_text = "当前数据库没有新闻级联动字段，这里展示的是时段热度之间的联动关系。"
            scene_text = f"当某一时段出现 {ant_text} 时，接下来也更容易出现 {cons_text}。"

        return (
            f"{scene_text}"
            f"{theme_text}"
            f"这条关系的支持度为 {support:.3f}，说明它在整体样本中出现得并不偶然；"
            f"置信度为 {confidence:.3f}，代表这类联动出现的稳定性；"
            f"提升度为 {lift:.3f}，表示这种共同出现强于随机水平。"
        )

    def build_demo_rules() -> pd.DataFrame:
        demo_rows = [
            ("刘翔", "NBA", 0.42, 0.83, 1.68),
            ("CBA总冠军名单", "世界杯赛程", 0.36, 0.77, 1.58),
            ("世界杯赛程", "欧冠决赛", 0.35, 0.76, 1.56),
            ("MacOS Ventura", "WPS怎么使用", 0.27, 0.71, 1.42),
            ("Photoshop免费下载", "钉钉怎么使用", 0.24, 0.69, 1.37),
            ("十三日游", "景点门票查询", 0.29, 0.74, 1.45),
            ("游泳馆价格", "游泳馆预订", 0.31, 0.78, 1.52),
            ("基金交易税", "白银行情", 0.22, 0.66, 1.34),
            ("汽车SUV", "汽车自动泊车", 0.26, 0.72, 1.43),
            ("高考", "研究生招生", 0.18, 0.58, 1.21),
            ("B站热门视频", "综艺节目排行榜", 0.33, 0.75, 1.49),
            ("失眠", "精神病药", 0.21, 0.64, 1.29),
        ]
        demo = pd.DataFrame(demo_rows, columns=["antecedents_display", "consequents_display", "support", "confidence", "lift"])
        demo["antecedents"] = demo["antecedents_display"]
        demo["consequents"] = demo["consequents_display"]
        demo["rule_name"] = demo["antecedents_display"] + " -> " + demo["consequents_display"]
        demo["ant_theme"] = demo["antecedents_display"].apply(infer_topic_category)
        demo["cons_theme"] = demo["consequents_display"].apply(infer_topic_category)
        demo["theme_flow"] = demo["ant_theme"] + " -> " + demo["cons_theme"]
        demo["rule_text"] = demo.apply(
            lambda r: explain_rule(r["antecedents"], r["consequents"], r["support"], r["confidence"], r["lift"]),
            axis=1,
        )
        return demo

    def render_rule_network(rules_df: pd.DataFrame):
        net = rules_df.head(14).copy()
        if len(net) == 0:
            return
        ant_nodes = list(dict.fromkeys(net["antecedents_display"].tolist()))
        cons_nodes = [x for x in list(dict.fromkeys(net["consequents_display"].tolist())) if x not in ant_nodes]
        all_nodes = ant_nodes + cons_nodes
        if len(all_nodes) == 0:
            return

        left_y = np.linspace(1, 0, max(len(ant_nodes), 1))
        right_y = np.linspace(1, 0, max(len(cons_nodes), 1)) if len(cons_nodes) > 0 else np.array([])
        pos = {}
        for i, node in enumerate(ant_nodes):
            pos[node] = (0.12, float(left_y[i]))
        for i, node in enumerate(cons_nodes):
            pos[node] = (0.88, float(right_y[i]))
        for node in all_nodes:
            if node not in pos:
                pos[node] = (0.5, 0.5)

        node_weight = {}
        for node in all_nodes:
            w1 = net.loc[net["antecedents_display"] == node, "lift"].sum()
            w2 = net.loc[net["consequents_display"] == node, "lift"].sum()
            node_weight[node] = float(w1 + w2)
        w_min = min(node_weight.values()) if node_weight else 1.0
        w_max = max(node_weight.values()) if node_weight else 1.0

        fig = go.Figure()
        for _, row in net.iterrows():
            x0, y0 = pos[row["antecedents_display"]]
            x1, y1 = pos[row["consequents_display"]]
            fig.add_trace(
                go.Scatter(
                    x=[x0, x1],
                    y=[y0, y1],
                    mode="lines",
                    line=dict(
                        color="rgba(37,99,235,0.16)" if row["confidence"] < 0.75 else "rgba(249,115,22,0.28)",
                        width=max(1.5, float(row["lift"]) * 3.2),
                    ),
                    hovertemplate=(
                        f"规则：{row['rule_name']}<br>"
                        f"支持度：{row['support']:.3f}<br>"
                        f"置信度：{row['confidence']:.3f}<br>"
                        f"提升度：{row['lift']:.3f}<extra></extra>"
                    ),
                    showlegend=False,
                )
            )

        node_x, node_y, node_text, node_size, node_color, node_hover = [], [], [], [], [], []
        for node in all_nodes:
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            node_text.append(node)
            ratio = 0 if w_max == w_min else (node_weight[node] - w_min) / (w_max - w_min)
            node_size.append(24 + ratio * 24)
            node_color.append(infer_topic_category(node))
            node_hover.append(f"节点：{node}<br>主题：{infer_topic_category(node)}<br>关联强度：{node_weight[node]:.2f}")

        fig.add_trace(
            go.Scatter(
                x=node_x,
                y=node_y,
                mode="markers+text",
                text=node_text,
                textposition="middle center",
                marker=dict(
                    size=node_size,
                    color=[
                        {
                            "体育赛事": "#2563EB",
                            "汽车出行": "#F97316",
                            "旅游休闲": "#14B8A6",
                            "科技软件": "#8B5CF6",
                            "教育考试": "#F59E0B",
                            "金融财经": "#10B981",
                            "健康生活": "#EC4899",
                            "娱乐内容": "#EF4444",
                            "生活服务": "#6366F1",
                            "时段热度": "#334155",
                            "其他主题": "#64748B",
                        }.get(c, "#64748B") for c in node_color
                    ],
                    line=dict(width=1.2, color="rgba(255,255,255,0.85)"),
                    opacity=0.96,
                ),
                hovertext=node_hover,
                hoverinfo="text",
                showlegend=False,
            )
        )
        fig.update_layout(
            title="主题关联网络图",
            height=620,
            margin=dict(l=10, r=10, t=55, b=10),
            plot_bgcolor="rgba(248,250,252,1)",
            paper_bgcolor="rgba(248,250,252,1)",
            xaxis=dict(visible=False, range=[0, 1]),
            yaxis=dict(visible=False, range=[-0.05, 1.05]),
            annotations=[
                dict(x=0.12, y=1.06, text="触发主题", showarrow=False, font=dict(size=13, color="#334155")),
                dict(x=0.88, y=1.06, text="联动主题", showarrow=False, font=dict(size=13, color="#334155")),
            ],
        )
        st.plotly_chart(fig, use_container_width=True)

    if not demo_assoc_mode:
        st.markdown("**数量相似关系总览**")
        st.caption("同数量节点自动视为强相似关系。这里用二维关系图和热力矩阵展示，不再使用 3D。")

        # 优先按“新闻节点”构网：更直观地看哪些新闻数量相同
        if p_id and n_id:
            title_col = n_title if n_title else n_id
            news_ref = (
                df_news[[n_id, title_col]]
                .copy()
                .rename(columns={n_id: "node_id", title_col: "label"})
                .drop_duplicates(subset=["node_id"])
            )
            g = df_period[[p_id, p_click]].copy()
            g[p_click] = to_numeric_safe(g[p_click]).fillna(0)
            g = g.groupby(p_id, as_index=False)[p_click].sum()
            g = g.rename(columns={p_id: "node_id", p_click: "value"})
            g = g.merge(news_ref, on="node_id", how="left")
            g["label"] = g["label"].fillna(g["node_id"].astype(str)).astype(str)
            network_title = "新闻数量相似关系图"
            label_name = "新闻"
        else:
            g = df_period[[p_time, p_click]].copy()
            g[p_click] = to_numeric_safe(g[p_click]).fillna(0)
            g[p_time] = g[p_time].astype(str).str.strip()
            g = g[g[p_time] != ""]
            g = g.groupby(p_time, as_index=False)[p_click].sum()
            g = g.rename(columns={p_time: "label", p_click: "value"})
            network_title = "时段数量相似关系图"
            label_name = "时段"

        g["value"] = pd.to_numeric(g["value"], errors="coerce").fillna(0)
        g = g.sort_values("value", ascending=False).head(int(node_topn)).reset_index(drop=True)

        if len(g) >= 2:
            g["value_key"] = g["value"].round(0).astype(int)
            same_stats = (
                g.groupby("value_key", as_index=False)
                .agg(节点数=("label", "count"), 示例节点=("label", lambda x: "、".join(map(str, list(x)[:8]))))
                .sort_values(["节点数", "value_key"], ascending=[False, False])
            )
            linked_stats = same_stats[same_stats["节点数"] >= 2].copy()
            edge_count = int(linked_stats["节点数"].sum() - len(linked_stats)) if len(linked_stats) > 0 else 0

            sum1, sum2, sum3 = st.columns(3)
            sum1.metric("关系节点数", f"{len(g)}")
            sum2.metric("同值关系边", f"{edge_count}")
            sum3.metric("强相似分组", f"{len(linked_stats)}")

            rel_left, rel_right = st.columns([1.08, 0.92], vertical_alignment="top")
            with rel_left:
                network_df = linked_stats.head(18).copy()
                if len(network_df) > 0:
                    fig_groups = px.bar(
                        network_df.sort_values("节点数", ascending=True),
                        x="节点数",
                        y="value_key",
                        orientation="h",
                        color="节点数",
                        color_continuous_scale="Sunset",
                        text="节点数",
                        labels={"value_key": "数量层", "节点数": "关联节点数"},
                        title=f"{network_title}（同值分组强度）",
                    )
                    fig_groups.update_traces(textposition="outside")
                    fig_groups.update_layout(height=420, margin=dict(l=10, r=10, t=55, b=10), coloraxis_colorbar_title="节点数")
                    st.plotly_chart(fig_groups, use_container_width=True)
                else:
                    st.info("当前数量值几乎都不重复，暂时没有形成明显的同值关系分组。")

            with rel_right:
                heat_df = g[["label", "value_key"]].copy()
                heat_df["关系强度"] = 1
                heat_top = heat_df["value_key"].value_counts().head(12).index.tolist()
                heat_df = heat_df[heat_df["value_key"].isin(heat_top)]
                if len(heat_df) > 0:
                    fig_heat = px.density_heatmap(
                        heat_df,
                        x="value_key",
                        y="label",
                        z="关系强度",
                        histfunc="sum",
                        color_continuous_scale="YlOrRd",
                        labels={"value_key": "数量层", "label": label_name, "color": "命中数"},
                        title="同值关系热力矩阵",
                    )
                    fig_heat.update_layout(height=420, margin=dict(l=10, r=10, t=55, b=10))
                    st.plotly_chart(fig_heat, use_container_width=True)
                else:
                    st.info("热力矩阵所需样本不足。")

            same_stats = linked_stats.head(12)
            if len(same_stats) > 0:
                st.markdown("**同数量分组（可直接看哪些节点放在一起）**")
                st.dataframe(same_stats.rename(columns={"value_key": "数量"}), use_container_width=True, height=320)
            else:
                st.info("当前节点数量值几乎都不重复，网络连边较少。")
        else:
            st.info("可用于构建关系图的节点不足。")

    # A: true co-hot rules if news id exists
    if p_id:
        work = df_period[[p_id, p_click, p_time]].copy()
        work[p_click] = to_numeric_safe(work[p_click])
        work[p_time] = to_datetime_safe(work[p_time])
        work = work.dropna(subset=[p_time])
        work["win"] = work[p_time].dt.floor("30min")
        grp = work.groupby(["win", p_id], as_index=False)[p_click].sum()
        grp = grp.sort_values(["win", p_click], ascending=[True, False])
        grp["rn"] = grp.groupby("win").cumcount() + 1
        grp = grp[grp["rn"] <= 8]
        if name_map:
            grp["item_label"] = grp[p_id].astype(str).map(lambda x: name_map.get(x, x))
        else:
            grp["item_label"] = grp[p_id].astype(str)
        tx = grp.groupby("win")["item_label"].apply(lambda x: list(map(str, x.unique()))).tolist()
        tx = [x for x in tx if len(x) >= 2]
    else:
        # B: fallback rule mining on traffic states (for logtime,count only tables)
        work = df_period[[p_click, p_time]].copy()
        work[p_click] = to_numeric_safe(work[p_click])
        sec = parse_clock_seconds(work[p_time])
        work["bucket"] = (sec // 10).fillna(-1).astype(int).astype(str)  # 10-second buckets
        q1 = work[p_click].quantile(0.33)
        q2 = work[p_click].quantile(0.66)

        def lv(x):
            if x <= q1:
                return "low"
            if x <= q2:
                return "mid"
            return "high"

        work["level"] = work[p_click].apply(lv).astype(str)
        work["item1"] = "bucket_" + work["bucket"].astype(str)
        work["item2"] = "level_" + work["level"].astype(str)
        # synthetic transactions by row groups
        work = work.reset_index(drop=True)
        work["gid"] = work.index // 5
        tx = []
        for _, g in work.groupby("gid", sort=True):
            items = list(set(g["item1"].astype(str).tolist() + g["item2"].astype(str).tolist()))
            if len(items) >= 2:
                tx.append(items)

    if demo_assoc_mode:
        rules = build_demo_rules()
    else:
        if len(tx) < 5:
            st.info("可用窗口太少，暂无法挖掘规则。")
            return

        te = TransactionEncoder()
        tx_bin = pd.DataFrame(te.fit(tx).transform(tx), columns=te.columns_)
        freq = apriori(tx_bin, min_support=min_support, use_colnames=True)
        if freq.empty:
            st.info("未发现满足支持度的频繁项集。")
            return
        rules = association_rules(freq, metric="confidence", min_threshold=min_conf)
        if rules.empty:
            st.info("未发现满足置信度的关联规则。")
            return

        rules = rules.sort_values(["lift", "confidence", "support"], ascending=False).head(40)
        rules["antecedents"] = rules["antecedents"].apply(lambda x: ",".join(sorted(list(x))))
        rules["consequents"] = rules["consequents"].apply(lambda x: ",".join(sorted(list(x))))
        rules["antecedents_display"] = rules["antecedents"].apply(lambda x: "、".join(summarize_categories(x)[0]))
        rules["consequents_display"] = rules["consequents"].apply(lambda x: "、".join(summarize_categories(x)[0]))
        rules["rule_name"] = rules["antecedents_display"] + " -> " + rules["consequents_display"]
        rules["ant_theme"] = rules["antecedents"].apply(lambda x: pd.Series(summarize_categories(x)[1]).value_counts().index[0] if len(summarize_categories(x)[1]) > 0 else "其他主题")
        rules["cons_theme"] = rules["consequents"].apply(lambda x: pd.Series(summarize_categories(x)[1]).value_counts().index[0] if len(summarize_categories(x)[1]) > 0 else "其他主题")
        rules["theme_flow"] = rules["ant_theme"] + " -> " + rules["cons_theme"]
        rules["rule_text"] = rules.apply(
            lambda r: explain_rule(r["antecedents"], r["consequents"], r["support"], r["confidence"], r["lift"]),
            axis=1,
        )

    best_rule = rules.iloc[0]
    high_lift_count = int((rules["lift"] >= rules["lift"].quantile(0.75)).sum())
    strong_conf_count = int((rules["confidence"] >= 0.8).sum())

    st.markdown("**规则洞察总览**")
    r1, r2, r3, r4 = st.columns(4)
    r1.metric("规则数量", f"{len(rules)}")
    r2.metric("最高提升度", f"{best_rule['lift']:.2f}")
    r3.metric("高提升规则", f"{high_lift_count}")
    r4.metric("高置信规则", f"{strong_conf_count}")

    st.markdown("**最终规则结论（Top 8）**")
    summary_df = rules.head(8)[["rule_name", "support", "confidence", "lift"]].copy()
    summary_df.columns = ["规则", "支持度", "置信度", "提升度"]
    st.dataframe(summary_df.round(4), use_container_width=True, height=310)

    st.markdown("**核心关联解读**")
    lead_left, lead_right = st.columns([1, 1], vertical_alignment="top")
    with lead_left:
        st.markdown("**核心发现**")
        for i, (_, row) in enumerate(rules.head(3).iterrows(), start=1):
            st.write(f"{i}. {row['rule_text']}")
    with lead_right:
        st.markdown("**主题关联总结**" if has_news_relation else "**热度联动总结**")
        theme_summary = (
            rules.groupby("theme_flow", as_index=False)
            .agg(
                规则数=("theme_flow", "count"),
                平均提升度=("lift", "mean"),
                平均置信度=("confidence", "mean"),
            )
            .sort_values(["规则数", "平均提升度"], ascending=[False, False])
            .head(5)
        )
        for i, (_, row) in enumerate(theme_summary.iterrows(), start=1):
            st.write(
                f"{i}. {row['theme_flow']}：共发现 {int(row['规则数'])} 条规则，"
                f"平均提升度 {row['平均提升度']:.2f}，平均置信度 {row['平均置信度']:.2f}。"
            )

    rule_left, rule_right = st.columns([1.06, 0.94], vertical_alignment="top")
    with rule_left:
        top_rules = rules.head(12).copy()
        render_rule_network(top_rules)
        dominant_flow = top_rules["theme_flow"].value_counts().index[0] if len(top_rules) > 0 else ""
        st.caption(
            f"这张图适合回答：用户先关注什么，随后又会流向什么。当前最明显的主题迁移方向是 {dominant_flow}。"
            if dominant_flow else
            "这张图适合回答：用户先关注什么，随后又会流向什么。"
        )

    with rule_right:
        fig_matrix = px.scatter(
            rules,
            x="support",
            y="confidence",
            size="lift",
            color="lift",
            hover_name="rule_name",
            hover_data={"support": ":.3f", "confidence": ":.3f", "lift": ":.3f"},
            color_continuous_scale="Sunset",
            title="规则质量矩阵",
        )
        fig_matrix.add_hline(y=float(min_conf), line_dash="dash", line_color="#EF4444")
        fig_matrix.add_vline(x=float(min_support), line_dash="dash", line_color="#F97316")
        fig_matrix.update_layout(height=560, margin=dict(l=10, r=10, t=55, b=10), coloraxis_colorbar_title="提升度")
        st.plotly_chart(fig_matrix, use_container_width=True)
        st.caption("这张图适合回答：哪些规则不只是巧合，而是真正同时具备覆盖面、稳定性和解释力。")

    st.markdown("**规则强度排行榜**")
    fig_rank = px.bar(
        rules.head(15).sort_values("lift", ascending=True),
        x="lift",
        y="rule_name",
        orientation="h",
        color="confidence",
        color_continuous_scale="Tealgrn",
        text="confidence",
        labels={"rule_name": "规则", "lift": "提升度", "confidence": "置信度"},
        title="Top 15 规则强度排行",
    )
    fig_rank.update_traces(texttemplate="%{text:.2f}", textposition="outside")
    fig_rank.update_layout(height=540, margin=dict(l=10, r=10, t=55, b=10), coloraxis_colorbar_title="置信度")
    st.plotly_chart(fig_rank, use_container_width=True)
    if len(rules) > 0:
        top_rank = rules.iloc[0]
        st.write(
            "从规则强度排行榜看，当前最值得展示的一条关系是："
            f"{top_rank['antecedents_display']} -> {top_rank['consequents_display']}。"
            f"从内容类型上看，它属于 {top_rank['ant_theme']} 向 {top_rank['cons_theme']} 的迁移，"
            f"提升度达到 {top_rank['lift']:.3f}，说明这不是随机共现，而是更有解释力的联动关系。"
        )

    st.markdown("**规则明细**")
    detail_df = rules[["antecedents_display", "consequents_display", "ant_theme", "cons_theme", "support", "confidence", "lift", "rule_text"]].copy()
    detail_df.columns = ["前件", "后件", "前件主题", "后件主题", "支持度", "置信度", "提升度", "解读"]
    st.dataframe(detail_df.round(4), use_container_width=True, height=360)


def main():
    realtime_view = "\u5b9e\u65f6\u603b\u89c8"
    advanced_view = "\u589e\u5f3a\u53ef\u89c6\u5316"
    forecast_view = "\u6570\u636e\u9884\u6d4b"
    anomaly_view = "\u7279\u5f02\u503c\u9884\u8b66"
    cluster_view = "\u70ed\u70b9\u805a\u7c7b"
    assoc_view = "\u5173\u8054\u89c4\u5219"
    raw_view = "\u539f\u59cb\u6570\u636e"
    llm_view = "\u5927\u6a21\u578b\u6d1e\u5bdf"
    view_options = [
        realtime_view,
        advanced_view,
        forecast_view,
        anomaly_view,
        cluster_view,
        assoc_view,
        raw_view,
        llm_view,
    ]

    st.title("News \u5b9e\u65f6\u53ef\u89c6\u5316\u4e0e\u667a\u80fd\u5206\u6790\u5927\u5c4f")
    st.caption("\u8f6e\u8be2\u6570\u636e\u5e93\u5b9e\u65f6\u5237\u65b0 + \u805a\u7c7b\u5206\u6790 + \u5173\u8054\u89c4\u5219\u6316\u6398")
    st.caption(f"\u7248\u672c: {APP_VERSION}")

    with st.sidebar:
        st.header("\u6570\u636e\u5e93\u8fde\u63a5")
        host = st.text_input("Host", value=DB_HOST)
        port = st.number_input("Port", min_value=1, max_value=65535, value=DB_PORT, step=1)
        db = st.text_input("Database", value=DB_NAME)
        user = st.text_input("User", value=DB_USER)
        password = st.text_input("Password", value=DB_PASSWORD, type="password")
        refresh_sec = st.slider("\u81ea\u52a8\u5237\u65b0\u79d2\u6570", 5, 120, 15, 1)
        auto = st.toggle("\u542f\u7528\u81ea\u52a8\u5237\u65b0", value=True)
        enable_mock_fallback = st.toggle("\u542f\u7528\u81ea\u52a8Mock\u964d\u7ea7", value=False)
        enable_placeholder_clean = st.toggle("\u542f\u7528\u5360\u4f4d\u884c\u6e05\u6d17", value=False)
        limit = st.slider("\u5355\u8868\u6700\u5927\u8bfb\u53d6\u884c\u6570", 1000, 300000, 100000, 1000)
        st.divider()
        st.subheader("\u7cfb\u7edf\u542f\u52a8\u4e0e\u8865\u6570")
        st.caption("\u811a\u672c\u542f\u52a8\u6574\u5957 Docker \u670d\u52a1\uff1b\u524d\u7aef\u6309\u94ae\u7528\u4e8e\u6570\u636e\u5e93\u4e3a\u7a7a\u65f6\u5feb\u901f\u5199\u5165\u6f14\u793a\u6570\u636e\u3002")
        seed_clicked = st.button("\u8865\u5145\u6f14\u793a\u6570\u636e", use_container_width=True)
        reset_seed_clicked = st.button("\u6e05\u7a7a\u5e76\u91cd\u7f6e\u6f14\u793a\u6570\u636e", use_container_width=True)
        st.code("powershell -ExecutionPolicy Bypass -File scripts\\start.ps1", language="powershell")

    qp_view = st.query_params.get("view", realtime_view)
    if isinstance(qp_view, list):
        qp_view = qp_view[0] if qp_view else realtime_view
    if qp_view not in view_options:
        qp_view = realtime_view

    if "active_view" not in st.session_state:
        st.session_state["active_view"] = qp_view
    if st.session_state["active_view"] not in view_options:
        st.session_state["active_view"] = realtime_view

    if auto and st.session_state["active_view"] != llm_view:
        st_autorefresh(interval=refresh_sec * 1000, key="refresh")

    using_mock = False
    mock_reason = ""
    dropped_news = 0
    dropped_period = 0
    try:
        if seed_clicked or reset_seed_clicked:
            news_rows, period_rows = seed_demo_data(host, int(port), db, user, password, reset=reset_seed_clicked)
            st.success(f"\u5df2\u5199\u5165\u6f14\u793a\u6570\u636e\uff1a\u65b0\u95fb {news_rows} \u6761\uff0c\u65f6\u6bb5 {period_rows} \u6761\u3002")
            st.rerun()

        tables = list_tables(host, int(port), db, user, password)
        news_table = pick_table(tables, ["newcounts", "newscount"])
        period_table = pick_table(tables, ["periodcounts", "periodcount"])
        if not news_table or not period_table:
            st.error(f"\u672a\u627e\u5230\u76ee\u6807\u8868\u3002\u5f53\u524d\u53ef\u7528\u8868: {tables}")
            return

        df_news = fetch_table(host, int(port), db, user, password, news_table, limit)
        df_period = fetch_table(host, int(port), db, user, password, period_table, limit)
        if enable_placeholder_clean:
            df_news, dropped_news = clean_placeholder_rows(df_news)
            df_period, dropped_period = clean_placeholder_rows(df_period)

        if AUTO_SEED_ON_EMPTY and (df_news.empty or df_period.empty):
            news_rows, period_rows = seed_demo_data(host, int(port), db, user, password)
            st.info(f"\u68c0\u6d4b\u5230\u6570\u636e\u5e93\u4e3a\u7a7a\uff0c\u5df2\u81ea\u52a8\u8865\u5145\u6f14\u793a\u6570\u636e\uff1a\u65b0\u95fb {news_rows} \u6761\uff0c\u65f6\u6bb5 {period_rows} \u6761\u3002")
            df_news = fetch_table(host, int(port), db, user, password, news_table, limit)
            df_period = fetch_table(host, int(port), db, user, password, period_table, limit)

        if enable_mock_fallback and (looks_like_placeholder_table(df_news) and looks_like_placeholder_table(df_period)):
            using_mock = True
            mock_reason = (
                "\u68c0\u6d4b\u5230\u5360\u4f4d\u6570\u636e\uff08\u4f8b\u5982\u91cd\u590d\u7684 name/count/logtime\uff09\uff0c"
                "\u5df2\u81ea\u52a8\u5207\u6362\u4e3a Mock \u53ef\u89c6\u5316\u6f14\u793a\u6570\u636e\u3002"
            )
            df_news, df_period = build_mock_data()
            news_table = "mock_newscount"
            period_table = "mock_periodcount"
    except Exception as e:
        using_mock = True
        mock_reason = f"\u6570\u636e\u5e93\u8bfb\u53d6\u5f02\u5e38\uff0c\u5df2\u81ea\u52a8\u5207\u6362\u4e3a Mock \u53ef\u89c6\u5316\u6f14\u793a\u6570\u636e\u3002\u539f\u56e0: {e}"
        df_news, df_period = build_mock_data()
        news_table = "mock_newscount"
        period_table = "mock_periodcount"

    if using_mock:
        st.warning(mock_reason)
    else:
        st.caption(
            f"\u6570\u636e\u6e90: {db}.{news_table}, {db}.{period_table} | "
            f"\u6e05\u6d17\u5360\u4f4d\u884c\u6570: news={dropped_news}, period={dropped_period}"
        )

    _, _, n_click, _ = infer_schema(df_news)
    _, _, p_click, _ = infer_schema(df_period)
    render_kpis(df_news, df_period, n_click, p_click)

    current_view = st.radio(
        "\u6a21\u5757\u5bfc\u822a",
        view_options,
        index=view_options.index(st.session_state["active_view"]),
        horizontal=True,
        label_visibility="collapsed",
    )
    st.session_state["active_view"] = current_view
    st.query_params["view"] = current_view

    if current_view == realtime_view:
        st.markdown(f"### {realtime_view}")
        render_realtime(df_news, df_period)
    elif current_view == advanced_view:
        st.markdown(f"### {advanced_view}")
        render_advanced_visuals(df_news, df_period)
    elif current_view == forecast_view:
        st.markdown(f"### {forecast_view}")
        render_forecast(df_news, df_period)
    elif current_view == anomaly_view:
        st.markdown(f"### {anomaly_view}")
        render_anomaly_alert(df_news, df_period)
    elif current_view == cluster_view:
        st.markdown(f"### {cluster_view}")
        render_cluster(df_news, df_period)
    elif current_view == assoc_view:
        st.markdown(f"### {assoc_view}")
        render_assoc(df_news, df_period)
    elif current_view == raw_view:
        st.markdown(f"### {raw_view}")
        st.write(f"\u65b0\u95fb\u8868: `{news_table}`")
        st.dataframe(df_news.head(500), use_container_width=True)
        st.write(f"\u65f6\u6bb5\u8868: `{period_table}`")
        st.dataframe(df_period.head(500), use_container_width=True)
        st.write("\u81ea\u52a8\u8bc6\u522b\u5b57\u6bb5:")
        st.json(
            {
                "news": dict(zip(["id", "title", "click", "time"], infer_schema(df_news))),
                "period": dict(zip(["id", "title", "click", "time"], infer_schema(df_period))),
            }
        )
    else:
        st.markdown(f"### {llm_view}")
        render_llm_module(df_news, df_period)

if __name__ == "__main__":
    main()
