"""
Polymarket SPX/NDX 垂直市场扫描 — 从指数相关市场发现交易者并追踪
"""
import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta
import time

# --- 1. 基础配置 ---
st.set_page_config(page_title="Polymarket 指数交易者追踪", layout="wide")
st.title("📊 Polymarket SPX/NDX 垂直市场扫描")

# 侧边栏：评分权重（与公式一致）
st.sidebar.header("评分权重配置")
st.sidebar.caption("Score = Consistency×0.25 + Returns×0.25 + WinRate×0.20 + MaxLoss×0.15 + ProfitFactor×0.15")
w_consistency = st.sidebar.slider("Consistency (一致性)", 0.0, 1.0, 0.25)
w_returns = st.sidebar.slider("Returns (收益率)", 0.0, 1.0, 0.25)
w_winrate = st.sidebar.slider("Win Rate (胜率)", 0.0, 1.0, 0.20)
w_maxloss = st.sidebar.slider("Max Loss (最大回撤)", 0.0, 1.0, 0.15)
w_pf = st.sidebar.slider("Profit Factor (盈亏比)", 0.0, 1.0, 0.15)

# 初始化 watchlist
if "watchlist" not in st.session_state:
    st.session_state.watchlist = []
if "spx_ndx_market_ids" not in st.session_state:
    st.session_state.spx_ndx_market_ids = []  # conditionIds 用于实时追踪

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
DATA_API_TRADES_URL = "https://data-api.polymarket.com/trades"
INDEX_KEYWORDS = [
    "s&p 500", "s&p500", "s\u0026p 500", "s\u0026p500",
    "nasdaq", "spx", "ndx",
    "s and p 500", "standard and poor",
]
PROFILE_BASE = "https://polymarket.com/profile/"

# 已知的 SPX/NDX 相关 event slug（Polymarket 对 restricted 市场不会在通用列表中返回，需要按 slug 精确获取）
KNOWN_INDEX_EVENT_SLUGS = [
    "spx-hit-jun-2026",
    "spx-close-dec-2026",
    "sp-500-performance-in-q1",
    "spx-hit-dec-2026",
    "bitcoin-vs-gold-vs-sp-500-in-2026",
    "sp-500-performance-in-q2",
    "sp-500-performance-in-q3",
    "sp-500-performance-in-q4",
    "nasdaq-100-hit-2026",
    "ndx-close-dec-2026",
]
# Polymarket 的 tag_id: "S&P 500" = 102849, "Indicies" = 102682, "Finance" = 120
INDEX_TAG_IDS = ["102849", "102682"]


def _market_matches_index_keywords(m):
    q = (m.get("question") or m.get("title") or "").lower()
    desc = (m.get("description") or "").lower()
    text = f"{q} {desc}"
    return any(kw in text for kw in INDEX_KEYWORDS)


def _extract_markets_from_event(ev, out_by_cid):
    """从一个 event dict 中提取所有 market 的 conditionId"""
    title = (ev.get("title") or ev.get("question") or "")[:80]
    markets_in = ev.get("markets") or []
    if isinstance(markets_in, list) and markets_in:
        for mk in markets_in:
            cid = mk.get("conditionId") if isinstance(mk, dict) else None
            q = (mk.get("question") or title)[:80] if isinstance(mk, dict) else title
            if cid and cid not in out_by_cid:
                out_by_cid[cid] = {"id": mk.get("id") or ev.get("id"), "conditionId": cid, "question": q}
    else:
        cid = ev.get("conditionId")
        if cid and cid not in out_by_cid:
            out_by_cid[cid] = {"id": ev.get("id"), "conditionId": cid, "question": title}


# --- 2. 第一步：定位市场 ---
@st.cache_data(ttl=300)
def fetch_index_markets():
    """
    三种策略拉取 SPX/NDX 相关市场：
    1. 按已知 event slug 精确获取（解决 restricted 市场不出现在通用列表中的问题）
    2. 按 tag_id (S&P 500, Indicies) 搜索
    3. 通用列表 + 关键词匹配（兜底）
    """
    out_by_cid = {}

    # 策略 1：按已知 slug 精确获取
    for slug in KNOWN_INDEX_EVENT_SLUGS:
        try:
            r = requests.get(GAMMA_EVENTS_URL, params={"slug": slug}, timeout=10)
            r.raise_for_status()
            events = r.json()
            if isinstance(events, list):
                for ev in events:
                    _extract_markets_from_event(ev, out_by_cid)
        except Exception:
            pass

    # 策略 2：按 tag_id 搜索
    for tag_id in INDEX_TAG_IDS:
        try:
            r = requests.get(
                GAMMA_EVENTS_URL,
                params={"tag_id": tag_id, "limit": 100, "closed": "false"},
                timeout=15,
            )
            r.raise_for_status()
            events = r.json()
            if isinstance(events, list):
                for ev in events:
                    _extract_markets_from_event(ev, out_by_cid)
        except Exception:
            pass

    # 策略 3：通用列表 + 关键词匹配
    try:
        r = requests.get(
            GAMMA_EVENTS_URL,
            params={"limit": 200, "closed": "false", "active": "true"},
            timeout=15,
        )
        r.raise_for_status()
        events = r.json()
        if isinstance(events, list):
            for ev in events:
                if _market_matches_index_keywords(ev):
                    _extract_markets_from_event(ev, out_by_cid)
    except Exception:
        pass

    return list(out_by_cid.values())


# --- 3. 第二步：提取地址 ---
def _normalize_ts(ts):
    """API 可能返回秒或毫秒，统一为秒"""
    if not ts:
        return 0
    return int(ts) // 1000 if int(ts) > 1e12 else int(ts)


def fetch_trades_for_markets(condition_ids, since_ts):
    """
    严格按 conditionId 从 /trades 拉取成交记录，只提取在这些市场中有交易的用户地址。
    逐个 conditionId 请求，确保只拿到 SPX/NDX 市场的真实交易者。
    """
    if not condition_ids:
        return set()
    addresses = set()

    for cid in condition_ids:
        for offset in (0, 5000):
            try:
                r = requests.get(
                    DATA_API_TRADES_URL,
                    params={"market": cid, "limit": 5000, "offset": offset},
                    timeout=20,
                )
                r.raise_for_status()
                trades = r.json()
            except Exception:
                break
            if not isinstance(trades, list) or not trades:
                break
            for t in trades:
                ts = _normalize_ts(t.get("timestamp") or t.get("timestampSeconds"))
                if ts < since_ts:
                    continue
                addr = (t.get("proxyWallet") or "").strip()
                if addr and addr.startswith("0x"):
                    addresses.add(addr)

    return addresses


# --- 4. 第三步：深度评分（你的公式）---
def get_address_metrics(addresses):
    """
    对地址做批量“性能查询”。此处为占位实现：无公开 Polymarket 绩效 API 时用模拟数据。
    接入真实数据源时替换此函数即可。
    """
    import random
    out = {}
    for addr in addresses:
        # 模拟 0–1 指标；实际应替换为你的绩效接口
        out[addr] = {
            "consistency": random.uniform(0.3, 0.98),
            "returns": random.uniform(0.2, 0.95),
            "win_rate": random.uniform(0.4, 0.9),
            "max_loss": random.uniform(0.05, 0.5),   # 越小越好
            "profit_factor": random.uniform(0.5, 1.0),
        }
    return out


def calculate_score(metrics, w_consistency, w_returns, w_winrate, w_maxloss, w_pf):
    """
    百分制: Score = (Consistency×0.25) + (Returns×0.25) + (WinRate×0.20) + (MaxLoss×0.15) + (ProfitFactor×0.15)
    MaxLoss 为回撤，越小越好，此处用 (1 - max_loss) 参与计算使分数越高越好。
    """
    c = metrics["consistency"]
    r = metrics["returns"]
    w = metrics["win_rate"]
    ml = 1.0 - metrics["max_loss"]  # 回撤取反
    pf = metrics["profit_factor"]
    raw = (c * w_consistency + r * w_returns + w * w_winrate + ml * w_maxloss + pf * w_pf)
    return round(min(100, max(0, raw * 100)), 1)


# --- 5. 第四步：界面筛选（data_editor + 添加到关注）---
def build_traders_df(addresses, metrics_map):
    """构建带评分的交易者 DataFrame，并加上勾选列"""
    rows = []
    for addr in addresses:
        m = metrics_map.get(addr, {})
        if not m:
            m = {"consistency": 0, "returns": 0, "win_rate": 0, "max_loss": 0.5, "profit_factor": 0}
        score = calculate_score(m, w_consistency, w_returns, w_winrate, w_maxloss, w_pf)
        rows.append({
            "选中": False,
            "address": addr,
            "Score": score,
            "Consistency": round(m.get("consistency", 0), 3),
            "Returns": round(m.get("returns", 0), 3),
            "WinRate": round(m.get("win_rate", 0), 3),
            "MaxLoss": round(m.get("max_loss", 0), 3),
            "ProfitFactor": round(m.get("profit_factor", 0), 3),
        })
    return pd.DataFrame(rows).sort_values("Score", ascending=False)


def fetch_recent_trades_for_watchlist(watchlist_addresses, condition_ids, since_ts):
    """
    拉取 watchlist 中每个用户最近 24 小时内在 SPX/NDX 市场的下注。
    严格按 conditionId 匹配，不用关键词模糊匹配。
    """
    if not watchlist_addresses or not condition_ids:
        return []
    condition_set = set(condition_ids)
    out = []
    for addr in watchlist_addresses:
        try:
            r = requests.get(
                DATA_API_TRADES_URL,
                params={"user": addr, "limit": 500},
                timeout=15,
            )
            r.raise_for_status()
            trades = r.json()
        except Exception:
            continue
        if not isinstance(trades, list):
            continue
        for t in trades:
            ts = _normalize_ts(t.get("timestamp") or t.get("timestampSeconds"))
            if ts < since_ts:
                continue
            cid = (t.get("conditionId") or "").strip()
            if cid not in condition_set:
                continue
            out.append({
                "timestamp": ts,
                "address": addr,
                "side": t.get("side", ""),
                "title": t.get("title") or t.get("slug") or "",
                "outcome": t.get("outcome", ""),
                "price": t.get("price"),
                "size": t.get("size"),
            })
    out.sort(key=lambda x: x["timestamp"], reverse=True)
    return out[:100]


# 扫描时间范围选项（用于第二步提取交易地址）
SCAN_RANGE_OPTIONS = {
    "过去24小时": timedelta(hours=24),
    "过去一周": timedelta(days=7),
    "过去一个月": timedelta(days=30),
}

# ========== 主流程 ==========
st.sidebar.divider()
st.sidebar.subheader("垂直市场扫描")
scan_range_label = st.sidebar.radio(
    "扫描时间范围",
    options=list(SCAN_RANGE_OPTIONS.keys()),
    index=0,
    help="提取该时间范围内在 SPX/NDX 市场有交易的用户地址",
)

# 初始化扫描结果缓存
if "scan_df" not in st.session_state:
    st.session_state.scan_df = None
if "scan_market_count" not in st.session_state:
    st.session_state.scan_market_count = 0
if "scan_addr_count" not in st.session_state:
    st.session_state.scan_addr_count = 0

do_scan = st.sidebar.button("🔄 扫描 SPX/NDX 市场并提取交易者")

if do_scan:
    with st.spinner("第一步：定位 SPX/NDX 活跃市场..."):
        index_markets = fetch_index_markets()
    if not index_markets:
        st.warning(
            "未找到包含 S&P 500 / Nasdaq / SPX / NDX 相关关键词的市场。"
            "可能当前暂无此类活跃市场，或 API 暂无返回；请稍后重试或检查网络。"
        )
        st.session_state.scan_df = None
        st.stop()
    condition_ids = [m["conditionId"] for m in index_markets]
    st.session_state.spx_ndx_market_ids = condition_ids
    st.session_state.scan_market_count = len(index_markets)

    delta = SCAN_RANGE_OPTIONS.get(scan_range_label, timedelta(hours=24))
    since_ts = int((datetime.now(timezone.utc) - delta).timestamp())
    with st.spinner(f"第二步：提取{scan_range_label}交易用户地址..."):
        all_addresses = fetch_trades_for_markets(condition_ids, since_ts)
    if not all_addresses:
        st.warning(
            f"{scan_range_label}内未找到 SPX/NDX 相关交易者。"
            "当前 Polymarket 上可能暂无此类市场的活跃成交，请稍后重试。"
        )
        st.session_state.scan_df = None
        st.stop()
    st.session_state.scan_addr_count = len(all_addresses)

    with st.spinner("第三步：批量性能查询与深度评分..."):
        metrics_map = get_address_metrics(list(all_addresses))
    df_traders = build_traders_df(list(all_addresses), metrics_map)
    st.session_state.scan_df = df_traders

if st.session_state.scan_df is not None:
    st.success(f"找到 {st.session_state.scan_market_count} 个相关市场 · 去重后得到 {st.session_state.scan_addr_count} 个 SPX/NDX 交易者")

    st.subheader("🏆 指数交易者列表（可勾选并加入关注）")
    edited = st.data_editor(
        st.session_state.scan_df,
        column_config={
            "选中": st.column_config.CheckboxColumn("选中", default=False),
            "address": st.column_config.TextColumn("地址", width="medium"),
            "Score": st.column_config.NumberColumn("得分", format="%.1f"),
        },
        hide_index=True,
        use_container_width=True,
        key="traders_editor",
    )
    if st.button("➕ 添加到关注"):
        selected = edited[edited["选中"] == True]
        addrs = selected["address"].tolist()
        added = 0
        for a in addrs:
            if a and a not in st.session_state.watchlist:
                st.session_state.watchlist.append(a)
                added += 1
        if added:
            st.success(f"已添加 {added} 个地址到关注列表")
        elif not addrs:
            st.warning("请先勾选至少一个地址，再点击添加。")
else:
    st.info("点击左侧「🔄 扫描 SPX/NDX 市场并提取交易者」开始垂直市场扫描。")

# --- 第五步：关注列表与实时追踪 ---
st.divider()
st.subheader("👁 关注列表与 SPX/NDX 实时动态")
if st.session_state.watchlist:
    st.caption("当前关注地址（点击地址可跳转 Polymarket 个人页）")
    for addr in st.session_state.watchlist:
        link = f"[{addr}]({PROFILE_BASE}{addr})"
        st.markdown(f"- {link}")
    if st.button("清空关注列表", type="secondary"):
        st.session_state.watchlist = []
        st.rerun()

    if st.button("🔄 刷新 SPX/NDX 下注记录"):
        st.session_state.watchlist_trades = None
        st.rerun()

    st.caption("展示关注列表中用户最近 24 小时内在 SPX/NDX 相关市场的下注记录")
    cids = st.session_state.get("spx_ndx_market_ids") or []
    since_24h = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp())

    if "watchlist_trades" not in st.session_state or st.session_state.watchlist_trades is None:
        with st.spinner("正在查询关注用户的 SPX/NDX 下注记录..."):
            st.session_state.watchlist_trades = fetch_recent_trades_for_watchlist(
                st.session_state.watchlist, cids, since_24h
            )

    recent = st.session_state.watchlist_trades
    if recent:
        trades_data = []
        for t in recent:
            ts_str = datetime.fromtimestamp(t["timestamp"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            trades_data.append({
                "时间": ts_str,
                "地址": t["address"],
                "方向": t.get("side", ""),
                "市场": (t.get("title") or "")[:60],
                "结果": t.get("outcome", ""),
                "价格": t.get("price"),
                "数量": t.get("size"),
                "Profile": f"{PROFILE_BASE}{t['address']}",
            })
        df_trades = pd.DataFrame(trades_data)
        st.dataframe(
            df_trades,
            column_config={
                "Profile": st.column_config.LinkColumn("Profile", display_text="查看"),
            },
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("关注用户在最近 24 小时内无 SPX/NDX 相关下注记录。请稍后刷新重试。")
else:
    st.info("关注列表为空。请先完成上方扫描并勾选地址后点击「添加到关注」。")
