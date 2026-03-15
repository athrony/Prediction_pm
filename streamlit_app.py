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
# 关键词（不区分大小写）：匹配标题/描述中含以下任一词的市场
INDEX_KEYWORDS = [
    "s&p 500", "s&p500", "nasdaq", "spx", "ndx",
    "s and p 500", "standard and poor", "stock index", "market index",
]
PROFILE_BASE = "https://polymarket.com/profile/"


def _market_matches_index_keywords(m):
    """判断市场标题/描述是否包含指数相关关键词"""
    q = (m.get("question") or m.get("title") or "").lower()
    desc = (m.get("description") or "").lower()
    text = f"{q} {desc}"
    return any(kw in text for kw in INDEX_KEYWORDS)


# --- 2. 第一步：定位市场 ---
@st.cache_data(ttl=300)
def fetch_index_markets():
    """
    从 Gamma API 拉取与 SPX/NDX 相关的市场（优先活跃，无活跃时含近期有交易的市场）。
    先请求 /events（活跃），再请求 /markets（不限 closed），合并并去重。
    """
    out_by_cid = {}  # conditionId -> info，用于去重

    # 策略 1：拉取活跃 events，从中提取 markets（含 conditionId）
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
                if not _market_matches_index_keywords(ev):
                    continue
                # events 可能内嵌 markets 或单个 conditionId
                markets_in = ev.get("markets") or []
                if isinstance(markets_in, list) and markets_in:
                    for mk in markets_in:
                        cid = mk.get("conditionId") if isinstance(mk, dict) else None
                        if cid and cid not in out_by_cid:
                            out_by_cid[cid] = {"id": mk.get("id") or ev.get("id"), "conditionId": cid, "question": (ev.get("title") or ev.get("question") or "")[:80]}
                else:
                    cid = ev.get("conditionId")
                    if cid and cid not in out_by_cid:
                        out_by_cid[cid] = {"id": ev.get("id"), "conditionId": cid, "question": (ev.get("title") or ev.get("question") or "")[:80]}
    except Exception as e:
        st.warning(f"拉取 events 时出错（将仅用 markets）: {e}")

    # 策略 2：拉取 markets（不传 closed，以拿到更多结果），按关键词筛选；多页以增加命中率
    for offset in (0, 500):
        try:
            r = requests.get(
                GAMMA_MARKETS_URL,
                params={"limit": 500, "offset": offset},
                timeout=15,
            )
            r.raise_for_status()
            markets = r.json()
            if not isinstance(markets, list) or not markets:
                break
            for m in markets:
                if not _market_matches_index_keywords(m):
                    continue
                cid = m.get("conditionId")
                if cid and cid not in out_by_cid:
                    out_by_cid[cid] = {"id": m.get("id"), "conditionId": cid, "question": (m.get("question") or m.get("title") or "")[:80]}
        except Exception as e:
            if offset == 0:
                st.error(f"拉取 markets 失败: {e}")
            break

    return list(out_by_cid.values())


# --- 3. 第二步：提取地址 ---
def _normalize_ts(ts):
    """API 可能返回秒或毫秒，统一为秒"""
    if not ts:
        return 0
    return int(ts) // 1000 if int(ts) > 1e12 else int(ts)


def fetch_trades_for_markets(condition_ids, since_ts):
    """
    拉取交易并提取用户地址。优先按 market(conditionId) 拉取；若无结果则拉取全平台近期成交再按 conditionId 过滤；
    若仍无则返回全平台近期交易地址。
    返回 (addresses_set, used_platform_fallback: bool)。
    """
    if not condition_ids:
        return set(), False
    condition_set = set(condition_ids)
    addresses = set()

    def parse_trades(trades, filter_by_cid=True):
        out = set()
        for t in trades:
            ts = _normalize_ts(t.get("timestamp") or t.get("timestampSeconds"))
            if ts < since_ts:
                continue
            if filter_by_cid and (t.get("conditionId") or "").strip() not in condition_set:
                continue
            addr = (t.get("proxyWallet") or t.get("user") or t.get("owner") or "").strip()
            if addr and isinstance(addr, str) and addr.startswith("0x"):
                out.add(addr)
        return out

    # 策略 1：按 market 拉取（每次最多 5 个 conditionId，避免 URL 过长）
    for i in range(0, min(len(condition_ids), 25), 5):
        chunk = condition_ids[i : i + 5]
        market_param = ",".join(chunk)
        try:
            r = requests.get(
                DATA_API_TRADES_URL,
                params={"market": market_param, "limit": 5000},
                timeout=20,
            )
            r.raise_for_status()
            trades = r.json()
            if isinstance(trades, list):
                addresses |= parse_trades(trades, filter_by_cid=True)
        except Exception:
            pass

    if addresses:
        return addresses, False

    # 策略 2：不按 market 拉取，取近期全平台成交再按 conditionId 过滤
    try:
        r = requests.get(DATA_API_TRADES_URL, params={"limit": 10000}, timeout=25)
        r.raise_for_status()
        trades = r.json()
        if isinstance(trades, list):
            addresses = parse_trades(trades, filter_by_cid=True)
    except Exception:
        pass

    if addresses:
        return addresses, False

    # 策略 3：仍无则用全平台近期交易地址（不按 conditionId 过滤）
    try:
        r = requests.get(DATA_API_TRADES_URL, params={"limit": 10000}, timeout=25)
        r.raise_for_status()
        trades = r.json()
        if isinstance(trades, list):
            addresses = parse_trades(trades, filter_by_cid=False)
    except Exception:
        pass

    return addresses, True


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
    """拉取 watchlist 中用户在 SPX/NDX 市场（condition_ids）的近期交易"""
    if not watchlist_addresses or not condition_ids:
        return []
    market_param = ",".join(condition_ids[:15])
    try:
        r = requests.get(
            DATA_API_TRADES_URL,
            params={"market": market_param, "limit": 500},
            timeout=15,
        )
        r.raise_for_status()
        trades = r.json()
    except Exception:
        return []
    watch_set = set(w.lower() for w in watchlist_addresses)
    out = []
    for t in trades:
        ts = t.get("timestamp") or t.get("timestampSeconds") or 0
        if ts < since_ts:
            continue
        addr = (t.get("proxyWallet") or t.get("user") or "").strip()
        if not addr or addr.lower() not in watch_set:
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
    return out[:50]


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
if st.sidebar.button("🔄 扫描 SPX/NDX 市场并提取交易者"):
    st.session_state.run_scan = True
else:
    st.session_state.run_scan = getattr(st.session_state, "run_scan", False)

if st.session_state.run_scan:
    with st.spinner("第一步：定位 SPX/NDX 活跃市场..."):
        index_markets = fetch_index_markets()
    if not index_markets:
        st.warning(
            "未找到包含 S&P 500 / Nasdaq / SPX / NDX 相关关键词的市场。"
            "可能当前暂无此类活跃市场，或 API 暂无返回；请稍后重试或检查网络。"
        )
        st.stop()
    condition_ids = [m["conditionId"] for m in index_markets]
    st.session_state.spx_ndx_market_ids = condition_ids
    st.success(f"找到 {len(index_markets)} 个相关市场")

    delta = SCAN_RANGE_OPTIONS.get(scan_range_label, timedelta(hours=24))
    since_ts = int((datetime.now(timezone.utc) - delta).timestamp())
    with st.spinner(f"第二步：提取{scan_range_label}交易用户地址..."):
        all_addresses, used_platform_fallback = fetch_trades_for_markets(condition_ids, since_ts)
    if not all_addresses:
        st.warning(f"{scan_range_label}内无交易流水，无法提取地址。请稍后重试。")
        st.stop()
    st.success(f"去重后得到 {len(all_addresses)} 个地址")
    if used_platform_fallback:
        st.info("未在 SPX/NDX 相关市场中找到近期成交，当前展示的是全平台近期活跃交易者。")

    with st.spinner("第三步：批量性能查询与深度评分..."):
        metrics_map = get_address_metrics(list(all_addresses))
    df_traders = build_traders_df(list(all_addresses), metrics_map)

    st.subheader("🏆 指数交易者列表（可勾选并加入关注）")
    edited = st.data_editor(
        df_traders,
        column_config={
            "选中": st.column_config.CheckboxColumn("选中", default=False),
            "address": st.column_config.TextColumn("地址", width="medium"),
            "Score": st.column_config.NumberColumn("得分", format="%.1f"),
        },
        hide_index=True,
        use_container_width=True,
    )
    if st.button("➕ 添加到关注"):
        selected = edited[edited["选中"] == True]
        addrs = selected["address"].tolist()
        for a in addrs:
            if a and a not in st.session_state.watchlist:
                st.session_state.watchlist.append(a)
        if addrs:
            st.success(f"已添加 {len(addrs)} 个地址到关注列表")
        st.rerun()
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

    if st.button("🔄 刷新实时动态"):
        st.rerun()
    st.caption("仅展示关注列表中用户在 SPX/NDX 市场的新订单（基于最近一次拉取）")
    cids = getattr(st.session_state, "spx_ndx_market_ids", []) or []
    poll_ts = int((datetime.now(timezone.utc) - timedelta(minutes=5)).timestamp())
    recent = fetch_recent_trades_for_watchlist(st.session_state.watchlist, cids, poll_ts)
    if recent:
        for t in recent:
            ts_str = datetime.fromtimestamp(t["timestamp"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            profile_link = f"[{t['address'][:10]}...]({PROFILE_BASE}{t['address']})"
            st.markdown(f"- **{ts_str}** — {profile_link} — {t.get('side', '')} — {t.get('title', '')[:50]} — {t.get('outcome', '')}")
    else:
        st.info("暂无 watchlist 用户在 SPX/NDX 市场的新订单；请稍后刷新页面重试。")
else:
    st.info("关注列表为空。请先完成上方扫描并勾选地址后点击「添加到关注」。")
