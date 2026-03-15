"""
Polymarket SPX/NDX 垂直市场扫描 — Finance/Indices 分类精英交易者发现与追踪
"""
import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta

st.set_page_config(page_title="Polymarket 指数交易者追踪", layout="wide")
st.title("📊 Polymarket Finance/Indices 精英交易者追踪")

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
DATA_API_BASE = "https://data-api.polymarket.com"
DATA_API_TRADES_URL = f"{DATA_API_BASE}/trades"
LEADERBOARD_URL = f"{DATA_API_BASE}/v1/leaderboard"
PROFILE_BASE = "https://polymarket.com/profile/"
INDEX_TAG_IDS = ["102849", "102682"]  # S&P 500, Indicies

if "watchlist" not in st.session_state:
    st.session_state.watchlist = []
if "spx_ndx_market_ids" not in st.session_state:
    st.session_state.spx_ndx_market_ids = []
if "scan_df" not in st.session_state:
    st.session_state.scan_df = None


# --- 侧边栏配置 ---
st.sidebar.header("扫描配置")
scan_period = st.sidebar.radio(
    "排行榜时间范围",
    options=["WEEK", "MONTH", "ALL"],
    format_func=lambda x: {"WEEK": "过去一周", "MONTH": "过去一个月", "ALL": "全部时间"}[x],
    index=1,
)
min_volume = st.sidebar.number_input("最低交易量 ($)", min_value=0, value=500, step=100, help="过滤掉交易量过低的小号")
min_pnl = st.sidebar.number_input("最低盈亏 ($)", value=-100, step=100, help="过滤掉亏损过多的用户")
top_n = st.sidebar.slider("拉取排行榜前 N 名", 10, 200, 100, step=10)

st.sidebar.divider()
st.sidebar.header("评分权重")
st.sidebar.caption("Score = Consistency×w1 + Returns×w2 + WinRate×w3 + MaxLoss×w4 + ProfitFactor×w5")
w_consistency = st.sidebar.slider("Consistency (一致性)", 0.0, 1.0, 0.25)
w_returns = st.sidebar.slider("Returns (收益率)", 0.0, 1.0, 0.25)
w_winrate = st.sidebar.slider("Win Rate (胜率)", 0.0, 1.0, 0.20)
w_maxloss = st.sidebar.slider("Max Loss (最大回撤)", 0.0, 1.0, 0.15)
w_pf = st.sidebar.slider("Profit Factor (盈亏比)", 0.0, 1.0, 0.15)


# --- 1. 定位市场 conditionId（用于关注列表追踪）---
@st.cache_data(ttl=600)
def fetch_index_condition_ids():
    out = set()
    for tag_id in INDEX_TAG_IDS:
        try:
            r = requests.get(GAMMA_EVENTS_URL, params={"tag_id": tag_id, "limit": 100, "closed": "false"}, timeout=15)
            r.raise_for_status()
            events = r.json()
            if isinstance(events, list):
                for ev in events:
                    for mk in (ev.get("markets") or []):
                        cid = mk.get("conditionId") if isinstance(mk, dict) else None
                        if cid:
                            out.add(cid)
                    cid = ev.get("conditionId")
                    if cid:
                        out.add(cid)
        except Exception:
            pass
    return list(out)


# --- 2. 从 FINANCE 排行榜拉取顶级交易者（真实 PNL & Volume）---
@st.cache_data(ttl=300)
def fetch_finance_leaderboard(time_period, order_by="PNL", limit=100):
    """从 /v1/leaderboard?category=FINANCE 拉取排行榜"""
    all_entries = []
    for offset in range(0, limit, 50):
        batch = min(50, limit - offset)
        try:
            r = requests.get(
                LEADERBOARD_URL,
                params={"category": "FINANCE", "timePeriod": time_period, "orderBy": order_by, "limit": batch, "offset": offset},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                all_entries.extend(data)
        except Exception:
            break
    return all_entries


def _normalize_ts(ts):
    if not ts:
        return 0
    return int(ts) // 1000 if int(ts) > 1e12 else int(ts)


# --- 3. 从 trades 统计每个用户在 SPX/NDX 市场的实际交易笔数和金额 ---
def enrich_with_index_trades(addresses, condition_ids, since_ts, progress_bar=None):
    """
    对给定用户集合，查询他们在 SPX/NDX 市场的实际交易笔数和总金额。
    返回 {addr: {"index_trades": int, "index_volume": float}}
    """
    condition_set = set(condition_ids)
    stats = {a: {"index_trades": 0, "index_volume": 0.0} for a in addresses}
    addr_list = list(addresses)
    total = len(addr_list)

    for i, addr in enumerate(addr_list):
        if progress_bar:
            progress_bar.progress((i + 1) / total, text=f"验证用户 SPX/NDX 交易记录... ({i+1}/{total})")
        try:
            r = requests.get(DATA_API_TRADES_URL, params={"user": addr, "limit": 500}, timeout=15)
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
            if cid in condition_set:
                stats[addr]["index_trades"] += 1
                stats[addr]["index_volume"] += (t.get("size") or 0) * (t.get("price") or 0)
    return stats


# --- 4. 评分 ---
def calculate_score(row):
    vol = row.get("Volume", 0)
    pnl = row.get("PNL", 0)
    idx_trades = row.get("指数交易笔数", 0)

    roi = (pnl / vol) if vol > 0 else 0
    returns_norm = min(1.0, max(0, (roi + 0.5) / 1.0))
    consistency = min(1.0, idx_trades / 20.0)
    win_rate_est = min(1.0, max(0, 0.5 + roi * 2))
    max_loss_norm = 1.0 - min(1.0, max(0, -pnl / max(vol, 1)) if pnl < 0 else 0)
    pf = min(1.0, max(0, (pnl + vol * 0.1) / max(vol * 0.2, 1)))

    raw = (
        consistency * w_consistency
        + returns_norm * w_returns
        + win_rate_est * w_winrate
        + max_loss_norm * w_maxloss
        + pf * w_pf
    )
    return round(min(100, max(0, raw * 100)), 1)


# --- 5. 构建表格 ---
def build_traders_df(leaderboard_entries, index_stats):
    rows = []
    for entry in leaderboard_entries:
        addr = entry.get("proxyWallet", "")
        if not addr:
            continue
        ist = index_stats.get(addr, {})
        rows.append({
            "选中": False,
            "Rank": int(entry.get("rank", 0)),
            "address": addr,
            "用户名": entry.get("userName") or "",
            "PNL": round(entry.get("pnl", 0), 2),
            "Volume": round(entry.get("vol", 0), 2),
            "指数交易笔数": ist.get("index_trades", 0),
            "指数交易额": round(ist.get("index_volume", 0), 2),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["Score"] = df.apply(calculate_score, axis=1)
    return df.sort_values("Score", ascending=False).reset_index(drop=True)


# --- 6. watchlist 下注查询 ---
def fetch_recent_trades_for_watchlist(watchlist_addresses, condition_ids, since_ts):
    if not watchlist_addresses or not condition_ids:
        return []
    condition_set = set(condition_ids)
    out = []
    for addr in watchlist_addresses:
        try:
            r = requests.get(DATA_API_TRADES_URL, params={"user": addr, "limit": 500}, timeout=15)
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
                "timestamp": ts, "address": addr,
                "side": t.get("side", ""),
                "title": t.get("title") or t.get("slug") or "",
                "outcome": t.get("outcome", ""),
                "price": t.get("price"), "size": t.get("size"),
            })
    out.sort(key=lambda x: x["timestamp"], reverse=True)
    return out[:100]


# ========== 主流程 ==========
st.sidebar.divider()
do_scan = st.sidebar.button("🔄 扫描 Finance 排行榜")

if do_scan:
    with st.spinner("第一步：获取 SPX/NDX 市场列表..."):
        cids = fetch_index_condition_ids()
    st.session_state.spx_ndx_market_ids = cids

    period_label = {"WEEK": "过去一周", "MONTH": "过去一个月", "ALL": "全部时间"}.get(scan_period, scan_period)
    st.write(f"第二步：拉取 FINANCE 排行榜 Top {top_n}（{period_label}）...")
    entries = fetch_finance_leaderboard(scan_period, limit=top_n)
    if not entries:
        st.warning("排行榜为空，请稍后重试。")
        st.session_state.scan_df = None
        st.stop()

    filtered = [e for e in entries if e.get("vol", 0) >= min_volume and e.get("pnl", 0) >= min_pnl]
    if not filtered:
        st.warning(f"排行榜 Top {top_n} 中无满足条件（Volume≥${min_volume}, PNL≥${min_pnl}）的交易者。")
        st.session_state.scan_df = None
        st.stop()
    st.success(f"排行榜拉取 {len(entries)} 人，筛选后 {len(filtered)} 人")

    addrs = {e["proxyWallet"] for e in filtered if e.get("proxyWallet")}
    delta_map = {"WEEK": timedelta(days=7), "MONTH": timedelta(days=30), "ALL": timedelta(days=365)}
    since_ts = int((datetime.now(timezone.utc) - delta_map.get(scan_period, timedelta(days=30))).timestamp())

    st.write(f"第三步：验证 {len(addrs)} 个用户的 SPX/NDX 实际交易...")
    progress_bar = st.progress(0, text="验证中...")
    index_stats = enrich_with_index_trades(addrs, cids, since_ts, progress_bar=progress_bar)
    progress_bar.empty()

    has_index = [e for e in filtered if index_stats.get(e.get("proxyWallet"), {}).get("index_trades", 0) > 0]
    st.success(f"其中 {len(has_index)} 人有 SPX/NDX 实际交易记录")

    df = build_traders_df(has_index if has_index else filtered, index_stats)
    st.session_state.scan_df = df
    st.session_state.scan_show_all = not has_index

if st.session_state.scan_df is not None:
    if getattr(st.session_state, "scan_show_all", False):
        st.warning("排行榜用户中暂无 SPX/NDX 实际交易记录，展示的是 FINANCE 排行榜全部用户。")

    st.subheader("🏆 Finance 排行榜交易者（可勾选并加入关注）")
    edited = st.data_editor(
        st.session_state.scan_df,
        column_config={
            "选中": st.column_config.CheckboxColumn("选中", default=False),
            "address": st.column_config.TextColumn("地址", width="medium"),
            "Score": st.column_config.NumberColumn("得分", format="%.1f"),
            "PNL": st.column_config.NumberColumn("盈亏($)", format="$%.2f"),
            "Volume": st.column_config.NumberColumn("交易量($)", format="$%.2f"),
            "指数交易额": st.column_config.NumberColumn("指数交易额($)", format="$%.2f"),
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
    st.info("点击左侧「🔄 扫描 Finance 排行榜」开始。")

# --- 关注列表与实时追踪 ---
st.divider()
st.subheader("👁 关注列表与 SPX/NDX 实时动态")
if st.session_state.watchlist:
    st.caption("当前关注地址（点击跳转 Polymarket 个人页）")
    for addr in st.session_state.watchlist:
        st.markdown(f"- [{addr}]({PROFILE_BASE}{addr})")
    if st.button("清空关注列表", type="secondary"):
        st.session_state.watchlist = []
        st.rerun()

    if st.button("🔄 刷新 SPX/NDX 下注记录"):
        st.session_state.watchlist_trades = None
        st.rerun()

    st.caption("展示关注用户最近 24 小时内在 SPX/NDX 市场的下注")
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
                "时间": ts_str, "地址": t["address"], "方向": t.get("side", ""),
                "市场": (t.get("title") or "")[:60], "结果": t.get("outcome", ""),
                "价格": t.get("price"), "数量": t.get("size"),
                "Profile": f"{PROFILE_BASE}{t['address']}",
            })
        st.dataframe(
            pd.DataFrame(trades_data),
            column_config={"Profile": st.column_config.LinkColumn("Profile", display_text="查看")},
            hide_index=True, use_container_width=True,
        )
    else:
        st.info("关注用户在最近 24 小时内无 SPX/NDX 下注记录。")
else:
    st.info("关注列表为空。请先扫描并勾选地址后点击「添加到关注」。")
