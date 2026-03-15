"""
Polymarket SPX/NDX 垂直市场扫描 — Market-First 精英交易者发现与追踪

策略：从 SPX/NDX 市场本身出发，通过 /trades?market= 和 /holders?market=
直接发现活跃交易者，而非从排行榜逐个验证。
"""
import streamlit as st
import pandas as pd
import requests
from collections import defaultdict
from datetime import datetime, timezone, timedelta

st.set_page_config(page_title="Polymarket 指数交易者追踪", layout="wide")
st.title("📊 Polymarket SPX/NDX 精英交易者追踪")

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
DATA_API_BASE = "https://data-api.polymarket.com"
DATA_API_TRADES_URL = f"{DATA_API_BASE}/trades"
DATA_API_HOLDERS_URL = f"{DATA_API_BASE}/holders"
PROFILE_BASE = "https://polymarket.com/profile/"
INDEX_TAG_IDS = ["102849", "102682"]  # S&P 500, Indicies

if "watchlist" not in st.session_state:
    st.session_state.watchlist = []
if "spx_ndx_market_ids" not in st.session_state:
    st.session_state.spx_ndx_market_ids = []
if "scan_df" not in st.session_state:
    st.session_state.scan_df = None

# --------------- 侧边栏配置 ---------------
st.sidebar.header("扫描配置")
scan_window = st.sidebar.radio(
    "扫描时间窗口",
    options=["3D", "7D", "30D", "ALL"],
    format_func=lambda x: {"3D": "过去 3 天", "7D": "过去 7 天", "30D": "过去 30 天", "ALL": "全部时间"}[x],
    index=1,
)
min_trades = st.sidebar.number_input(
    "最少交易笔数", min_value=1, value=3, step=1,
    help="用户在 SPX/NDX 市场至少需要多少笔交易才被纳入",
)
min_volume = st.sidebar.number_input(
    "最低交易量 ($)", min_value=0, value=100, step=50,
    help="过滤掉交易量过低的小号",
)

st.sidebar.divider()
st.sidebar.header("评分权重")
st.sidebar.caption("Score = Consistency×w1 + Returns×w2 + WinRate×w3 + MaxLoss×w4 + ProfitFactor×w5")
w_consistency = st.sidebar.slider("Consistency (一致性)", 0.0, 1.0, 0.25)
w_returns = st.sidebar.slider("Returns (收益率)", 0.0, 1.0, 0.25)
w_winrate = st.sidebar.slider("Win Rate (胜率)", 0.0, 1.0, 0.20)
w_maxloss = st.sidebar.slider("Max Loss (最大回撤)", 0.0, 1.0, 0.15)
w_pf = st.sidebar.slider("Profit Factor (盈亏比)", 0.0, 1.0, 0.15)


# ========== 1. 定位市场 conditionId ==========

@st.cache_data(ttl=600)
def fetch_index_condition_ids():
    """从 Gamma Events API 获取所有 SPX/NDX 相关市场的 conditionId"""
    out = set()
    for tag_id in INDEX_TAG_IDS:
        for closed_flag in ("false", "true"):
            try:
                r = requests.get(
                    GAMMA_EVENTS_URL,
                    params={"tag_id": tag_id, "limit": 100, "closed": closed_flag},
                    timeout=15,
                )
                r.raise_for_status()
                events = r.json()
                if not isinstance(events, list):
                    continue
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


# ========== 2. Market-First: 从市场交易记录直接发现交易者 ==========

def _normalize_ts(ts):
    if not ts:
        return 0
    return int(ts) // 1000 if int(ts) > 1e12 else int(ts)


def _time_window_seconds(window_key):
    mapping = {"3D": 3, "7D": 7, "30D": 30, "ALL": 3650}
    days = mapping.get(window_key, 7)
    return int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())


@st.cache_data(ttl=300, show_spinner=False)
def fetch_trades_by_market(condition_ids, since_ts):
    """
    用 /trades?market={cid} 按市场拉取所有交易记录，
    返回原始交易列表 (list[dict])。
    每个 conditionId 单独请求并分页。
    """
    all_trades = []
    for cid in condition_ids:
        offset = 0
        page_size = 10000
        while True:
            try:
                r = requests.get(
                    DATA_API_TRADES_URL,
                    params={
                        "market": cid,
                        "limit": page_size,
                        "offset": offset,
                        "takerOnly": "false",
                    },
                    timeout=30,
                )
                r.raise_for_status()
                batch = r.json()
            except Exception:
                break
            if not isinstance(batch, list) or not batch:
                break
            for t in batch:
                ts = _normalize_ts(t.get("timestamp") or t.get("timestampSeconds"))
                if ts >= since_ts:
                    all_trades.append(t)
            if len(batch) < page_size:
                break
            offset += page_size
            if offset >= 10000:
                break
    return all_trades


@st.cache_data(ttl=300, show_spinner=False)
def fetch_holders_by_market(condition_ids):
    """
    用 /holders?market={cid} 获取当前持仓者。
    返回 {proxyWallet: total_holding_value}
    """
    holdings = defaultdict(float)
    for cid in condition_ids:
        try:
            r = requests.get(
                DATA_API_HOLDERS_URL,
                params={"market": cid, "limit": 500},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue
        if not isinstance(data, list):
            continue
        for token_group in data:
            for h in (token_group.get("holders") or []):
                wallet = h.get("proxyWallet", "")
                if wallet:
                    holdings[wallet] += h.get("amount", 0)
    return dict(holdings)


# ========== 3. 从原始交易数据计算每个用户的真实指标 ==========

def compute_trader_stats(trades_list):
    """
    从原始交易列表聚合每个用户的统计数据：
    - trade_count: 交易笔数
    - volume: 总交易额 (size * price)
    - buy_volume / sell_volume
    - gross_profit / gross_loss (基于 BUY 以低价买入 vs SELL 以高价卖出的估算)
    - win_count / loss_count (BUY price < 0.5 或 SELL price > 0.5 视为 "好交易")
    - markets_traded: 参与的不同市场数
    - username / pseudonym (从交易记录提取)

    返回 dict[address, stats_dict]
    """
    user_data = defaultdict(lambda: {
        "trade_count": 0,
        "volume": 0.0,
        "buy_volume": 0.0,
        "sell_volume": 0.0,
        "gross_profit": 0.0,
        "gross_loss": 0.0,
        "win_count": 0,
        "loss_count": 0,
        "markets": set(),
        "username": "",
        "pseudonym": "",
    })

    for t in trades_list:
        wallet = (t.get("proxyWallet") or "").strip()
        if not wallet:
            continue

        size = t.get("size") or 0
        price = t.get("price") or 0
        side = (t.get("side") or "").upper()
        cid = t.get("conditionId") or ""
        notional = size * price

        u = user_data[wallet]
        u["trade_count"] += 1
        u["volume"] += notional
        u["markets"].add(cid)

        if not u["username"]:
            u["username"] = t.get("name") or t.get("pseudonym") or ""
        if not u["pseudonym"]:
            u["pseudonym"] = t.get("pseudonym") or ""

        if side == "BUY":
            u["buy_volume"] += notional
            potential_profit = size * (1.0 - price)
            potential_loss = size * price
            if price < 0.50:
                u["win_count"] += 1
                u["gross_profit"] += potential_profit
            else:
                u["loss_count"] += 1
                u["gross_loss"] += potential_loss
        elif side == "SELL":
            u["sell_volume"] += notional
            potential_profit = size * price
            potential_loss = size * (1.0 - price)
            if price > 0.50:
                u["win_count"] += 1
                u["gross_profit"] += potential_profit
            else:
                u["loss_count"] += 1
                u["gross_loss"] += potential_loss

    result = {}
    for wallet, d in user_data.items():
        total = d["win_count"] + d["loss_count"]
        result[wallet] = {
            "trade_count": d["trade_count"],
            "volume": round(d["volume"], 2),
            "buy_volume": round(d["buy_volume"], 2),
            "sell_volume": round(d["sell_volume"], 2),
            "win_count": d["win_count"],
            "loss_count": d["loss_count"],
            "win_rate": round(d["win_count"] / total, 4) if total > 0 else 0.0,
            "gross_profit": round(d["gross_profit"], 2),
            "gross_loss": round(d["gross_loss"], 2),
            "profit_factor": round(d["gross_profit"] / d["gross_loss"], 2) if d["gross_loss"] > 0 else 99.0,
            "markets_traded": len(d["markets"]),
            "username": d["username"],
            "pseudonym": d["pseudonym"],
        }
    return result


# ========== 4. 评分（基于真实指标） ==========

def calculate_score(row, weights):
    w_c, w_r, w_w, w_m, w_p = weights
    trade_count = row.get("交易笔数", 0)
    volume = row.get("交易量", 0)
    win_rate = row.get("胜率", 0)
    profit_factor = row.get("盈亏比", 0)
    gross_profit = row.get("潜在盈利", 0)
    gross_loss = row.get("潜在亏损", 0)

    consistency = min(1.0, trade_count / 30.0)

    net = gross_profit - gross_loss
    roi = net / volume if volume > 0 else 0
    returns_norm = min(1.0, max(0, (roi + 0.3) / 0.8))

    wr_norm = min(1.0, max(0, win_rate))

    max_loss_norm = 1.0 - min(1.0, gross_loss / max(volume, 1))

    pf_norm = min(1.0, profit_factor / 5.0) if profit_factor < 99 else 1.0

    raw = (
        consistency * w_c
        + returns_norm * w_r
        + wr_norm * w_w
        + max_loss_norm * w_m
        + pf_norm * w_p
    )
    return round(min(100, max(0, raw * 100)), 1)


# ========== 5. 构建交易者表格 ==========

def build_traders_df(trader_stats, holder_data, weights):
    rows = []
    for addr, s in trader_stats.items():
        display_name = s.get("username") or s.get("pseudonym") or ""
        holding = holder_data.get(addr, 0)
        rows.append({
            "选中": False,
            "address": addr,
            "用户名": display_name,
            "交易笔数": s["trade_count"],
            "交易量": s["volume"],
            "胜率": s["win_rate"],
            "盈亏比": s["profit_factor"],
            "潜在盈利": s["gross_profit"],
            "潜在亏损": s["gross_loss"],
            "参与市场数": s["markets_traded"],
            "当前持仓": round(holding, 2),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["Score"] = df.apply(lambda r: calculate_score(r, weights), axis=1)
    return df.sort_values("Score", ascending=False).reset_index(drop=True)


# ========== 6. Watchlist 下注查询（保留原有逻辑） ==========

def fetch_recent_trades_for_watchlist(watchlist_addresses, condition_ids, since_ts):
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
                "timestamp": ts, "address": addr,
                "side": t.get("side", ""),
                "title": t.get("title") or t.get("slug") or "",
                "outcome": t.get("outcome", ""),
                "price": t.get("price"), "size": t.get("size"),
            })
    out.sort(key=lambda x: x["timestamp"], reverse=True)
    return out[:200]


# ==================== 主流程 ====================
st.sidebar.divider()
do_scan = st.sidebar.button("🔍 扫描 SPX/NDX 市场交易者")

if do_scan:
    # Step 1: 获取市场 conditionId
    with st.spinner("第一步：获取 SPX/NDX 市场列表..."):
        cids = fetch_index_condition_ids()
    if not cids:
        st.error("未找到任何 SPX/NDX 市场，请稍后重试。")
        st.stop()
    st.session_state.spx_ndx_market_ids = cids
    st.success(f"找到 {len(cids)} 个 SPX/NDX 市场")

    since_ts = _time_window_seconds(scan_window)
    window_label = {"3D": "3 天", "7D": "7 天", "30D": "30 天", "ALL": "全部"}.get(scan_window, scan_window)

    # Step 2: 从市场直接拉取交易记录
    with st.spinner(f"第二步：从 {len(cids)} 个市场拉取交易记录（{window_label}）..."):
        all_trades = fetch_trades_by_market(tuple(cids), since_ts)
    st.success(f"共获取 {len(all_trades)} 条交易记录")

    if not all_trades:
        st.warning("该时间窗口内无 SPX/NDX 交易记录。")
        st.session_state.scan_df = None
        st.stop()

    # Step 3: 聚合用户统计 + 获取持仓数据
    with st.spinner("第三步：聚合交易者数据 & 查询当前持仓..."):
        trader_stats = compute_trader_stats(all_trades)
        holder_data = fetch_holders_by_market(tuple(cids))

    total_traders = len(trader_stats)
    trader_stats = {
        addr: s for addr, s in trader_stats.items()
        if s["trade_count"] >= min_trades and s["volume"] >= min_volume
    }
    st.success(
        f"发现 {total_traders} 个交易者，"
        f"筛选后（≥{min_trades} 笔, ≥${min_volume}）剩余 {len(trader_stats)} 人"
    )

    if not trader_stats:
        st.warning("没有满足筛选条件的交易者，尝试降低最少交易笔数或最低交易量。")
        st.session_state.scan_df = None
        st.stop()

    # Step 4: 构建表格
    weights = (w_consistency, w_returns, w_winrate, w_maxloss, w_pf)
    df = build_traders_df(trader_stats, holder_data, weights)
    st.session_state.scan_df = df

# --------------- 展示扫描结果 ---------------
if st.session_state.scan_df is not None:
    st.subheader("🏆 SPX/NDX 活跃交易者（可勾选并加入关注）")
    edited = st.data_editor(
        st.session_state.scan_df,
        column_config={
            "选中": st.column_config.CheckboxColumn("选中", default=False),
            "address": st.column_config.TextColumn("地址", width="medium"),
            "Score": st.column_config.NumberColumn("得分", format="%.1f"),
            "交易量": st.column_config.NumberColumn("交易量($)", format="$%.2f"),
            "胜率": st.column_config.NumberColumn("胜率", format="%.1%%"),
            "盈亏比": st.column_config.NumberColumn("盈亏比", format="%.2f"),
            "潜在盈利": st.column_config.NumberColumn("潜在盈利($)", format="$%.2f"),
            "潜在亏损": st.column_config.NumberColumn("潜在亏损($)", format="$%.2f"),
            "当前持仓": st.column_config.NumberColumn("当前持仓", format="%.2f"),
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
    st.info("点击左侧「🔍 扫描 SPX/NDX 市场交易者」开始。")

# --------------- 关注列表与实时追踪 ---------------
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
