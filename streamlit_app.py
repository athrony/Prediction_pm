"""
Polymarket SPX/NDX 垂直市场扫描 — Market-First 精英交易者发现与追踪

策略：从 SPX/NDX 市场本身出发，通过 /trades?market= 和 /holders?market=
直接发现活跃交易者，而非从排行榜逐个验证。

本地缓存：交易记录持久化到 data/ 目录，增量更新避免重复拉取。
"""
import io
import json
import streamlit as st
import pandas as pd
import requests
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

st.set_page_config(page_title="Polymarket 指数交易者追踪", layout="wide")
st.title("📊 Polymarket SPX/NDX 精英交易者追踪")

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
GAMMA_PROFILE_URL = "https://gamma-api.polymarket.com/public-profile"
DATA_API_BASE = "https://data-api.polymarket.com"
DATA_API_TRADES_URL = f"{DATA_API_BASE}/trades"
DATA_API_HOLDERS_URL = f"{DATA_API_BASE}/holders"
PROFILE_BASE = "https://polymarket.com/profile/"
INDEX_TAG_IDS = ["102849", "102682"]  # S&P 500, Indicies

DATA_DIR = Path(__file__).parent / "data"
TRADES_CACHE_FILE = DATA_DIR / "trades_cache.json"
WATCHLIST_FILE = DATA_DIR / "watchlist.json"
CONDITION_IDS_FILE = DATA_DIR / "condition_ids.json"


# ========== 本地缓存工具 ==========

def _ensure_data_dir():
    DATA_DIR.mkdir(exist_ok=True)


def load_cached_trades():
    if TRADES_CACHE_FILE.exists():
        try:
            with open(TRADES_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {"trades": [], "last_ts": 0, "fetched_at": ""}


def save_cached_trades(cache_obj):
    _ensure_data_dir()
    with open(TRADES_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache_obj, f, ensure_ascii=False)


def _trade_key(t):
    """用于去重的复合键"""
    return (
        t.get("conditionId", ""),
        t.get("proxyWallet", ""),
        str(t.get("timestamp", "")),
        t.get("side", ""),
        str(t.get("size", "")),
        str(t.get("price", "")),
    )


def merge_trades(existing, new_trades):
    """将新交易合并到现有列表，按复合键去重"""
    seen = {_trade_key(t) for t in existing}
    merged = list(existing)
    added = 0
    for t in new_trades:
        k = _trade_key(t)
        if k not in seen:
            seen.add(k)
            merged.append(t)
            added += 1
    return merged, added


def load_watchlist():
    """加载关注列表。兼容旧格式 [addr, ...] 自动迁移为 [{address, username, avatar}, ...]"""
    if WATCHLIST_FILE.exists():
        try:
            with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return []
        if not isinstance(data, list):
            return []
        if data and isinstance(data[0], str):
            migrated = [{"address": a, "username": "", "avatar": ""} for a in data]
            save_watchlist(migrated)
            return migrated
        return data
    return []


def save_watchlist(wl):
    _ensure_data_dir()
    with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
        json.dump(wl, f, ensure_ascii=False)


def watchlist_addresses(wl):
    """从关注列表提取地址列表"""
    return [item["address"] for item in wl]


def watchlist_find(wl, addr):
    """在关注列表中查找指定地址的条目"""
    for item in wl:
        if item["address"] == addr:
            return item
    return None


def fetch_user_profile(address):
    """从 Polymarket Gamma API 获取用户公开资料（头像、用户名）"""
    try:
        r = requests.get(GAMMA_PROFILE_URL, params={"address": address}, timeout=10)
        r.raise_for_status()
        data = r.json()
        return {
            "username": data.get("name") or data.get("pseudonym") or "",
            "avatar": data.get("profileImage") or "",
        }
    except Exception:
        return {"username": "", "avatar": ""}


def watchlist_to_excel_bytes(wl):
    """将关注列表导出为 Excel 字节流"""
    rows = []
    for item in wl:
        rows.append({
            "address": item["address"],
            "username": item.get("username", ""),
            "avatar": item.get("avatar", ""),
            "profile_url": f"{PROFILE_BASE}{item['address']}",
        })
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    return buf.getvalue()


def watchlist_from_excel(uploaded_file):
    """从 Excel 导入关注列表"""
    df = pd.read_excel(uploaded_file, engine="openpyxl")
    items = []
    for _, row in df.iterrows():
        addr = str(row.get("address", "")).strip()
        if not addr:
            continue
        items.append({
            "address": addr,
            "username": str(row.get("username", "") or ""),
            "avatar": str(row.get("avatar", "") or ""),
        })
    return items


def load_cached_condition_ids():
    if CONDITION_IDS_FILE.exists():
        try:
            with open(CONDITION_IDS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return []


def save_condition_ids(cids):
    _ensure_data_dir()
    with open(CONDITION_IDS_FILE, "w", encoding="utf-8") as f:
        json.dump(cids, f, ensure_ascii=False)


def save_scan_excel(df):
    """扫描结果自动保存到 data/ 目录，文件名含时间戳"""
    _ensure_data_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = DATA_DIR / f"scan_{ts}.xlsx"
    export_df = df.drop(columns=["选中"], errors="ignore")
    export_df.to_excel(path, index=False, engine="openpyxl")
    return path


def df_to_excel_bytes(df):
    """将 DataFrame 转为 xlsx 字节流，用于浏览器下载"""
    export_df = df.drop(columns=["选中"], errors="ignore")
    buf = io.BytesIO()
    export_df.to_excel(buf, index=False, engine="openpyxl")
    return buf.getvalue()


def load_scan_from_excel(uploaded_file):
    """从上传的 Excel 文件恢复扫描结果 DataFrame"""
    df = pd.read_excel(uploaded_file, engine="openpyxl")
    df.insert(0, "选中", False)
    return df


def list_saved_excels():
    """列出 data/ 目录下所有已保存的 scan_*.xlsx 文件"""
    if not DATA_DIR.exists():
        return []
    files = sorted(DATA_DIR.glob("scan_*.xlsx"), reverse=True)
    return files


# ========== Session State 初始化 ==========

if "watchlist" not in st.session_state:
    st.session_state.watchlist = load_watchlist()
if "spx_ndx_market_ids" not in st.session_state:
    st.session_state.spx_ndx_market_ids = load_cached_condition_ids()
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
    result = list(out)
    if result:
        save_condition_ids(result)
    return result


# ========== 2. Market-First: 从市场交易记录直接发现交易者 ==========

def _normalize_ts(ts):
    if not ts:
        return 0
    return int(ts) // 1000 if int(ts) > 1e12 else int(ts)


def _time_window_seconds(window_key):
    mapping = {"3D": 3, "7D": 7, "30D": 30, "ALL": 3650}
    days = mapping.get(window_key, 7)
    return int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())


def fetch_trades_by_market_raw(condition_ids, since_ts):
    """
    从 API 拉取交易记录（不含缓存逻辑）。
    返回原始交易列表 (list[dict])。
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


def fetch_trades_incremental(condition_ids, since_ts, force_full=False):
    """
    增量拉取：读取本地缓存，仅从缓存中最新时间戳之后拉取新数据，
    合并后写回缓存。返回时间窗口内的全部交易。

    force_full=True 时忽略缓存，全量重新拉取。
    """
    cache = load_cached_trades()

    if force_full:
        new_trades = fetch_trades_by_market_raw(condition_ids, since_ts)
        merged, added = merge_trades([], new_trades)
        cache_obj = {
            "trades": merged,
            "last_ts": max((_normalize_ts(t.get("timestamp") or t.get("timestampSeconds")) for t in merged), default=0),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        save_cached_trades(cache_obj)
        return merged, len(merged), 0

    cached_trades = cache.get("trades", [])
    cached_last_ts = cache.get("last_ts", 0)

    fetch_since = max(since_ts, cached_last_ts - 3600) if cached_last_ts > 0 else since_ts
    new_trades = fetch_trades_by_market_raw(condition_ids, fetch_since)
    merged, added = merge_trades(cached_trades, new_trades)

    new_last_ts = max(
        cached_last_ts,
        max((_normalize_ts(t.get("timestamp") or t.get("timestampSeconds")) for t in new_trades), default=0),
    )
    cache_obj = {
        "trades": merged,
        "last_ts": new_last_ts,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    save_cached_trades(cache_obj)

    filtered = [
        t for t in merged
        if _normalize_ts(t.get("timestamp") or t.get("timestampSeconds")) >= since_ts
    ]
    return filtered, added, len(cached_trades)


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


# ========== 6. Watchlist: 查询关注用户的 Active 持仓 ==========

DATA_API_POSITIONS_URL = f"{DATA_API_BASE}/positions"


def fetch_active_positions_for_watchlist(watchlist_addresses, condition_ids, progress_bar=None):
    """
    查询关注用户在 SPX/NDX 市场中所有 Active 持仓（未平仓）。

    conditionId 数量过多时 URL 会超长触发 414，因此先拉取用户全部持仓，
    再按 conditionId 集合在客户端过滤。
    """
    if not watchlist_addresses or not condition_ids:
        return []
    cid_set = set(condition_ids)
    out = []
    total = len(watchlist_addresses)
    for i, addr in enumerate(watchlist_addresses):
        if progress_bar:
            short = addr[:8] + "..." + addr[-4:]
            progress_bar.progress((i + 1) / total, text=f"查询持仓 ({i+1}/{total}) {short}")
        try:
            r = requests.get(
                DATA_API_POSITIONS_URL,
                params={"user": addr, "limit": 500, "sizeThreshold": 0.01},
                timeout=10,
            )
            r.raise_for_status()
            all_pos = r.json()
        except Exception:
            continue
        if not isinstance(all_pos, list):
            continue

        for p in all_pos:
            cid = (p.get("conditionId") or "").strip()
            size = p.get("size", 0)
            if cid not in cid_set or size <= 0:
                continue
            out.append({
                "address": addr,
                "title": p.get("title") or p.get("slug") or "",
                "outcome": p.get("outcome", ""),
                "size": round(size, 2),
                "avg_price": round(p.get("avgPrice", 0), 4),
                "cur_price": round(p.get("curPrice", 0), 4),
                "current_value": round(p.get("currentValue", 0), 2),
                "initial_value": round(p.get("initialValue", 0), 2),
                "cash_pnl": round(p.get("cashPnl", 0), 2),
                "pct_pnl": round(p.get("percentPnl", 0) * 100, 1) if p.get("percentPnl") else 0,
                "end_date": p.get("endDate", ""),
            })
    out.sort(key=lambda x: abs(x.get("current_value", 0)), reverse=True)
    return out


# ==================== 主流程 ====================
st.sidebar.divider()

# 缓存状态展示
cache = load_cached_trades()
cached_count = len(cache.get("trades", []))
fetched_at = cache.get("fetched_at", "")
if cached_count > 0:
    ts_label = fetched_at[:19].replace("T", " ") if fetched_at else "未知"
    st.sidebar.caption(f"💾 本地缓存: {cached_count} 条交易 | 更新于 {ts_label}")
else:
    st.sidebar.caption("💾 本地缓存: 无数据")

col_s1, col_s2 = st.sidebar.columns(2)
do_scan = col_s1.button("🔍 增量扫描")
do_full = col_s2.button("🔄 全量刷新")

if do_scan or do_full:
    force_full = do_full

    # Step 1
    with st.spinner("第一步：获取 SPX/NDX 市场列表..."):
        cids = fetch_index_condition_ids()
    if not cids:
        st.error("未找到任何 SPX/NDX 市场，请稍后重试。")
        st.stop()
    st.session_state.spx_ndx_market_ids = cids
    st.success(f"找到 {len(cids)} 个 SPX/NDX 市场")

    since_ts = _time_window_seconds(scan_window)
    window_label = {"3D": "3 天", "7D": "7 天", "30D": "30 天", "ALL": "全部"}.get(scan_window, scan_window)

    # Step 2
    mode_label = "全量刷新" if force_full else "增量更新"
    with st.spinner(f"第二步：{mode_label} — 从 {len(cids)} 个市场拉取交易记录（{window_label}）..."):
        all_trades, new_count, old_count = fetch_trades_incremental(
            cids, since_ts, force_full=force_full,
        )

    if force_full:
        st.success(f"全量拉取完成：共 {len(all_trades)} 条交易，已缓存到本地")
    else:
        st.success(
            f"增量更新完成：本地已有 {old_count} 条 → 新增 {new_count} 条 → "
            f"时间窗口内共 {len(all_trades)} 条"
        )

    if not all_trades:
        st.warning("该时间窗口内无 SPX/NDX 交易记录。")
        st.session_state.scan_df = None
        st.stop()

    # Step 3
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

    # Step 4
    weights = (w_consistency, w_returns, w_winrate, w_maxloss, w_pf)
    df = build_traders_df(trader_stats, holder_data, weights)
    st.session_state.scan_df = df

    # 自动保存 Excel
    try:
        saved_path = save_scan_excel(df)
        st.success(f"扫描结果已自动保存: {saved_path.name}")
    except Exception as e:
        st.warning(f"自动保存 Excel 失败: {e}")

# --------------- 导入历史数据 ---------------
st.sidebar.divider()
st.sidebar.header("数据导入 / 导出")

saved_files = list_saved_excels()
if saved_files:
    file_names = [f.name for f in saved_files]
    selected_file = st.sidebar.selectbox(
        "加载本地历史扫描", options=["不加载"] + file_names,
        help="选择之前保存的扫描结果 Excel 文件",
    )
    if selected_file != "不加载" and st.sidebar.button("📂 加载选中文件"):
        fpath = DATA_DIR / selected_file
        try:
            st.session_state.scan_df = load_scan_from_excel(fpath)
            st.sidebar.success(f"已加载 {selected_file}")
        except Exception as e:
            st.sidebar.error(f"加载失败: {e}")

uploaded = st.sidebar.file_uploader(
    "或上传 Excel 导入", type=["xlsx"],
    help="上传之前导出的扫描结果 .xlsx 文件",
)
if uploaded is not None:
    try:
        st.session_state.scan_df = load_scan_from_excel(uploaded)
        st.sidebar.success(f"已导入 {uploaded.name}")
    except Exception as e:
        st.sidebar.error(f"导入失败: {e}")

# --------------- 展示扫描结果 ---------------
if st.session_state.scan_df is not None:
    st.subheader("🏆 SPX/NDX 活跃交易者（可勾选并加入关注）")

    col_dl, col_add = st.columns([1, 1])
    with col_dl:
        st.download_button(
            "📥 导出当前结果为 Excel",
            data=df_to_excel_bytes(st.session_state.scan_df),
            file_name=f"polymarket_spx_ndx_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

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
    with col_add:
        if st.button("➕ 添加到关注"):
            selected = edited[edited["选中"] == True]
            existing_addrs = set(watchlist_addresses(st.session_state.watchlist))
            added = 0
            for _, row in selected.iterrows():
                addr = row.get("address", "")
                if addr and addr not in existing_addrs:
                    username = row.get("用户名", "")
                    st.session_state.watchlist.append({
                        "address": addr,
                        "username": username,
                        "avatar": "",
                    })
                    existing_addrs.add(addr)
                    added += 1
            if added:
                save_watchlist(st.session_state.watchlist)
                st.success(f"已添加 {added} 个地址到关注列表（已持久化）")
            elif selected.empty:
                st.warning("请先勾选至少一个地址，再点击添加。")
else:
    st.info("点击左侧「🔍 增量扫描」或「🔄 全量刷新」开始，或从侧边栏导入历史数据。")

# --------------- 关注列表与实时追踪 ---------------
st.divider()
st.subheader("👁 关注列表与 SPX/NDX 实时动态")

if st.session_state.watchlist:
    # --- 关注列表工具栏 ---
    wl_col1, wl_col2, wl_col3, wl_col4 = st.columns([1, 1, 1, 1])
    with wl_col1:
        if st.button("🔄 刷新头像 / 用户名"):
            with st.spinner("正在获取用户资料..."):
                updated = 0
                for item in st.session_state.watchlist:
                    profile = fetch_user_profile(item["address"])
                    if profile["username"] or profile["avatar"]:
                        item["username"] = profile["username"] or item.get("username", "")
                        item["avatar"] = profile["avatar"] or item.get("avatar", "")
                        updated += 1
                save_watchlist(st.session_state.watchlist)
            st.success(f"已更新 {updated} 个用户的资料")
            st.rerun()
    with wl_col2:
        st.download_button(
            "📥 导出关注列表",
            data=watchlist_to_excel_bytes(st.session_state.watchlist),
            file_name=f"watchlist_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with wl_col3:
        wl_upload = st.file_uploader("📤 导入关注列表", type=["xlsx"], key="wl_upload", label_visibility="collapsed")
    with wl_col4:
        if st.button("🗑 清空关注列表", type="secondary"):
            st.session_state.watchlist = []
            save_watchlist([])
            st.rerun()

    if wl_upload is not None:
        try:
            imported = watchlist_from_excel(wl_upload)
            existing = set(watchlist_addresses(st.session_state.watchlist))
            added = 0
            for item in imported:
                if item["address"] not in existing:
                    st.session_state.watchlist.append(item)
                    existing.add(item["address"])
                    added += 1
            save_watchlist(st.session_state.watchlist)
            st.success(f"已导入 {added} 个新地址（重复 {len(imported) - added} 个已跳过）")
            st.rerun()
        except Exception as e:
            st.error(f"导入失败: {e}")

    # --- 手动添加地址 ---
    with st.expander("➕ 手动添加地址"):
        manual_addr = st.text_input("输入钱包地址", placeholder="0x...", key="manual_addr_input")
        if st.button("添加", key="manual_add_btn"):
            addr = manual_addr.strip()
            if addr:
                existing = set(watchlist_addresses(st.session_state.watchlist))
                if addr in existing:
                    st.warning("该地址已在关注列表中。")
                else:
                    profile = fetch_user_profile(addr)
                    st.session_state.watchlist.append({
                        "address": addr,
                        "username": profile["username"],
                        "avatar": profile["avatar"],
                    })
                    save_watchlist(st.session_state.watchlist)
                    st.success(f"已添加: {profile['username'] or addr[:12] + '...'}")
                    st.rerun()
            else:
                st.warning("请输入有效的钱包地址。")

    # --- 每个用户卡片 ---
    st.caption(f"当前关注 {len(st.session_state.watchlist)} 个用户")
    for idx, item in enumerate(st.session_state.watchlist):
        addr = item["address"]
        uname = item.get("username", "") or addr[:10] + "..."
        avatar = item.get("avatar", "")

        cols = st.columns([0.6, 4, 1.5, 1, 1])
        with cols[0]:
            if avatar:
                st.image(avatar, width=40)
            else:
                st.markdown("👤")
        with cols[1]:
            st.markdown(f"**{uname}**")
            st.caption(f"`{addr[:8]}...{addr[-6:]}`")
        with cols[2]:
            st.link_button("🔗 Profile", f"{PROFILE_BASE}{addr}", use_container_width=True)
        with cols[3]:
            if st.button("🔍", key=f"solo_{idx}", help="单独查询该用户持仓"):
                st.session_state[f"solo_query_{idx}"] = True
        with cols[4]:
            if st.button("❌", key=f"rm_{idx}", help="移除该用户"):
                st.session_state.watchlist.pop(idx)
                save_watchlist(st.session_state.watchlist)
                st.rerun()

        # 单独查询某个用户持仓
        if st.session_state.get(f"solo_query_{idx}"):
            cids = st.session_state.get("spx_ndx_market_ids") or []
            if not cids:
                st.warning("请先执行一次扫描以获取 SPX/NDX 市场列表。")
            else:
                with st.spinner(f"查询 {uname} 的持仓..."):
                    solo_pos = fetch_active_positions_for_watchlist([addr], cids)
                if solo_pos:
                    solo_data = []
                    for p in solo_pos:
                        solo_data.append({
                            "市场": (p.get("title") or "")[:60],
                            "方向": p.get("outcome", ""),
                            "持仓量": p.get("size"),
                            "均价": p.get("avg_price"),
                            "现价": p.get("cur_price"),
                            "现值($)": p.get("current_value"),
                            "成本($)": p.get("initial_value"),
                            "盈亏($)": p.get("cash_pnl"),
                            "盈亏%": p.get("pct_pnl"),
                            "到期日": (p.get("end_date") or "")[:10],
                        })
                    st.dataframe(pd.DataFrame(solo_data), hide_index=True, use_container_width=True)
                else:
                    st.info(f"{uname} 暂无 SPX/NDX Active 持仓。")
            del st.session_state[f"solo_query_{idx}"]

    # --- 批量查询所有关注用户持仓 ---
    st.divider()
    cids = st.session_state.get("spx_ndx_market_ids") or []

    if st.button("🔄 查询 / 刷新全部关注用户 SPX/NDX 持仓"):
        if not cids:
            st.warning("请先执行一次扫描以获取 SPX/NDX 市场列表。")
        else:
            addrs = watchlist_addresses(st.session_state.watchlist)
            progress_bar = st.progress(0, text="查询持仓中...")
            st.session_state.watchlist_positions = fetch_active_positions_for_watchlist(
                addrs, cids, progress_bar=progress_bar
            )
            progress_bar.empty()

    st.caption("关注用户在 SPX/NDX 市场中所有 Active（未平仓）持仓")
    positions = st.session_state.get("watchlist_positions")
    if positions:
        pos_data = []
        for p in positions:
            addr = p["address"]
            wl_item = watchlist_find(st.session_state.watchlist, addr)
            display = (wl_item.get("username", "") if wl_item else "") or addr[:10] + "..."
            pos_data.append({
                "用户": display,
                "市场": (p.get("title") or "")[:60],
                "方向": p.get("outcome", ""),
                "持仓量": p.get("size"),
                "均价": p.get("avg_price"),
                "现价": p.get("cur_price"),
                "现值($)": p.get("current_value"),
                "成本($)": p.get("initial_value"),
                "盈亏($)": p.get("cash_pnl"),
                "盈亏%": p.get("pct_pnl"),
                "到期日": (p.get("end_date") or "")[:10],
                "Profile": f"{PROFILE_BASE}{addr}",
            })
        st.dataframe(
            pd.DataFrame(pos_data),
            column_config={
                "Profile": st.column_config.LinkColumn("Profile", display_text="查看"),
                "现值($)": st.column_config.NumberColumn(format="$%.2f"),
                "成本($)": st.column_config.NumberColumn(format="$%.2f"),
                "盈亏($)": st.column_config.NumberColumn(format="$%.2f"),
                "盈亏%": st.column_config.NumberColumn(format="%.1f%%"),
            },
            hide_index=True, use_container_width=True,
        )
    elif positions is not None:
        st.info("关注用户在 SPX/NDX 市场中暂无 Active 持仓。")
    else:
        st.info("点击上方「🔄 查询 / 刷新全部关注用户 SPX/NDX 持仓」查看持仓数据。")
else:
    st.info("关注列表为空。请先扫描并勾选地址后点击「添加到关注」。")
