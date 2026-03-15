"""
Polymarket SPX/NDX 本地扫描工具 — 无需 Streamlit，命令行直接运行

用法:
    python local_scan.py                    # 增量扫描，默认 7 天窗口
    python local_scan.py --window 30D       # 30 天窗口
    python local_scan.py --full             # 全量刷新（忽略缓存）
    python local_scan.py --min-trades 5     # 最少 5 笔交易
    python local_scan.py --positions        # 同时查询关注用户的持仓

所有结果保存到 data/ 目录，与 Streamlit 版本共享缓存。
"""
import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests

# ========== 常量 ==========

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
DATA_API_BASE = "https://data-api.polymarket.com"
DATA_API_TRADES_URL = f"{DATA_API_BASE}/trades"
DATA_API_HOLDERS_URL = f"{DATA_API_BASE}/holders"
DATA_API_POSITIONS_URL = f"{DATA_API_BASE}/positions"
PROFILE_BASE = "https://polymarket.com/profile/"
INDEX_TAG_IDS = ["102849", "102682"]

DATA_DIR = Path(__file__).parent / "data"
TRADES_CACHE_FILE = DATA_DIR / "trades_cache.json"
WATCHLIST_FILE = DATA_DIR / "watchlist.json"
CONDITION_IDS_FILE = DATA_DIR / "condition_ids.json"

DEFAULT_WEIGHTS = (0.25, 0.25, 0.20, 0.15, 0.15)


# ========== 缓存工具（与 streamlit_app.py 共享） ==========

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
    return (
        t.get("conditionId", ""),
        t.get("proxyWallet", ""),
        str(t.get("timestamp", "")),
        t.get("side", ""),
        str(t.get("size", "")),
        str(t.get("price", "")),
    )


def merge_trades(existing, new_trades):
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
    """加载关注列表，兼容旧格式 [addr, ...] 和新格式 [{address, ...}, ...]"""
    if WATCHLIST_FILE.exists():
        try:
            with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return []
        if not isinstance(data, list):
            return []
        if data and isinstance(data[0], str):
            return data
        return [item["address"] for item in data if isinstance(item, dict) and "address" in item]
    return []


def save_condition_ids(cids):
    _ensure_data_dir()
    with open(CONDITION_IDS_FILE, "w", encoding="utf-8") as f:
        json.dump(cids, f, ensure_ascii=False)


def load_cached_condition_ids():
    if CONDITION_IDS_FILE.exists():
        try:
            with open(CONDITION_IDS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return []


# ========== 核心逻辑 ==========

def _normalize_ts(ts):
    if not ts:
        return 0
    return int(ts) // 1000 if int(ts) > 1e12 else int(ts)


def _time_window_seconds(window_key):
    mapping = {"3D": 3, "7D": 7, "30D": 30, "ALL": 3650}
    days = mapping.get(window_key, 7)
    return int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())


def fetch_index_condition_ids():
    print("[1/4] 获取 SPX/NDX 市场列表...")
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
            except Exception as e:
                print(f"  警告: tag_id={tag_id} closed={closed_flag} 请求失败: {e}")
    result = list(out)
    if result:
        save_condition_ids(result)
    print(f"  找到 {len(result)} 个 SPX/NDX 市场")
    return result


def fetch_trades_by_market_raw(condition_ids, since_ts):
    all_trades = []
    total = len(condition_ids)
    for idx, cid in enumerate(condition_ids):
        print(f"\r  拉取市场 {idx+1}/{total}...", end="", flush=True)
        offset = 0
        page_size = 10000
        while True:
            try:
                r = requests.get(
                    DATA_API_TRADES_URL,
                    params={"market": cid, "limit": page_size, "offset": offset, "takerOnly": "false"},
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
    print()
    return all_trades


def fetch_trades_incremental(condition_ids, since_ts, force_full=False):
    cache = load_cached_trades()

    if force_full:
        print("[2/4] 全量拉取交易记录...")
        new_trades = fetch_trades_by_market_raw(condition_ids, since_ts)
        merged, added = merge_trades([], new_trades)
        cache_obj = {
            "trades": merged,
            "last_ts": max((_normalize_ts(t.get("timestamp") or t.get("timestampSeconds")) for t in merged), default=0),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        save_cached_trades(cache_obj)
        print(f"  全量拉取完成: {len(merged)} 条交易，已缓存")
        return merged, len(merged), 0

    cached_trades = cache.get("trades", [])
    cached_last_ts = cache.get("last_ts", 0)
    print(f"[2/4] 增量拉取（本地缓存 {len(cached_trades)} 条）...")

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
    print(f"  本地已有 {len(cached_trades)} 条 -> 新增 {added} 条 -> 窗口内 {len(filtered)} 条")
    return filtered, added, len(cached_trades)


def fetch_holders_by_market(condition_ids):
    holdings = defaultdict(float)
    for cid in condition_ids:
        try:
            r = requests.get(DATA_API_HOLDERS_URL, params={"market": cid, "limit": 500}, timeout=15)
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


def compute_trader_stats(trades_list):
    user_data = defaultdict(lambda: {
        "trade_count": 0, "volume": 0.0, "buy_volume": 0.0, "sell_volume": 0.0,
        "gross_profit": 0.0, "gross_loss": 0.0, "win_count": 0, "loss_count": 0,
        "markets": set(), "username": "", "pseudonym": "",
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
            if price < 0.50:
                u["win_count"] += 1
                u["gross_profit"] += size * (1.0 - price)
            else:
                u["loss_count"] += 1
                u["gross_loss"] += size * price
        elif side == "SELL":
            u["sell_volume"] += notional
            if price > 0.50:
                u["win_count"] += 1
                u["gross_profit"] += size * price
            else:
                u["loss_count"] += 1
                u["gross_loss"] += size * (1.0 - price)

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

    raw = consistency * w_c + returns_norm * w_r + wr_norm * w_w + max_loss_norm * w_m + pf_norm * w_p
    return round(min(100, max(0, raw * 100)), 1)


def build_traders_df(trader_stats, holder_data, weights):
    rows = []
    for addr, s in trader_stats.items():
        display_name = s.get("username") or s.get("pseudonym") or ""
        holding = holder_data.get(addr, 0)
        rows.append({
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
            "Profile": f"{PROFILE_BASE}{addr}",
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["Score"] = df.apply(lambda r: calculate_score(r, weights), axis=1)
    return df.sort_values("Score", ascending=False).reset_index(drop=True)


def fetch_watchlist_positions(watchlist, condition_ids):
    if not watchlist or not condition_ids:
        return pd.DataFrame()
    cid_set = set(condition_ids)
    rows = []
    total = len(watchlist)
    for i, addr in enumerate(watchlist):
        print(f"\r  查询持仓 {i+1}/{total}...", end="", flush=True)
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
            rows.append({
                "address": addr,
                "市场": p.get("title") or p.get("slug") or "",
                "方向": p.get("outcome", ""),
                "持仓量": round(size, 2),
                "均价": round(p.get("avgPrice", 0), 4),
                "现价": round(p.get("curPrice", 0), 4),
                "现值($)": round(p.get("currentValue", 0), 2),
                "成本($)": round(p.get("initialValue", 0), 2),
                "盈亏($)": round(p.get("cashPnl", 0), 2),
                "盈亏%": round(p.get("percentPnl", 0) * 100, 1) if p.get("percentPnl") else 0,
                "到期日": (p.get("endDate") or "")[:10],
                "Profile": f"{PROFILE_BASE}{addr}",
            })
    print()
    return pd.DataFrame(rows)


# ========== 主函数 ==========

def main():
    parser = argparse.ArgumentParser(description="Polymarket SPX/NDX 本地扫描工具")
    parser.add_argument("--window", default="7D", choices=["3D", "7D", "30D", "ALL"],
                        help="扫描时间窗口 (默认: 7D)")
    parser.add_argument("--full", action="store_true", help="全量刷新（忽略缓存）")
    parser.add_argument("--min-trades", type=int, default=3, help="最少交易笔数 (默认: 3)")
    parser.add_argument("--min-volume", type=float, default=100, help="最低交易量 $ (默认: 100)")
    parser.add_argument("--positions", action="store_true", help="同时查询关注用户的 SPX/NDX 持仓")
    args = parser.parse_args()

    window_label = {"3D": "3 天", "7D": "7 天", "30D": "30 天", "ALL": "全部"}.get(args.window, args.window)
    print(f"=== Polymarket SPX/NDX 扫描 ===")
    print(f"时间窗口: {window_label} | 最少交易: {args.min_trades} 笔 | 最低交易量: ${args.min_volume}")
    print(f"模式: {'全量刷新' if args.full else '增量更新'}")
    print()

    # Step 1
    cids = fetch_index_condition_ids()
    if not cids:
        print("错误: 未找到任何 SPX/NDX 市场")
        sys.exit(1)

    # Step 2
    since_ts = _time_window_seconds(args.window)
    all_trades, new_count, old_count = fetch_trades_incremental(cids, since_ts, force_full=args.full)

    if not all_trades:
        print("该时间窗口内无 SPX/NDX 交易记录。")
        sys.exit(0)

    # Step 3
    print("[3/4] 聚合交易者数据 & 查询持仓...")
    trader_stats = compute_trader_stats(all_trades)
    holder_data = fetch_holders_by_market(cids)

    total_traders = len(trader_stats)
    trader_stats = {
        addr: s for addr, s in trader_stats.items()
        if s["trade_count"] >= args.min_trades and s["volume"] >= args.min_volume
    }
    print(f"  发现 {total_traders} 个交易者，筛选后剩余 {len(trader_stats)} 人")

    if not trader_stats:
        print("没有满足筛选条件的交易者。")
        sys.exit(0)

    # Step 4
    print("[4/4] 构建排名 & 保存...")
    df = build_traders_df(trader_stats, holder_data, DEFAULT_WEIGHTS)

    _ensure_data_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    scan_path = DATA_DIR / f"scan_{ts}.xlsx"
    df.to_excel(scan_path, index=False, engine="openpyxl")
    print(f"\n  扫描结果已保存: {scan_path}")

    # 打印 Top 20
    print(f"\n{'='*80}")
    print(f"Top 20 SPX/NDX 交易者:")
    print(f"{'='*80}")
    display_cols = ["Score", "address", "用户名", "交易笔数", "交易量", "胜率", "盈亏比"]
    top = df.head(20)
    print(top[display_cols].to_string(index=False))

    # 关注用户持仓
    if args.positions:
        watchlist = load_watchlist()
        if not watchlist:
            print("\n关注列表为空，跳过持仓查询。")
        else:
            print(f"\n查询 {len(watchlist)} 个关注用户的 SPX/NDX 持仓...")
            pos_df = fetch_watchlist_positions(watchlist, cids)
            if pos_df.empty:
                print("关注用户在 SPX/NDX 市场中暂无 Active 持仓。")
            else:
                pos_path = DATA_DIR / f"positions_{ts}.xlsx"
                pos_df.to_excel(pos_path, index=False, engine="openpyxl")
                print(f"  持仓数据已保存: {pos_path}")
                print(f"\n关注用户持仓 ({len(pos_df)} 条):")
                print(pos_df.to_string(index=False))

    print(f"\n完成! 文件保存在: {DATA_DIR.resolve()}")


if __name__ == "__main__":
    main()
