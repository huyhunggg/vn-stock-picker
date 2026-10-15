#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VN-STOCK FULL MARKET QUANT SCREENER (HOSE + HNX + UPCOM ~ 1600+ MÃ)
- Quét đa luồng (Multi-threading 16 workers + Thread-local Session) siêu tốc
- Tách riêng Bảng Độc Lập: Mẫu hình VIC (Bắt đáy MA100 trong Uptrend lớn)
- Tích hợp 3 Bộ lọc Định lượng chuẩn RS (0-99), Vốn hóa, Buy Active, 1W/1M:
  1. 🟢 Bộ lọc mặc định: EARLY SIGNAL SCANNER
  2. 🟡 Bộ thứ hai: CONFIRMATION SCANNER
  3. 🔵 Bộ thứ ba: ACCUMULATION HUNTER
- Hiển thị trực tiếp % lợi nhuận mục tiêu trong cột TP (3ATR) và % rủi ro Cắt lỗ (2ATR)
"""

import os
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import requests
import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

VN_TZ = timezone(timedelta(hours=7))
_thread_local = threading.local()

# ==============================================================================
# 1. CẤU HÌNH QUÉT TOÀN THỊ TRƯỜNG
# ==============================================================================
SCAN_MODE = os.getenv("SCAN_MODE", "ALL")
DATA_SOURCE = os.getenv("VNSTOCK_SOURCE", "VCI")

MIN_VOL_MA20 = int(os.getenv("MIN_VOL_MA20", "100000"))
MIN_TRADING_VAL_BILLION = float(os.getenv("MIN_VAL_BIL", "2.0"))
MIN_PRICE_VND = float(os.getenv("MIN_PRICE_VND", "5.0"))
MIN_MARKET_CAP_BIL = float(os.getenv("MIN_MARKET_CAP_BIL", "1000.0"))  # Vốn hóa > 1.000 tỷ

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "16"))
MAX_DISPLAY_ROWS = int(os.getenv("MAX_DISPLAY_ROWS", "40"))


def get_thread_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
    return _thread_local.session


# ==============================================================================
# 2. LẤY DANH SÁCH MÃ & VỐN HÓA TOÀN THỊ TRƯỜNG (HOSE, HNX, UPCOM)
# ==============================================================================
def get_stock_universe() -> list[str]:
    symbols = set()

    # Nguồn 1: VNDIRECT Finfo API
    try:
        url = (
            "https://finfo-api.vndirect.com.vn/v4/stocks"
            "?q=type:stock,ifc~floor:HOSE,HNX,UPCOM~status:listed&size=3000"
        )
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        if resp.status_code == 200:
            for item in resp.json().get("data", []):
                code = str(item.get("code", "")).strip().upper()
                if len(code) == 3 and code.isalpha():
                    symbols.add(code)
            logger.info(f"Đã tải {len(symbols)} mã từ nguồn VNDIRECT Finfo API.")
    except Exception as e:
        logger.debug(f"Lỗi nguồn VNDIRECT Finfo: {e}")

    # Nguồn 2: Entrade / DNSE
    if len(symbols) < 500:
        try:
            url = "https://services.entrade.com.vn/chart-api/symbols?type=stock"
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
            if resp.status_code == 200:
                for s in resp.json().get("symbols", []):
                    code = str(s).strip().upper()
                    if len(code) == 3 and code.isalpha():
                        symbols.add(code)
                logger.info(f"Tổng số mã sau khi gộp nguồn Entrade: {len(symbols)} mã.")
        except Exception as e:
            logger.debug(f"Lỗi nguồn Entrade symbols: {e}")

    # Nguồn 3: Vnstock Listing
    if len(symbols) < 500:
        try:
            from vnstock import Listing
            listing = Listing(source=DATA_SOURCE) if "source" in Listing.__init__.__code__.co_varnames else Listing()
            df_all = listing.all_symbols()
            if isinstance(df_all, pd.DataFrame) and "symbol" in df_all.columns:
                for s in df_all["symbol"].dropna().unique():
                    code = str(s).strip().upper()
                    if len(code) == 3 and code.isalpha():
                        symbols.add(code)
        except Exception as e:
            logger.debug(f"Lỗi nguồn Vnstock Listing: {e}")

    return sorted(list(symbols))


def get_market_cap_map() -> dict[str, float]:
    """
    Lấy dữ liệu Vốn hóa thị trường (đơn vị: Tỷ VNĐ) cho toàn bộ cổ phiếu
    chỉ bằng 1 request tổng từ VNDIRECT Finfo Ratios (itemCode: 51003 = Market Cap).
    """
    mcap_map: dict[str, float] = {}
    min_date = (datetime.now(VN_TZ) - timedelta(days=45)).strftime("%Y-%m-%d")

    try:
        url = (
            f"https://finfo-api.vndirect.com.vn/v4/ratios/latest"
            f"?filter=itemCode:51003&where=reportDate:gt:{min_date}&page=1&size=3000"
        )
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        if resp.status_code == 200:
            for item in resp.json().get("data", []):
                code = str(item.get("code", "")).strip().upper()
                val = item.get("value")
                if code and val is not None:
                    # Đổi từ VNĐ sang Tỷ VNĐ
                    mcap_map[code] = float(val) / 1_000_000_000.0
            logger.info(f"Đã tải dữ liệu Vốn hóa cho {len(mcap_map)} mã cổ phiếu.")
    except Exception as e:
        logger.debug(f"Lỗi tải Vốn hóa từ VNDIRECT Ratios: {e}")

    return mcap_map


# ==============================================================================
# 3. KẾT NỐI DỮ LIỆU LỊCH SỬ OHLCV TỐC ĐỘ CAO
# ==============================================================================
def normalize_price_scale(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    median_close = df["close"].median()
    if median_close > 1000:
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df[col] = df[col] / 1000.0
    return df


def fetch_historical_data(symbol: str, days: int = 260, session: requests.Session | None = None) -> pd.DataFrame:
    end_dt = datetime.now(VN_TZ)
    start_dt = end_dt - timedelta(days=days)
    from_ts = int(start_dt.timestamp())
    to_ts = int(end_dt.timestamp())

    req_client = session if session is not None else requests

    # Cách 1: API Chart Entrade/DNSE
    try:
        idx_type = "index" if symbol.upper() in ["VNINDEX", "VN30", "HNXINDEX"] else "stock"
        url = (
            f"https://services.entrade.com.vn/chart-api/v2/ohlcs/{idx_type}"
            f"?from={from_ts}&to={to_ts}&symbol={symbol}&resolution=1D"
        )
        resp = req_client.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            if data and "t" in data and len(data["t"]) > 0:
                df = pd.DataFrame({
                    "time": pd.to_datetime(data["t"], unit="s"),
                    "open": pd.to_numeric(data["o"], errors="coerce"),
                    "high": pd.to_numeric(data["h"], errors="coerce"),
                    "low": pd.to_numeric(data["l"], errors="coerce"),
                    "close": pd.to_numeric(data["c"], errors="coerce"),
                    "volume": pd.to_numeric(data["v"], errors="coerce"),
                })
                return normalize_price_scale(df)
    except Exception:
        pass

    # Cách 2: API Dchart VNDIRECT
    try:
        url = (
            f"https://dchart-api.vndirect.com.vn/dchart/history"
            f"?resolution=D&symbol={symbol}&from={from_ts}&to={to_ts}"
        )
        resp = req_client.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            if data and data.get("s") == "ok" and len(data.get("t", [])) > 0:
                df = pd.DataFrame({
                    "time": pd.to_datetime(data["t"], unit="s"),
                    "open": pd.to_numeric(data["o"], errors="coerce"),
                    "high": pd.to_numeric(data["h"], errors="coerce"),
                    "low": pd.to_numeric(data["l"], errors="coerce"),
                    "close": pd.to_numeric(data["c"], errors="coerce"),
                    "volume": pd.to_numeric(data["v"], errors="coerce"),
                })
                return normalize_price_scale(df)
    except Exception:
        pass

    return pd.DataFrame()


# ==============================================================================
# 4. TÍNH TOÁN CHỈ BÁO KỸ THUẬT & ĐỊNH LƯỢNG (BUY ACTIVE, 1W, 1M, ATR, MA100)
# ==============================================================================
def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("time").reset_index(drop=True)

    df["MA20"] = df["close"].rolling(window=20).mean()
    df["MA50"] = df["close"].rolling(window=50).mean()
    df["MA100"] = df["close"].rolling(window=100).mean()
    df["MA200"] = df["close"].rolling(window=200).mean()

    df["Vol_MA20"] = df["volume"].rolling(window=20).mean()
    df["R_Vol"] = df["volume"] / df["Vol_MA20"].replace(0, np.nan)

    # Hiệu suất 1 tuần (5 phiên), 1 tháng (20 phiên), 3 tháng (60 phiên)
    df["Return_5d"] = df["close"].pct_change(periods=5) * 100.0
    df["Return_20d"] = df["close"].pct_change(periods=20) * 100.0
    df["Return_60d"] = df["close"].pct_change(periods=60) * 100.0

    high_5d = df["high"].rolling(window=5).max()
    low_5d = df["low"].rolling(window=5).min()
    df["Range_5d_Pct"] = ((high_5d - low_5d) / low_5d.replace(0, np.nan)) * 100.0

    # Tính tỷ lệ Mua Chủ Động ước lượng (Buy Active %) từ hành vi Giá - Khối lượng (BVC)
    candle_spread = (df["high"] - df["low"]).replace(0, np.nan)
    close_loc = ((df["close"] - df["low"]) / candle_spread).fillna(0.5)
    body_bias = np.where(df["close"] >= df["open"], 0.06, -0.04)
    active_buy_ratio_1d = np.clip(close_loc * 0.85 + 0.075 + body_bias, 0.05, 0.95)
    active_buy_vol = df["volume"] * active_buy_ratio_1d
    # Trung bình trọng số giữa phiên hiện tại (60%) và 5 phiên gần nhất (40%)
    buy_active_5d = (
        active_buy_vol.rolling(window=5).sum()
        / df["volume"].rolling(window=5).sum().replace(0, np.nan)
    ) * 100.0
    df["Buy_Active_Pct"] = (active_buy_ratio_1d * 100.0 * 0.60) + (buy_active_5d.fillna(50.0) * 0.40)

    # RSI(14) chuẩn Wilder
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["RSI14"] = 100.0 - (100.0 / (1.0 + rs))

    # MACD (12, 26, 9)
    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_Hist"] = df["MACD"] - df["MACD_Signal"]

    # Bollinger Bands (20, 2)
    std20 = df["close"].rolling(window=20).std()
    df["BB_Upper"] = df["MA20"] + 2.0 * std20
    df["BB_Lower"] = df["MA20"] - 2.0 * std20
    df["BB_Width"] = ((df["BB_Upper"] - df["BB_Lower"]) / df["MA20"].replace(0, np.nan)) * 100.0

    # ATR(14)
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift(1)).abs()
    low_close = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df["ATR14"] = tr.rolling(window=14).mean()

    return df


# ==============================================================================
# 5. TRÍCH XUẤT METRICS CỔ PHIẾU & PHÂN LOẠI ĐA TẦNG
# ==============================================================================
def extract_stock_snapshot(symbol: str, df: pd.DataFrame, mcap_map: dict[str, float]) -> dict | None:
    if df is None or len(df) < 105:
        return None

    df = calculate_indicators(df)
    last = df.iloc[-1]
    prev = df.iloc[-2]
    prev2 = df.iloc[-3]

    if np.isnan(last["close"]) or np.isnan(last["Vol_MA20"]) or np.isnan(last["MA100"]):
        return None

    close_price = float(last["close"])
    vol_ma20 = float(last["Vol_MA20"])
    avg_trading_val_bil = float((close_price * vol_ma20) / 1_000_000.0)

    # Lọc cơ bản Tầng 1
    if close_price < MIN_PRICE_VND:
        return None
    if vol_ma20 < MIN_VOL_MA20 and avg_trading_val_bil < MIN_TRADING_VAL_BILLION:
        return None

    # Vốn hóa (Tỷ VNĐ): Ưu tiên từ API VNDIRECT, nếu khuyết thì ước lượng bảo수 từ thanh khoản
    if symbol in mcap_map and mcap_map[symbol] > 0:
        market_cap_bil = float(mcap_map[symbol])
    else:
        # Ước lượng bảo thủ: GTGD TB 20 phiên thường chiếm ~0.5% - 1% vốn hóa
        market_cap_bil = round(avg_trading_val_bil * 120.0, 1)

    ret_1w = float(last["Return_5d"]) if not np.isnan(last["Return_5d"]) else 0.0
    ret_1m = float(last["Return_20d"]) if not np.isnan(last["Return_20d"]) else 0.0
    ret_3m = float(last["Return_60d"]) if not np.isnan(last["Return_60d"]) else ret_1m

    # Điểm sức mạnh tổng hợp để xếp hạng RS (Percentile 1 - 99 toàn thị trường)
    rs_raw_composite = (0.50 * ret_1m) + (0.30 * ret_3m) + (0.20 * ret_1w)

    candle_range = max(float(last["high"] - last["low"]), 0.001)
    close_pos = float((close_price - float(last["low"])) / candle_range)
    daily_change_pct = float(((close_price - float(prev["close"])) / float(prev["close"])) * 100.0)
    dist_to_ma100_pct = float(((close_price - float(last["MA100"])) / float(last["MA100"])) * 100.0)

    ma50_slope_up = float(last["MA50"]) >= float(df["MA50"].iloc[-5])
    ma100_slope_up = float(last["MA100"]) >= float(df["MA100"].iloc[-10]) * 0.998

    # Kiểm tra Mẫu hình VIC (Bắt đáy MA100 trong Uptrend lớn)
    macro_uptrend = ma100_slope_up and (float(last["MA50"]) >= float(last["MA100"]) * 0.97)
    near_ma100 = (-4.0 <= dist_to_ma100_pct <= 5.0) or (
        float(df["low"].iloc[-3:].min()) <= float(last["MA100"]) * 1.025
        and close_price >= float(last["MA100"]) * 0.965
    )
    min_rsi_3d = float(df["RSI14"].iloc[-3:].min())
    rsi_bottoming = (26.0 <= min_rsi_3d <= 46.0) and (float(last["RSI14"]) >= float(prev["RSI14"]) - 1.5)
    macd_recovering = (
        (float(last["MACD_Hist"]) > float(prev["MACD_Hist"]))
        or (
            float(prev["MACD_Hist"]) > float(prev2["MACD_Hist"])
            and float(last["MACD_Hist"]) >= float(prev["MACD_Hist"]) * 0.92
        )
        or (float(last["MACD"]) > float(last["MACD_Signal"]) and float(prev["MACD"]) <= float(prev["MACD_Signal"]))
    )
    price_stabilizing = (close_price >= float(last["open"]) or close_pos >= 0.38) and (float(last["R_Vol"]) <= 1.45)
    is_vic_ma100 = macro_uptrend and near_ma100 and rsi_bottoming and macd_recovering and price_stabilizing

    # Kiểm tra tín hiệu kỹ thuật bổ trợ
    is_breakout = (
        close_price > float(last["MA20"])
        and close_price > float(last["MA50"])
        and float(last["R_Vol"]) >= 1.40
        and daily_change_pct >= 2.2
        and close_pos >= 0.65
    )
    is_tight_base = (float(last["BB_Width"]) <= 15.0 or float(last["Range_5d_Pct"]) <= 6.0) and float(last["R_Vol"]) <= 0.95

    dist_from_ma20_pct = ((close_price - float(last["MA20"])) / float(last["MA20"])) * 100.0
    is_sell_warning = (
        (close_price < float(last["MA20"]) and float(prev["close"]) >= float(prev["MA20"]) and float(last["R_Vol"]) >= 1.30)
        or float(last["RSI14"]) >= 79.0
        or dist_from_ma20_pct >= 16.0
    )

    # Quản trị rủi ro ATR + Tính % TP và % SL
    atr_val = float(last["ATR14"]) if not np.isnan(last["ATR14"]) else float(close_price * 0.03)
    stop_loss = round(close_price - 2.0 * atr_val, 2)
    target_price = round(close_price + 3.0 * atr_val, 2)
    sl_pct = round(((stop_loss - close_price) / close_price) * 100.0, 1)
    tp_pct = round(((target_price - close_price) / close_price) * 100.0, 1)

    # Chấm điểm riêng cho Mẫu hình VIC MA100
    vic_proximity_pts = float(np.clip(35.0 - abs(dist_to_ma100_pct) * 5.5, 10.0, 35.0))
    vic_trend_pts = 30.0 if (float(last["MA50"]) > float(last["MA100"]) and ma100_slope_up) else 20.0
    vic_rebound_pts = (18.0 if float(last["MACD_Hist"]) > float(prev["MACD_Hist"]) else 10.0) + float(
        np.clip(close_pos * 17.0, 5.0, 17.0)
    )
    vic_score = round(vic_proximity_pts + vic_trend_pts + vic_rebound_pts, 1)

    return {
        "symbol": symbol,
        "close": round(close_price, 2),
        "daily_change_pct": round(daily_change_pct, 2),
        "vol_ma20": int(round(vol_ma20)),
        "r_vol": round(float(last["R_Vol"]), 2),
        "avg_val_bil": round(avg_trading_val_bil, 1),
        "market_cap_bil": round(market_cap_bil, 0),
        "ret_1w": round(ret_1w, 2),
        "ret_1m": round(ret_1m, 2),
        "rs_raw": rs_raw_composite,
        "buy_active_pct": round(float(last["Buy_Active_Pct"]), 1),
        "ma20": round(float(last["MA20"]), 2),
        "ma50": round(float(last["MA50"]), 2),
        "ma100": round(float(last["MA100"]), 2),
        "dist_ma100_pct": round(dist_to_ma100_pct, 2),
        "rsi14": round(float(last["RSI14"]), 1),
        "macd_hist": round(float(last["MACD_Hist"]), 2),
        "stop_loss": stop_loss,
        "target_price": target_price,
        "sl_pct": sl_pct,
        "tp_pct": tp_pct,
        "sl_display": f"{stop_loss:.2f} ({sl_pct:+.1f}%)",
        "tp_display": f"{target_price:.2f} (+{tp_pct:.1f}%)",
        "is_vic_ma100": is_vic_ma100,
        "vic_score": vic_score,
        "is_breakout": is_breakout,
        "is_tight_base": is_tight_base,
        "is_sell_warning": is_sell_warning,
    }


def build_vic_table_row(item: dict) -> dict:
    """Định dạng dòng riêng biệt cho Bảng Mẫu hình VIC (Bắt đáy MA100)."""
    return {
        "Mã": item["symbol"],
        "Tín hiệu": "BẮT ĐÁY MA100 (MẪU HÌNH VIC)",
        "Điểm VIC": item["vic_score"],
        "Giá": item["close"],
        "% Phiên": item["daily_change_pct"],
        "MA100": item["ma100"],
        "Cách MA100 (%)": item["dist_ma100_pct"],
        "RSI(14)": item["rsi14"],
        "MACD Hist": item["macd_hist"],
        "Vol/MA20": item["r_vol"],
        "Vốn hóa (Tỷ)": int(item["market_cap_bil"]),
        "Cắt lỗ (2ATR)": item["sl_display"],
        "Mục tiêu TP (3ATR)": item["tp_display"],
        "TP (%)": item["tp_pct"],
    }


def build_quant_scanner_row(item: dict, scanner_name: str, signal_note: str) -> dict:
    """Định dạng dòng cho 3 Bộ lọc Định lượng (Early Signal, Confirmation, Accumulation)."""
    return {
        "Mã": item["symbol"],
        "Bộ lọc": scanner_name,
        "Tín hiệu": signal_note,
        "RS": int(item["rs_rating"]),
        "Giá": item["close"],
        "% Phiên": item["daily_change_pct"],
        "1 Tuần (%)": item["ret_1w"],
        "1 Tháng (%)": item["ret_1m"],
        "Buy Active (%)": item["buy_active_pct"],
        "KLTB 20P": f"{item['vol_ma20']:,}",
        "Vốn hóa (Tỷ)": int(item["market_cap_bil"]),
        "Cắt lỗ (2ATR)": item["sl_display"],
        "Mục tiêu TP (3ATR)": item["tp_display"],
        "TP (%)": item["tp_pct"],
    }


# ==============================================================================
# 6. HIỂN THỊ BÁO CÁO TRÊN GITHUB ACTIONS SUMMARY
# ==============================================================================
def write_github_summary(
    vnindex_info: dict,
    vic_ma100_list: list[dict],
    vic_stock_status: dict | None,
    early_signal_list: list[dict],
    confirmation_list: list[dict],
    accumulation_list: list[dict],
    sell_list: list[dict],
    total_universe: int,
    total_scanned: int,
    elapsed_sec: float,
):
    now_str = datetime.now(VN_TZ).strftime("%H:%M - %d/%m/%Y")
    vni_close = vnindex_info.get("close", 0.0)
    vni_chg = vnindex_info.get("change_pct", 0.0)
    vni_ret20 = vnindex_info.get("ret_20d", 0.0)

    lines = [
        f"## 📊 Báo Cáo Quét Định Lượng Toàn Thị Trường ({now_str})",
        f"- **Tổng số mã đã quét:** `{total_scanned} / {total_universe}` mã (Thời gian chạy: `{elapsed_sec:.1f}s`)",
        f"- **VN-INDEX:** `{vni_close:,.2f}` (`{vni_chg:+.2f}%`) | **Hiệu suất 20 phiên:** `{vni_ret20:+.2f}%`",
        "",
        "---",
        f"## 🎯 BẢNG RIÊNG: MẪU HÌNH VIC - BẮT ĐÁY MA100 ({len(vic_ma100_list)} mã)",
        "> **Tiêu chí độc lập:** Uptrend trung hạn (`MA50 >= MA100`, `MA100` dốc lên), giá chỉnh về sát `MA100` (`-4%` đến `+5%`), `RSI(14)` vùng đáy `26–46` bắt đầu hồi phục kèm `MACD Histogram` thu hẹp đà giảm.",
    ]

    if vic_stock_status:
        lines.append(
            f"- **📌 Trạng thái cổ phiếu mẫu `VIC` hiện tại:** Giá `{vic_stock_status['close']}` "
            f"(`{vic_stock_status['daily_change_pct']:+.2f}%`) | MA100: `{vic_stock_status['ma100']}` "
            f"(Cách MA100: `{vic_stock_status['dist_ma100_pct']:+.2f}%`) | RSI(14): `{vic_stock_status['rsi14']}` "
            f"| Cắt lỗ: `{vic_stock_status['sl_display']}` | **TP (3ATR): `{vic_stock_status['tp_display']}`**"
        )
        lines.append("")

    if vic_ma100_list:
        df_vic = pd.DataFrame(vic_ma100_list).drop(columns=["TP (%)"])
        lines.append(df_vic.head(MAX_DISPLAY_ROWS).to_markdown(index=False))
    else:
        lines.append("_Hiện chưa có cổ phiếu nào chạm vùng bắt đáy MA100 theo mẫu hình VIC._")

    lines.extend([
        "",
        "---",
        f"## 🟡 BỘ LỌC 2: CONFIRMATION SCANNER ({len(confirmation_list)} mã)",
        "> **Tiêu chí:** `RS > 85` | `1 tháng > 10%` | `3% < 1 tuần < 12%` | `Buy Active > 52%` | `KLTB 20 phiên > 200.000` | `Vốn hóa > 1.000 tỷ`",
    ])
    if confirmation_list:
        df_conf = pd.DataFrame(confirmation_list).drop(columns=["Bộ lọc", "TP (%)"])
        lines.append(df_conf.head(MAX_DISPLAY_ROWS).to_markdown(index=False))
    else:
        lines.append("_Chưa có mã thỏa mãn đồng thời các tiêu chí Confirmation Scanner._")

    lines.extend([
        "",
        "---",
        f"## 🟢 BỘ LỌC 1 (MẶC ĐỊNH): EARLY SIGNAL SCANNER ({len(early_signal_list)} mã)",
        "> **Tiêu chí:** `RS > 75` | `1 tháng > 5%` | `-3% < 1 tuần < 8%` | `Buy Active > 51%` | `KLTB 20 phiên > 100.000` | `Vốn hóa > 1.000 tỷ`",
    ])
    if early_signal_list:
        df_early = pd.DataFrame(early_signal_list).drop(columns=["Bộ lọc", "TP (%)"])
        lines.append(df_early.head(MAX_DISPLAY_ROWS).to_markdown(index=False))
    else:
        lines.append("_Chưa có mã thỏa mãn đồng thời các tiêu chí Early Signal Scanner._")

    lines.extend([
        "",
        "---",
        f"## 🔵 BỘ LỌC 3: ACCUMULATION HUNTER ({len(accumulation_list)} mã)",
        "> **Tiêu chí:** `RS > 70` | `1 tháng > 0%` | `-5% < 1 tuần < 5%` | `Buy Active > 50%` | `KLTB 20 phiên > 100.000` | `Vốn hóa > 1.000 tỷ`",
    ])
    if accumulation_list:
        df_acc = pd.DataFrame(accumulation_list).drop(columns=["Bộ lọc", "TP (%)"])
        lines.append(df_acc.head(MAX_DISPLAY_ROWS).to_markdown(index=False))
    else:
        lines.append("_Chưa có mã thỏa mãn đồng thời các tiêu chí Accumulation Hunter._")

    lines.extend([
        "",
        "---",
        f"## ⚠️ CẢNH BÁO CHỐT LỜI / GÃY NỀN MA20 ({len(sell_list)} mã)",
    ])
    if sell_list:
        df_sell = pd.DataFrame(sell_list).drop(columns=["Bộ lọc", "TP (%)"])
        lines.append(df_sell.head(25).to_markdown(index=False))
    else:
        lines.append("_Không có cảnh báo bán đột biến._")

    summary_text = chr(10).join(lines)
    print(summary_text)

    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(summary_text + chr(10))


# ==============================================================================
# 7. HÀM CHẠY CHÍNH (QUÉT ĐA LUỒNG + XẾP HẠNG RS TOÀN THỊ TRƯỜNG)
# ==============================================================================
def fetch_and_extract_worker(symbol: str, mcap_map: dict[str, float]) -> tuple[bool, dict | None]:
    session = get_thread_session()
    df = fetch_historical_data(symbol, days=260, session=session)
    if df.empty:
        return False, None
    snap = extract_stock_snapshot(symbol, df, mcap_map)
    return True, snap


def run_scanner():
    start_time = time.time()
    logger.info("Khởi chạy Hệ thống Quét Định lượng Đa tầng (HOSE + HNX + UPCOM)...")

    df_vni = fetch_historical_data("VNINDEX", days=120)
    if not df_vni.empty and len(df_vni) >= 21:
        vni_last = df_vni.iloc[-1]
        vni_prev = df_vni.iloc[-2]
        vni_20d_ago = df_vni.iloc[-21]
        vnindex_ret_20d = float(((vni_last["close"] - vni_20d_ago["close"]) / vni_20d_ago["close"]) * 100.0)
        vnindex_info = {
            "close": float(vni_last["close"]) * (1000.0 if vni_last["close"] < 5.0 else 1.0),
            "change_pct": float(((vni_last["close"] - vni_prev["close"]) / vni_prev["close"]) * 100.0),
            "ret_20d": vnindex_ret_20d,
        }
    else:
        vnindex_info = {"close": 0.0, "change_pct": 0.0, "ret_20d": 0.0}

    symbols = get_stock_universe()
    mcap_map = get_market_cap_map()
    total_universe = len(symbols)
    logger.info(f"Đang quét {total_universe} mã với {MAX_WORKERS} workers song song...")

    snapshots: list[dict] = []
    scanned_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_sym = {
            executor.submit(fetch_and_extract_worker, sym, mcap_map): sym
            for sym in symbols
        }
        for future in as_completed(future_to_sym):
            try:
                has_data, snap = future.result()
                if has_data:
                    scanned_count += 1
                if snap is not None:
                    snapshots.append(snap)
            except Exception:
                pass

    # Tính điểm RS (Relative Strength Percentile 1 - 99) chuẩn trên toàn bộ thị trường
    if snapshots:
        raw_scores = pd.Series([s["rs_raw"] for s in snapshots])
        rs_percentiles = (raw_scores.rank(pct=True) * 99.0).round().clip(1, 99).astype(int).tolist()
        for snap, rs_val in zip(snapshots, rs_percentiles):
            snap["rs_rating"] = rs_val

    vic_ma100_list: list[dict] = []
    early_signal_list: list[dict] = []
    confirmation_list: list[dict] = []
    accumulation_list: list[dict] = []
    sell_list: list[dict] = []
    vic_stock_status: dict | None = None

    for item in snapshots:
        if item["symbol"] == "VIC":
            vic_stock_status = item

        # 1. BẢNG RIÊNG ĐỘC LẬP: Mẫu hình VIC (Bắt đáy MA100)
        if item["is_vic_ma100"]:
            vic_ma100_list.append(build_vic_table_row(item))

        # Điều kiện Vốn hóa > 1.000 tỷ áp dụng chung cho 3 Bộ lọc RS
        has_mcap_1000b = item["market_cap_bil"] > MIN_MARKET_CAP_BIL
        rs = item["rs_rating"]
        r1m = item["ret_1m"]
        r1w = item["ret_1w"]
        buy_act = item["buy_active_pct"]
        vol20 = item["vol_ma20"]

        # 2. 🟡 BỘ THỨ HAI: CONFIRMATION SCANNER
        # RS > 85 | 1 tháng > 10% | 3% < 1 tuần < 12% | Buy Active > 52% | KLTB 20P > 200.000 | Vốn hóa > 1.000 tỷ
        if (
            has_mcap_1000b
            and rs > 85
            and r1m > 10.0
            and (3.0 < r1w < 12.0)
            and buy_act > 52.0
            and vol20 > 200_000
        ):
            sig = "CONFIRMATION + BREAKOUT" if item["is_breakout"] else "CONFIRMATION SCANNER"
            confirmation_list.append(build_quant_scanner_row(item, "CONFIRMATION", sig))

        # 3. 🟢 BỘ LỌC MẶC ĐỊNH: EARLY SIGNAL SCANNER
        # RS > 75 | 1 tháng > 5% | -3% < 1 tuần < 8% | Buy Active > 51% | KLTB 20P > 100.000 | Vốn hóa > 1.000 tỷ
        if (
            has_mcap_1000b
            and rs > 75
            and r1m > 5.0
            and (-3.0 < r1w < 8.0)
            and buy_act > 51.0
            and vol20 > 100_000
        ):
            sig = "EARLY SIGNAL (BREAKOUT)" if item["is_breakout"] else "EARLY SIGNAL SCANNER"
            early_signal_list.append(build_quant_scanner_row(item, "EARLY_SIGNAL", sig))

        # 4. 🔵 BỘ THỨ BA: ACCUMULATION HUNTER
        # RS > 70 | 1 tháng > 0% | -5% < 1 tuần < 5% | Buy Active > 50% | KLTB 20P > 100.000 | Vốn hóa > 1.000 tỷ
        if (
            has_mcap_1000b
            and rs > 70
            and r1m > 0.0
            and (-5.0 < r1w < 5.0)
            and buy_act > 50.0
            and vol20 > 100_000
        ):
            sig = "ACCUMULATION (SIẾT NỀN)" if item["is_tight_base"] else "ACCUMULATION HUNTER"
            accumulation_list.append(build_quant_scanner_row(item, "ACCUMULATION", sig))

        # 5. ⚠️ CẢNH BÁO BÁN / CHỐT LỜI
        if item["is_sell_warning"] and has_mcap_1000b:
            sig = "CẮT LỖ / GÃY MA20" if item["close"] < item["ma20"] else "CHỐT LỜI (QUÁ MUA)"
            sell_list.append(build_quant_scanner_row(item, "SELL_WARNING", sig))

    # Sắp xếp thứ tự ưu tiên hiển thị
    vic_ma100_list.sort(key=lambda x: (x["Điểm VIC"], -abs(x["Cách MA100 (%)"])), reverse=True)
    confirmation_list.sort(key=lambda x: (x["RS"], x["Buy Active (%)"], x["1 Tháng (%)"]), reverse=True)
    early_signal_list.sort(key=lambda x: (x["RS"], x["Buy Active (%)"], x["1 Tháng (%)"]), reverse=True)
    accumulation_list.sort(key=lambda x: (x["RS"], x["Buy Active (%)"]), reverse=True)
    sell_list.sort(key=lambda x: x["RS"], reverse=True)

    # Xuất file CSV riêng cho bảng Mẫu hình VIC và file CSV tổng hợp cho 3 bộ lọc RS
    if vic_ma100_list:
        pd.DataFrame(vic_ma100_list).to_csv("scan_vic_ma100_latest.csv", index=False, encoding="utf-8-sig")

    quant_results = confirmation_list + early_signal_list + accumulation_list + sell_list
    if quant_results:
        pd.DataFrame(quant_results).to_csv("scan_results_latest.csv", index=False, encoding="utf-8-sig")

    elapsed = time.time() - start_time
    write_github_summary(
        vnindex_info=vnindex_info,
        vic_ma100_list=vic_ma100_list,
        vic_stock_status=vic_stock_status,
        early_signal_list=early_signal_list,
        confirmation_list=confirmation_list,
        accumulation_list=accumulation_list,
        sell_list=sell_list,
        total_universe=total_universe,
        total_scanned=scanned_count,
        elapsed_sec=elapsed,
    )
    logger.info(f"Hoàn tất quét {scanned_count}/{total_universe} mã trong {elapsed:.1f} giây!")


if __name__ == "__main__":
    run_scanner()
