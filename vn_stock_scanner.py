#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VN-STOCK FULL MARKET QUANT SCREENER (HOSE + HNX + UPCOM ~ 1600+ MÃ)
- Quét đa luồng (Multi-threading 16 workers) toàn bộ thị trường trong ~45 giây
- Tự động lấy danh sách toàn bộ cổ phiếu 3 sàn không phụ thuộc API Key
- Hiển thị đầy đủ +/- điểm và +/- % cho cả VN-INDEX và từng mã cổ phiếu
- Tích hợp ngầm 3 phễu lọc (Early Signal, Confirmation, Accumulation Hunter)
  để xuất ra đúng 4 bảng khuyến nghị hành động:
  1. 🚀 Top Mã Mạnh (Bùng nổ dòng tiền - Mua Breakout)
  2. 💎 Top Mã Tiềm Năng (Siết nền cạn cung - Mua Gom)
  3. ⚠️ Cảnh Báo Chốt Lời / Gãy Nền MA20
  4. 🎯 Bảng Riêng: Bắt đáy MA100 (Mẫu hình VIC)
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
# 1. CẤU HÌNH QUÉT TOÀN THỊ TRƯỜNG (MẶC ĐỊNH QUÉT ALL 3 SÀN)
# ==============================================================================
SCAN_MODE = os.getenv("SCAN_MODE", "ALL")
DATA_SOURCE = os.getenv("VNSTOCK_SOURCE", "VCI")

MIN_VOL_MA20 = int(os.getenv("MIN_VOL_MA20", "100000"))          # KLTB 20 phiên > 100.000 cp
MIN_TRADING_VAL_BILLION = float(os.getenv("MIN_VAL_BIL", "2.0")) # Hoặc GTGD >= 2 tỷ VNĐ/phiên
MIN_PRICE_VND = float(os.getenv("MIN_PRICE_VND", "5.0"))         # Thị giá >= 5,000 VNĐ
MIN_MARKET_CAP_BIL = float(os.getenv("MIN_MCAP_BIL", "1000.0"))  # Vốn hóa > 1.000 tỷ VNĐ

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "16"))
MAX_DISPLAY_ROWS = int(os.getenv("MAX_DISPLAY_ROWS", "40"))


def get_thread_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
    return _thread_local.session


# ==============================================================================
# 2. LẤY DANH SÁCH TOÀN BỘ MÃ CỔ PHIẾU 3 SÀN (HOSE, HNX, UPCOM) & VỐN HÓA
# ==============================================================================
def get_stock_universe() -> list[str]:
    symbols = set()

    # Nguồn 1: API Danh mục toàn thị trường của VNDIRECT Finfo
    try:
        url = (
            "https://finfo-api.vndirect.com.vn/v4/stocks"
            "?q=type:stock,ifc~floor:HOSE,HNX,UPCOM~status:listed&size=3000"
        )
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            for item in data:
                code = str(item.get("code", "")).strip().upper()
                if len(code) == 3 and code.isalpha():
                    symbols.add(code)
            logger.info(f"Đã tải {len(symbols)} mã từ nguồn VNDIRECT Finfo API.")
    except Exception as e:
        logger.debug(f"Lỗi nguồn VNDIRECT Finfo: {e}")

    # Nguồn 2: API Danh sách mã của Entrade / DNSE
    if len(symbols) < 500:
        try:
            url = "https://services.entrade.com.vn/chart-api/symbols?type=stock"
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
            if resp.status_code == 200:
                syms = resp.json().get("symbols", [])
                for s in syms:
                    code = str(s).strip().upper()
                    if len(code) == 3 and code.isalpha():
                        symbols.add(code)
                logger.info(f"Tổng số mã sau khi gộp nguồn Entrade: {len(symbols)} mã.")
        except Exception as e:
            logger.debug(f"Lỗi nguồn Entrade symbols: {e}")

    # Nguồn 3: Thư viện vnstock Listing
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
                logger.info(f"Tổng số mã sau khi gộp nguồn Vnstock: {len(symbols)} mã.")
        except Exception as e:
            logger.debug(f"Lỗi nguồn Vnstock Listing: {e}")

    return sorted(list(symbols))


def get_market_cap_map() -> dict[str, float]:
    """Lấy Vốn hóa thị trường (Tỷ VNĐ) từ VNDIRECT Finfo Ratios (itemCode: 51003)."""
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
                    mcap_map[code] = float(val) / 1_000_000_000.0
            logger.info(f"Đã tải dữ liệu Vốn hóa cho {len(mcap_map)} mã.")
    except Exception as e:
        logger.debug(f"Lỗi tải Vốn hóa: {e}")
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

    # Cách 2: API Dchart của VNDIRECT
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
# 4. TÍNH TOÁN CHỈ BÁO & BỘ LỌC ĐỊNH LƯỢNG ĐA TẦNG
# ==============================================================================
def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("time").reset_index(drop=True)

    df["MA20"] = df["close"].rolling(window=20).mean()
    df["MA50"] = df["close"].rolling(window=50).mean()
    df["MA100"] = df["close"].rolling(window=100).mean()
    df["MA200"] = df["close"].rolling(window=200).mean()

    df["Vol_MA20"] = df["volume"].rolling(window=20).mean()
    df["R_Vol"] = df["volume"] / df["Vol_MA20"].replace(0, np.nan)

    # Biến động 1 tuần (5 phiên), 1 tháng (20 phiên), 3 tháng (60 phiên)
    df["Return_5d"] = df["close"].pct_change(periods=5) * 100.0
    df["Return_20d"] = df["close"].pct_change(periods=20) * 100.0
    df["Return_60d"] = df["close"].pct_change(periods=60) * 100.0
    df["High_20d"] = df["high"].shift(1).rolling(window=20).max()

    high_5d = df["high"].rolling(window=5).max()
    low_5d = df["low"].rolling(window=5).min()
    df["Range_5d_Pct"] = ((high_5d - low_5d) / low_5d.replace(0, np.nan)) * 100.0

    # Ước lượng tỷ lệ Mua chủ động (Buy Active %)
    candle_spread = (df["high"] - df["low"]).replace(0, np.nan)
    close_loc = ((df["close"] - df["low"]) / candle_spread).fillna(0.5)
    body_bias = np.where(df["close"] >= df["open"], 0.06, -0.04)
    active_buy_1d = np.clip(close_loc * 0.85 + 0.075 + body_bias, 0.05, 0.95)
    active_buy_5d = (
        (df["volume"] * active_buy_1d).rolling(window=5).sum()
        / df["volume"].rolling(window=5).sum().replace(0, np.nan)
    ) * 100.0
    df["Buy_Active_Pct"] = (active_buy_1d * 100.0 * 0.60) + (active_buy_5d.fillna(50.0) * 0.40)

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


def evaluate_stock(
    symbol: str,
    df: pd.DataFrame,
    vnindex_ret_20d: float,
    mcap_bil: float = 0.0,
) -> dict | None:
    if df is None or len(df) < 105:
        return None

    df = calculate_indicators(df)
    last = df.iloc[-1]
    prev = df.iloc[-2]
    prev2 = df.iloc[-3]

    if np.isnan(last["close"]) or np.isnan(last["Vol_MA20"]) or np.isnan(last["MA100"]):
        return None

    # Giá trị giao dịch trung bình 20 phiên (tỷ VNĐ) & Vốn hóa ước tính
    avg_trading_val_bil = float((last["close"] * last["Vol_MA20"]) / 1_000_000.0)
    market_cap_bil = float(mcap_bil) if mcap_bil > 0 else round(avg_trading_val_bil * 120.0, 1)

    # Tầng 1: Lọc thanh khoản cơ bản
    if last["close"] < MIN_PRICE_VND:
        return None
    if last["Vol_MA20"] < MIN_VOL_MA20 and avg_trading_val_bil < MIN_TRADING_VAL_BILLION:
        return None

    ret_1w = float(last["Return_5d"]) if not np.isnan(last["Return_5d"]) else 0.0
    ret_1m = float(last["Return_20d"]) if not np.isnan(last["Return_20d"]) else 0.0
    ret_3m = float(last["Return_60d"]) if not np.isnan(last["Return_60d"]) else ret_1m
    buy_active_pct = float(last["Buy_Active_Pct"])
    vol_ma20 = float(last["Vol_MA20"])

    rs_diff = float(ret_1m - vnindex_ret_20d)
    rs_rating = int(np.clip(round(58.0 + (rs_diff * 2.1) + (ret_3m * 0.35) + (ret_1w * 0.45)), 1, 99))

    ma50_slope_up = last["MA50"] >= df["MA50"].iloc[-5]
    ma100_slope_up = last["MA100"] >= df["MA100"].iloc[-10] * 0.998

    candle_range = max(float(last["high"] - last["low"]), 0.001)
    close_pos = float((last["close"] - last["low"]) / candle_range)

    # Tính cả mức tăng/giảm điểm (giá) và % tăng/giảm trong phiên của cổ phiếu
    daily_change_pts = float(last["close"] - prev["close"])
    daily_change_pct = float((daily_change_pts / prev["close"]) * 100.0)
    session_change_str = f"{daily_change_pts:+.2f} ({daily_change_pct:+.2f}%)"

    dist_to_ma100_pct = float(((last["close"] - last["MA100"]) / last["MA100"]) * 100.0)

    # Kiểm tra 3 bộ tiêu chí định lượng (Early Signal, Confirmation, Accumulation Hunter)
    has_mcap_1000b = market_cap_bil >= MIN_MARKET_CAP_BIL
    pass_early_signal = (
        has_mcap_1000b
        and vol_ma20 > 100_000
        and rs_rating > 75
        and ret_1m > 5.0
        and (-3.0 < ret_1w < 8.0)
        and buy_active_pct > 51.0
    )
    pass_confirmation = (
        has_mcap_1000b
        and vol_ma20 > 200_000
        and rs_rating > 85
        and ret_1m > 10.0
        and (3.0 < ret_1w < 12.0)
        and buy_active_pct > 52.0
    )
    pass_accumulation = (
        has_mcap_1000b
        and vol_ma20 > 100_000
        and rs_rating > 70
        and ret_1m > 0.0
        and (-5.0 < ret_1w < 5.0)
        and buy_active_pct > 50.0
    )

    # ==========================================================================
    # 1. MẪU HÌNH VIC: BẮT ĐÁY NHỊP CHỈNH VỀ MA100 TRONG UPTREND LỚN
    # ==========================================================================
    macro_uptrend = ma100_slope_up and (last["MA50"] >= last["MA100"] * 0.97)
    near_ma100 = (-4.0 <= dist_to_ma100_pct <= 5.0) or (
        df["low"].iloc[-3:].min() <= last["MA100"] * 1.025 and last["close"] >= last["MA100"] * 0.965
    )
    min_rsi_3d = float(df["RSI14"].iloc[-3:].min())
    rsi_bottoming = (26.0 <= min_rsi_3d <= 46.0) and (last["RSI14"] >= prev["RSI14"] - 1.5)
    macd_recovering = (
        (last["MACD_Hist"] > prev["MACD_Hist"])
        or (prev["MACD_Hist"] > prev2["MACD_Hist"] and last["MACD_Hist"] >= prev["MACD_Hist"] * 0.92)
        or (last["MACD"] > last["MACD_Signal"] and prev["MACD"] <= prev["MACD_Signal"])
    )
    price_stabilizing = (last["close"] >= last["open"] or close_pos >= 0.38) and (last["R_Vol"] <= 1.45)

    is_vic_ma100_pullback = (
        macro_uptrend
        and near_ma100
        and rsi_bottoming
        and macd_recovering
        and price_stabilizing
    )

    # ==========================================================================
    # 2. MÃ TIỀM NĂNG: TÍCH LŨY SIẾT NỀN CẠN CUNG - MUA GOM
    # ==========================================================================
    is_potential_acc = (
        last["close"] > last["MA50"]
        and ma50_slope_up
        and (0.98 * last["MA20"] <= last["close"] <= 1.05 * last["MA20"])
        and (last["BB_Width"] <= 15.0 or last["Range_5d_Pct"] <= 6.0)
        and abs(daily_change_pct) <= 2.5
        and last["R_Vol"] <= 0.95
        and 44.0 <= last["RSI14"] <= 66.0
        and (pass_accumulation or (rs_diff >= -2.0 and buy_active_pct >= 50.0 and (-5.0 < ret_1w < 5.0)))
    )

    is_pocket_pivot = (
        last["close"] > last["MA50"]
        and prev["BB_Width"] <= 16.0
        and 1.0 <= daily_change_pct < 2.5
        and 1.10 <= last["R_Vol"] < 1.5
        and close_pos >= 0.60
        and (pass_early_signal or (rs_diff >= -1.0 and buy_active_pct > 51.0 and (-3.0 < ret_1w < 8.0)))
    )

    # ==========================================================================
    # 3. MÃ MẠNH: BÙNG NỔ DÒNG TIỀN - MUA BREAKOUT
    # ==========================================================================
    is_strong_breakout = (
        last["close"] > last["MA20"]
        and last["close"] > last["MA50"]
        and last["R_Vol"] >= 1.40
        and daily_change_pct >= 2.2
        and close_pos >= 0.65
        and 53.0 <= last["RSI14"] <= 77.0
        and last["MACD_Hist"] > 0
        and (pass_confirmation or pass_early_signal or (rs_diff >= 1.0 and buy_active_pct > 51.0))
    )

    # ==========================================================================
    # 4. CẢNH BÁO BÁN / CHỐT LỜI
    # ==========================================================================
    dist_from_ma20_pct = ((last["close"] - last["MA20"]) / last["MA20"]) * 100.0
    is_sell_signal = (
        (last["close"] < last["MA20"] and prev["close"] >= prev["MA20"] and last["R_Vol"] >= 1.30)
        or last["RSI14"] >= 79.0
        or dist_from_ma20_pct >= 16.0
    )

    category = None
    action_label = ""
    if is_strong_breakout:
        category = "STRONG"
        action_label = "MUA BREAKOUT"
    elif is_vic_ma100_pullback:
        category = "MA100_PULLBACK"
        action_label = "BAT DAY MA100 (MAU HINH VIC)"
    elif is_potential_acc or is_pocket_pivot:
        category = "POTENTIAL"
        action_label = "GOM NEN (
