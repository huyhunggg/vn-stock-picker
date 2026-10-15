#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VN-STOCK FULL MARKET QUANT SCREENER (HOSE + HNX + UPCOM ~ 1600+ MÃ)
- Quét đa luồng (Multi-threading 16 workers) toàn bộ thị trường trong ~45 giây
- Tự động lấy danh sách toàn bộ cổ phiếu 3 sàn không phụ thuộc API Key
- Tích hợp ngầm 3 phễu lọc (Early Signal, Confirmation, Accumulation Hunter)
  để xuất ra đúng các bảng khuyến nghị hành động chuẩn:
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
SCAN_MODE = os.getenv("SCAN_MODE", "ALL")  # Mặc định quét toàn bộ 3 sàn
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
    """
    Tự động lấy toàn bộ ~1650 mã cổ phiếu trên 3 sàn HOSE, HNX, UPCOM
    bằng nhiều nguồn dự phòng để không bao giờ bị giới hạn số mã.
    """
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

    # Nguồn 2: API Danh sách mã của Entrade / DNSE (nếu nguồn 1 thiếu)
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

    # Ước lượng tỷ lệ Mua chủ động (Buy Active %) từ hành vi nến và khối lượng
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

    # Tầng 1: Lọc thanh khoản cơ bản (KLTB 20 phiên >= 100k hoặc GTGD >= 2 tỷ VNĐ/phiên)
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
    # Điểm RS (1 - 99) dùng để kiểm tra ngưỡng RS > 70, RS > 75, RS > 85
    rs_rating = int(np.clip(round(58.0 + (rs_diff * 2.1) + (ret_3m * 0.35) + (ret_1w * 0.45)), 1, 99))

    ma50_slope_up = last["MA50"] >= df["MA50"].iloc[-5]
    ma100_slope_up = last["MA100"] >= df["MA100"].iloc[-10] * 0.998

    candle_range = max(float(last["high"] - last["low"]), 0.001)
    close_pos = float((last["close"] - last["low"]) / candle_range)
    daily_change_pct = float(((last["close"] - prev["close"]) / prev["close"]) * 100.0)
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
    # 2. MÃ TIỀM NĂNG: TÍCH LŨY SIẾT NỀN CẠN CUNG - MUA GOM (KẾT HỢP ACCUMULATION & EARLY)
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
    # 3. MÃ MẠNH: BÙNG NỔ DÒNG TIỀN - MUA BREAKOUT (KẾT HỢP CONFIRMATION & EARLY)
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
        action_label = "GOM NEN (POCKET PIVOT)" if is_pocket_pivot else "TICH LUY CAN CUNG"
    elif is_sell_signal:
        category = "SELL_WARNING"
        action_label = "CAT LO / GAY MA20" if last["close"] < last["MA20"] else "CHOT LOI (QUA MUA)"
    else:
        return None

    # --- CHẤM ĐIỂM ĐỊNH LƯỢNG (ƯU TIÊN CÁC MÃ ĐẠT CONFIRMATION / EARLY / ACCUMULATION) ---
    if category == "MA100_PULLBACK":
        proximity_pts = float(np.clip(35.0 - abs(dist_to_ma100_pct) * 5.5, 10.0, 35.0))
        trend_pts = 30.0 if (last["MA50"] > last["MA100"] and ma100_slope_up) else 20.0
        rebound_pts = (18.0 if last["MACD_Hist"] > prev["MACD_Hist"] else 10.0) + float(
            np.clip(close_pos * 17.0, 5.0, 17.0)
        )
        total_score = round(proximity_pts + trend_pts + rebound_pts, 1)
    else:
        score_rs = float(np.clip((rs_diff + 4.0) * 3.2, 0.0, 35.0))
        if category == "STRONG":
            score_vol = float(np.clip(last["R_Vol"] * 14.5 * close_pos, 0.0, 35.0))
        elif category == "POTENTIAL":
            tightness_bonus = max(0.0, (16.0 - float(last["BB_Width"])) * 2.2)
            score_vol = float(np.clip(tightness_bonus + close_pos * 10.0, 0.0, 35.0))
        else:
            score_vol = float(np.clip(last["R_Vol"] * 12.0, 0.0, 35.0))

        trend_pts = 0.0
        if last["close"] > last["MA20"] > last["MA50"]:
            trend_pts += 15.0
        elif last["close"] > last["MA50"]:
            trend_pts += 10.0
        if not np.isnan(last["MA200"]) and last["close"] > last["MA200"]:
            trend_pts += 5.0
        if 52.0 <= last["RSI14"] <= 70.0 and last["MACD_Hist"] >= prev["MACD_Hist"]:
            trend_pts += 10.0
        elif 45.0 <= last["RSI14"] <= 74.0:
            trend_pts += 6.0

        # Cộng điểm thưởng hội tụ nếu thỏa mãn trọn vẹn các bộ tiêu chí Confirmation / Early / Accumulation
        scanner_bonus = 0.0
        if pass_confirmation:
            scanner_bonus = 8.0
        elif pass_early_signal:
            scanner_bonus = 5.0
        elif pass_accumulation:
            scanner_bonus = 4.0

        total_score = round(min(99.0, score_rs + score_vol + trend_pts + scanner_bonus), 1)

    atr_val = float(last["ATR14"]) if not np.isnan(last["ATR14"]) else float(last["close"] * 0.03)
    stop_loss = round(float(last["close"]) - 2.0 * atr_val, 2)
    target_price = round(float(last["close"]) + 3.0 * atr_val, 2)
    tp_pct = round(((target_price - float(last["close"])) / float(last["close"])) * 100.0, 1)

    # Bảng riêng cho Mẫu hình VIC (Bắt đáy MA100)
    if category == "MA100_PULLBACK":
        return {
            "Mã": symbol,
            "Nhóm": category,
            "Tín hiệu": action_label,
            "Điểm": total_score,
            "Giá": round(float(last["close"]), 2),
            "% Phiên": round(daily_change_pct, 2),
            "Vol/MA20": round(float(last["R_Vol"]), 2),
            "MA100": round(float(last["MA100"]), 2),
            "Cách MA100 (%)": round(dist_to_ma100_pct, 2),
            "RSI(14)": round(float(last["RSI14"]), 1),
            "MACD Hist": round(float(last["MACD_Hist"]), 2),
            "Cắt lỗ (2ATR)": stop_loss,
            "Mục tiêu (3ATR)": f"{target_price:.2f} (+{tp_pct:.1f}%)",
        }

    # Bảng chuẩn cho Top Mã Mạnh, Top Mã Tiềm Năng & Cảnh Báo Chốt Lời (Giữ đúng bộ cột chuẩn)
    return {
        "Mã": symbol,
        "Nhóm": category,
        "Tín hiệu": action_label,
        "Điểm": total_score,
        "Giá": round(float(last["close"]), 2),
        "% Phiên": round(daily_change_pct, 2),
        "Vol/MA20": round(float(last["R_Vol"]), 2),
        "RS vs VNI (%)": round(rs_diff, 1),
        "RSI(14)": round(float(last["RSI14"]), 1),
        "Độ siết BB (%)": round(float(last["BB_Width"]), 1),
        "Cắt lỗ (2ATR)": stop_loss,
        "Mục tiêu (3ATR)": f"{target_price:.2f} (+{tp_pct:.1f}%)",
    }


# ==============================================================================
# 5. HIỂN THỊ BẢNG KẾT QUẢ TRÊN GITHUB ACTIONS SUMMARY
# ==============================================================================
def write_github_summary(
    vnindex_info: dict,
    strong_list: list[dict],
    ma100_list: list[dict],
    potential_list: list[dict],
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
        f"## 📊 Báo Cáo Quét Toàn Thị Trường HOSE + HNX + UPCOM ({now_str})",
        f"- **Tổng số mã niêm yết đã quét:** `{total_scanned} / {total_universe}` mã (Thời gian chạy: `{elapsed_sec:.1f}s`)",
        f"- **Điều kiện lọc Thanh khoản:** Khối lượng TB 20 phiên >= `{MIN_VOL_MA20:,}` cp hoặc GTGD >= `{MIN_TRADING_VAL_BILLION}` tỷ VNĐ/phiên",
        f"- **VN-INDEX:** `{vni_close:,.2f}` (`{vni_chg:+.2f}%`) | **Hiệu suất 20 phiên:** `{vni_ret20:+.2f}%`",
        "",
        f"### 🚀 1. Top Mã Mạnh (Bùng nổ dòng tiền - Mua Breakout)",
    ]

    if strong_list:
        df_strong = pd.DataFrame(strong_list).drop(columns=["Nhóm"])
        lines.append(df_strong.head(MAX_DISPLAY_ROWS).to_markdown(index=False))
    else:
        lines.append("_Chưa có mã đạt chuẩn nổ Vol & vượt nền trong phiên._")

    lines.append("")
    lines.append(f"### 💎 2. Top Mã Tiềm Năng (Siết nền cạn cung - Mua Gom)")
    if potential_list:
        df_pot = pd.DataFrame(potential_list).drop(columns=["Nhóm"])
        lines.append(df_pot.head(MAX_DISPLAY_ROWS).to_markdown(index=False))
    else:
        lines.append("_Không có mã đạt tiêu chuẩn siết nền cạn cung._")

    lines.append("")
    lines.append(f"### ⚠️ 3. Cảnh Báo Chốt Lời / Gãy Nền MA20")
    if sell_list:
        df_sell = pd.DataFrame(sell_list).drop(columns=["Nhóm"])
        lines.append(df_sell.head(25).to_markdown(index=False))
    else:
        lines.append("_Không có cảnh báo bán đột biến._")

    lines.append("")
    lines.append("---")
    lines.append(f"### 🎯 4. Bảng Riêng: Nhóm Bắt Đáy MA100 - Mẫu hình VIC (Tìm thấy: {len(ma100_list)} mã)")
    if ma100_list:
        df_ma100 = pd.DataFrame(ma100_list).drop(columns=["Nhóm"])
        lines.append(df_ma100.head(MAX_DISPLAY_ROWS).to_markdown(index=False))
    else:
        lines.append("_Chưa có mã chạm MA100 kèm tín hiệu chững đà rơi RSI/MACD._")

    summary_text = chr(10).join(lines)
    print(summary_text)

    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(summary_text + chr(10))


# ==============================================================================
# 6. HÀM CHẠY CHÍNH (QUÉT ĐA LUỒNG 16 WORKERS TOÀN THỊ TRƯỜNG)
# ==============================================================================
def process_single_symbol(symbol: str, vnindex_ret_20d: float, mcap_bil: float) -> tuple[bool, dict | None]:
    session = get_thread_session()
    df = fetch_historical_data(symbol, days=260, session=session)
    if df.empty:
        return False, None
    res = evaluate_stock(symbol, df, vnindex_ret_20d, mcap_bil=mcap_bil)
    return True, res


def run_scanner():
    start_time = time.time()
    logger.info("Bắt đầu chạy Hệ thống Quét Toàn Thị Trường (HOSE + HNX + UPCOM)...")

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
        vnindex_ret_20d = 0.0
        vnindex_info = {"close": 0.0, "change_pct": 0.0, "ret_20d": 0.0}

    symbols = get_stock_universe()
    mcap_map = get_market_cap_map()
    total_universe = len(symbols)
    logger.info(f"Tổng số mã cổ phiếu 3 sàn cần quét: {total_universe} mã (Chạy {MAX_WORKERS} luồng song song)")

    strong_stocks = []
    ma100_pullback_stocks = []
    potential_stocks = []
    sell_warnings = []
    scanned_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_sym = {
            executor.submit(process_single_symbol, sym, vnindex_ret_20d, mcap_map.get(sym, 0.0)): sym
            for sym in symbols
        }
        for future in as_completed(future_to_sym):
            try:
                has_data, res = future.result()
                if has_data:
                    scanned_count += 1
                if not res:
                    continue

                if res["Nhóm"] == "STRONG":
                    strong_stocks.append(res)
                elif res["Nhóm"] == "MA100_PULLBACK":
                    ma100_pullback_stocks.append(res)
                elif res["Nhóm"] == "POTENTIAL":
                    potential_stocks.append(res)
                elif res["Nhóm"] == "SELL_WARNING":
                    sell_warnings.append(res)
            except Exception:
                pass

    ma100_pullback_stocks.sort(key=lambda x: (x["Điểm"], -abs(x["Cách MA100 (%)"])), reverse=True)
    strong_stocks.sort(key=lambda x: (x["Điểm"], x["Vol/MA20"]), reverse=True)
    potential_stocks.sort(key=lambda x: x["Điểm"], reverse=True)
    sell_warnings.sort(key=lambda x: x["Vol/MA20"], reverse=True)

    # Xuất file CSV cho 3 bảng chính và file CSV riêng cho bảng Mẫu hình VIC
    main_results = strong_stocks + potential_stocks + sell_warnings
    if main_results:
        pd.DataFrame(main_results).to_csv("scan_results_latest.csv", index=False, encoding="utf-8-sig")

    if ma100_pullback_stocks:
        pd.DataFrame(ma100_pullback_stocks).to_csv("scan_vic_ma100_latest.csv", index=False, encoding="utf-8-sig")

    elapsed = time.time() - start_time
    write_github_summary(
        vnindex_info=vnindex_info,
        strong_list=strong_stocks,
        ma100_list=ma100_pullback_stocks,
        potential_list=potential_stocks,
        sell_list=sell_warnings,
        total_universe=total_universe,
        total_scanned=scanned_count,
        elapsed_sec=elapsed,
    )
    logger.info(f"Hoàn tất quét {scanned_count}/{total_universe} mã trong {elapsed:.1f} giây!")


if __name__ == "__main__":
    run_scanner()
