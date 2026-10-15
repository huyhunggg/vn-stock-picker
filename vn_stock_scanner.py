#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VN-STOCK MULTI-FACTOR QUANT SCREENER (GITHUB ACTIONS EDITION)
Quét cổ phiếu Tiềm Năng (Tích lũy cạn cung) & Cổ phiếu Mạnh (Breakout dòng tiền)
Hiển thị bảng trực tiếp trên trang chủ GitHub Actions Summary & xuất file CSV.
"""

import os
import time
import logging
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

# ==============================================================================
# 1. CẤU HÌNH THAM SỐ BỘ LỌC
# ==============================================================================
SCAN_MODE = os.getenv("SCAN_MODE", "LIQUID_UNIVERSE")
DATA_SOURCE = os.getenv("VNSTOCK_SOURCE", "VCI")

MIN_VOL_MA20 = int(os.getenv("MIN_VOL_MA20", "500000"))
MIN_TRADING_VAL_BILLION = float(os.getenv("MIN_VAL_BIL", "10.0"))
MIN_PRICE_VND = float(os.getenv("MIN_PRICE_VND", "10.0"))
API_DELAY_SECONDS = float(os.getenv("API_DELAY_SECONDS", "0.25"))

EXTRA_LIQUID_SYMBOLS = [
    "MSB", "HPG", "GAS", "DGW", "VPB", "SHB", "MBB", "TCB", "ACB", "CTG",
    "BID", "VCB", "STB", "LPB", "TPB", "VIB", "EIB", "HDB", "OCB", "NAB",
    "SSI", "VND", "VCI", "HCM", "SHS", "MBS", "FTS", "BSI", "CTS", "VIX",
    "ORS", "AGR", "BVS", "VDS", "TCI",
    "HSG", "NKG", "VGS", "GDA",
    "FPT", "MWG", "FRT", "PNJ", "MSN", "VNM", "SAB", "DBC", "PAN", "BAF", "ANV", "VHC", "IDI",
    "DIG", "DXG", "PDR", "NVL", "KDH", "NLG", "CEO", "HDC", "HDG", "TCH", "IJC", "NTL",
    "KBC", "IDC", "SZC", "VGC", "GVR", "PHR", "DPR", "BCM", "SIP", "NTC",
    "PVD", "PVS", "BSR", "PLX", "OIL", "PVT", "PVC", "CNG",
    "GMD", "HAH", "VOS", "VSC",
    "PC1", "REE", "GEG", "NT2", "POW", "TV2",
    "CTR", "VGI", "FOX", "ELC", "CMG",
    "DGC", "DCM", "DPM", "CSV", "BFC", "LAS",
    "HHV", "VCG", "LCG", "FCN", "C4G", "CTD", "HBC", "BMP", "NTP", "PTB", "VCS", "TNG", "MSH", "GIL"
]


# ==============================================================================
# 2. KẾT NỐI DỮ LIỆU (VNSTOCK + FALLBACK TỰ ĐỘNG QUA ENTRADE/DNSE)
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


def fetch_historical_data(symbol: str, days: int = 260) -> pd.DataFrame:
    end_dt = datetime.now(VN_TZ)
    start_dt = end_dt - timedelta(days=days)
    end_date = end_dt.strftime("%Y-%m-%d")
    start_date = start_dt.strftime("%Y-%m-%d")

    # Cách 1: Thử lấy qua API Chart Entrade/DNSE (Nhanh, ổn định trên GitHub Actions, không cần API Key)
    try:
        from_ts = int(start_dt.timestamp())
        to_ts = int(end_dt.timestamp())
        idx_type = "index" if symbol.upper() in ["VNINDEX", "VN30", "HNXINDEX"] else "stock"
        url = (
            f"https://services.entrade.com.vn/chart-api/v2/ohlcs/{idx_type}"
            f"?from={from_ts}&to={to_ts}&symbol={symbol}&resolution=1D"
        )
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=10)
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

    # Cách 2: Lấy qua vnstock
    try:
        from vnstock import Quote
        quote = Quote(symbol=symbol, source=DATA_SOURCE)
        df = quote.history(start=start_date, end=end_date, interval="1D")
        if df is not None and not df.empty:
            return normalize_price_scale(df)
    except Exception as e:
        logger.debug(f"Bỏ qua mã {symbol}: {e}")

    return pd.DataFrame()


def get_stock_universe() -> list[str]:
    symbols = set(EXTRA_LIQUID_SYMBOLS)
    try:
        from vnstock import Listing
        listing = Listing(source=DATA_SOURCE) if "source" in Listing.__init__.__code__.co_varnames else Listing()
        if SCAN_MODE.upper() == "ALL":
            df_all = listing.all_symbols()
            if isinstance(df_all, pd.DataFrame) and "symbol" in df_all.columns:
                all_syms = [
                    str(s).strip().upper()
                    for s in df_all["symbol"].dropna().unique()
                    if len(str(s).strip()) == 3 and str(s).strip().isalpha()
                ]
                return sorted(list(set(all_syms) | symbols))
        else:
            for group in ["VN100", "HNX30"]:
                try:
                    df_grp = listing.symbols_by_group(group)
                    if isinstance(df_grp, pd.Series):
                        symbols.update(df_grp.dropna().astype(str).str.upper().tolist())
                    elif isinstance(df_grp, pd.DataFrame) and "symbol" in df_grp.columns:
                        symbols.update(df_grp["symbol"].dropna().astype(str).str.upper().tolist())
                except Exception:
                    pass
    except Exception:
        pass

    return sorted(list(symbols))


# ==============================================================================
# 3. TÍNH TOÁN CHỈ BÁO & CHẤM ĐIỂM ĐỊNH LƯỢNG (4 TẦNG)
# ==============================================================================
def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("time").reset_index(drop=True)

    df["MA20"] = df["close"].rolling(window=20).mean()
    df["MA50"] = df["close"].rolling(window=50).mean()
    df["MA200"] = df["close"].rolling(window=200).mean()
    df["Vol_MA20"] = df["volume"].rolling(window=20).mean()
    df["R_Vol"] = df["volume"] / df["Vol_MA20"].replace(0, np.nan)

    df["Return_20d"] = df["close"].pct_change(periods=20) * 100.0
    df["High_20d"] = df["high"].shift(1).rolling(window=20).max()

    high_5d = df["high"].rolling(window=5).max()
    low_5d = df["low"].rolling(window=5).min()
    df["Range_5d_Pct"] = ((high_5d - low_5d) / low_5d.replace(0, np.nan)) * 100.0

    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["RSI14"] = 100.0 - (100.0 / (1.0 + rs))

    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_Hist"] = df["MACD"] - df["MACD_Signal"]

    std20 = df["close"].rolling(window=20).std()
    df["BB_Upper"] = df["MA20"] + 2.0 * std20
    df["BB_Lower"] = df["MA20"] - 2.0 * std20
    df["BB_Width"] = ((df["BB_Upper"] - df["BB_Lower"]) / df["MA20"].replace(0, np.nan)) * 100.0

    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift(1)).abs()
    low_close = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df["ATR14"] = tr.rolling(window=14).mean()

    return df


def evaluate_stock(symbol: str, df: pd.DataFrame, vnindex_ret_20d: float) -> dict | None:
    if df is None or len(df) < 60:
        return None

    df = calculate_indicators(df)
    last = df.iloc[-1]
    prev = df.iloc[-2]

    avg_trading_val_bil = (last["close"] * last["Vol_MA20"]) / 1_000_000.0

    if last["close"] < MIN_PRICE_VND:
        return None
    if last["Vol_MA20"] < MIN_VOL_MA20 and avg_trading_val_bil < MIN_TRADING_VAL_BILLION:
        return None

    rs_diff = float(last["Return_20d"] - vnindex_ret_20d)
    ma50_slope_up = last["MA50"] >= df["MA50"].iloc[-5]

    candle_range = max(float(last["high"] - last["low"]), 0.001)
    close_pos = float((last["close"] - last["low"]) / candle_range)
    daily_change_pct = float(((last["close"] - prev["close"]) / prev["close"]) * 100.0)

    # Nhánh A: Mã Tiềm Năng (Tích lũy cạn cung / Siết nền)
    is_potential_acc = (
        last["close"] > last["MA50"]
        and ma50_slope_up
        and (0.985 * last["MA20"] <= last["close"] <= 1.045 * last["MA20"])
        and (last["BB_Width"] <= 14.0 or last["Range_5d_Pct"] <= 5.5)
        and abs(daily_change_pct) <= 2.2
        and last["R_Vol"] <= 0.90
        and 45.0 <= last["RSI14"] <= 65.0
        and rs_diff >= -1.0
    )

    is_pocket_pivot = (
        last["close"] > last["MA50"]
        and prev["BB_Width"] <= 15.0
        and 1.2 <= daily_change_pct < 2.5
        and 1.15 <= last["R_Vol"] < 1.5
        and close_pos >= 0.65
        and rs_diff >= 0.0
    )

    # Nhánh B: Mã Mạnh (Breakout dòng tiền)
    is_strong_breakout = (
        last["close"] > last["MA20"]
        and last["close"] > last["MA50"]
        and last["R_Vol"] >= 1.45
        and daily_change_pct >= 2.3
        and close_pos >= 0.68
        and 54.0 <= last["RSI14"] <= 76.0
        and last["MACD_Hist"] > 0
        and rs_diff >= 1.5
    )

    # Nhánh Cảnh báo Bán
    dist_from_ma20_pct = ((last["close"] - last["MA20"]) / last["MA20"]) * 100.0
    is_sell_signal = (
        (last["close"] < last["MA20"] and prev["close"] >= prev["MA20"] and last["R_Vol"] >= 1.25)
        or last["RSI14"] >= 78.0
        or dist_from_ma20_pct >= 15.0
    )

    category = None
    action_label = ""
    if is_strong_breakout:
        category = "STRONG"
        action_label = "MUA BREAKOUT"
    elif is_potential_acc or is_pocket_pivot:
        category = "POTENTIAL"
        action_label = "GOM NEN (POCKET PIVOT)" if is_pocket_pivot else "TICH LUY CAN CUNG"
    elif is_sell_signal:
        category = "SELL_WARNING"
        action_label = "CAT LO / GAY MA20" if last["close"] < last["MA20"] else "CHOT LOI (QUA MUA)"
    else:
        return None

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

    total_score = round(score_rs + score_vol + trend_pts, 1)

    atr_val = float(last["ATR14"]) if not np.isnan(last["ATR14"]) else float(last["close"] * 0.03)
    stop_loss = round(float(last["close"]) - 2.0 * atr_val, 2)
    target_price = round(float(last["close"]) + 3.0 * atr_val, 2)

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
        "Mục tiêu (3ATR)": target_price,
    }


# ==============================================================================
# 4. HIỂN THỊ KẾT QUẢ TRÊN LOG & GITHUB ACTIONS SUMMARY
# ==============================================================================
def write_github_summary(
    vnindex_info: dict,
    strong_list: list[dict],
    potential_list: list[dict],
    sell_list: list[dict],
    total_scanned: int,
):
    now_str = datetime.now(VN_TZ).strftime("%H:%M - %d/%m/%Y")
    vni_close = vnindex_info.get("close", 0.0)
    vni_chg = vnindex_info.get("change_pct", 0.0)
    vni_ret20 = vnindex_info.get("ret_20d", 0.0)

    lines = [
        f"## 📊 Báo Cáo Quét Cổ Phiếu Định Lượng ({now_str})",
        f"- **Số mã đã quét:** `{total_scanned}` mã",
        f"- **VN-INDEX:** `{vni_close:,.2f}` (`{vni_chg:+.2f}%`) | **Hiệu suất 20 phiên:** `{vni_ret20:+.2f}%`",
        "",
        "### 🚀 1. Top Mã Mạnh (Bùng nổ dòng tiền - Mua Breakout)",
    ]

    if strong_list:
        df_strong = pd.DataFrame(strong_list).drop(columns=["Nhóm"])
        lines.append(df_strong.head(15).to_markdown(index=False))
    else:
        lines.append("_Chưa có mã đạt chuẩn nổ Vol & vượt nền trong phiên._")

    lines.append("")
    lines.append("### 💎 2. Top Mã Tiềm Năng (Siết nền cạn cung - Mua Gom)")
    if potential_list:
        df_pot = pd.DataFrame(potential_list).drop(columns=["Nhóm"])
        lines.append(df_pot.head(15).to_markdown(index=False))
    else:
        lines.append("_Không có mã đạt tiêu chuẩn siết nền cạn cung._")

    lines.append("")
    lines.append("### ⚠️ 3. Cảnh Báo Chốt Lời / Gãy Nền MA20")
    if sell_list:
        df_sell = pd.DataFrame(sell_list).drop(columns=["Nhóm"])
        lines.append(df_sell.head(10).to_markdown(index=False))
    else:
        lines.append("_Không có cảnh báo bán đột biến._")

    summary_text = chr(10).join(lines)
    print(summary_text)

    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(summary_text + chr(10))


# ==============================================================================
# 5. HÀM CHẠY CHÍNH (MAIN PIPELINE)
# ==============================================================================
def run_scanner():
    logger.info("Bắt đầu chạy Hệ thống Quét Cổ phiếu Định lượng...")

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
    logger.info(f"Tổng số mã trong danh sách quét: {len(symbols)} mã")

    strong_stocks = []
    potential_stocks = []
    sell_warnings = []
    scanned_count = 0

    for symbol in symbols:
        df = fetch_historical_data(symbol, days=260)
        time.sleep(API_DELAY_SECONDS)

        if df.empty:
            continue

        scanned_count += 1
        res = evaluate_stock(symbol, df, vnindex_ret_20d)
        if not res:
            continue

        if res["Nhóm"] == "STRONG":
            strong_stocks.append(res)
        elif res["Nhóm"] == "POTENTIAL":
            potential_stocks.append(res)
        elif res["Nhóm"] == "SELL_WARNING":
            sell_warnings.append(res)

    strong_stocks.sort(key=lambda x: (x["Điểm"], x["Vol/MA20"]), reverse=True)
    potential_stocks.sort(key=lambda x: (x["Điểm"], -x["Độ siết BB (%)"]), reverse=True)
    sell_warnings.sort(key=lambda x: x["Vol/MA20"], reverse=True)

    all_results = strong_stocks + potential_stocks + sell_warnings
    if all_results:
        pd.DataFrame(all_results).to_csv("scan_results_latest.csv", index=False, encoding="utf-8-sig")

    write_github_summary(
        vnindex_info=vnindex_info,
        strong_list=strong_stocks,
        potential_list=potential_stocks,
        sell_list=sell_warnings,
        total_scanned=scanned_count,
    )
    logger.info("Hoàn tất quét thị trường!")


if __name__ == "__main__":
    run_scanner()
