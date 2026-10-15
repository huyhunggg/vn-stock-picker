#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VN-STOCK MULTI-FACTOR QUANT SCREENER & TELEGRAM BOT
Tự động quét cổ phiếu Tiềm Năng (Tích lũy cạn cung) & Cổ phiếu Mạnh (Breakout dòng tiền)
Nguồn dữ liệu: vnstock (VCI / TCBS)
"""

import os
import time
import logging
from datetime import datetime, timedelta, timezone
import requests
import numpy as np
import pandas as pd

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Múi giờ Việt Nam (UTC+7)
VN_TZ = timezone(timedelta(hours=7))

# ==============================================================================
# 1. CẤU HÌNH THAM SỐ BỘ LỌC & TELEGRAM
# ==============================================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Chế độ quét:
# - "LIQUID_UNIVERSE": Quét rổ VN100 + HNX30 + các mã thanh khoản cao (nhanh, tránh bị chặn Rate Limit)
# - "ALL": Quét toàn bộ sàn HOSE, HNX, UPCOM
SCAN_MODE = os.getenv("SCAN_MODE", "LIQUID_UNIVERSE")
DATA_SOURCE = os.getenv("VNSTOCK_SOURCE", "VCI")  # Nguồn VCI hoặc TCBS

# Ngưỡng lọc Tầng 1 (Thanh khoản & Thị giá)
MIN_VOL_MA20 = int(os.getenv("MIN_VOL_MA20", "500000"))       # Tối thiểu 500,000 cp/phiên
MIN_TRADING_VAL_BILLION = float(os.getenv("MIN_VAL_BIL", "10.0"))  # Hoặc GTGD >= 10 tỷ VNĐ/phiên
MIN_PRICE_VND = float(os.getenv("MIN_PRICE_VND", "10.0"))     # Thị giá >= 10,000 VNĐ (đơn vị nghìn đồng)

# Thời gian nghỉ giữa mỗi lần gọi API để tránh Rate Limit (giây)
API_DELAY_SECONDS = float(os.getenv("API_DELAY_SECONDS", "0.35"))

# Danh sách các mã thanh khoản cao bổ sung ngoài VN100 (Ngân hàng, Chứng khoán, Thép, BĐS, Bán lẻ, Dầu khí...)
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
    "GMD", "HAH", "VOS", "VSC", "PVT",
    "PC1", "REE", "GEG", "NT2", "POW", "TV2",
    "CTR", "VGI", "FOX", "ELC", "CMG",
    "DGC", "DCM", "DPM", "CSV", "BFC", "LAS",
    "HHV", "VCG", "LCG", "FCN", "C4G", "CTD", "HBC", "BMP", "NTP", "PTB", "VCS", "TNG", "MSH", "GIL"
]


# ==============================================================================
# 2. KẾT NỐI DỮ LIỆU VNSTOCK (HỖ TRỢ VNSTOCK v3 / v4 / v5)
# ==============================================================================
def normalize_price_scale(df: pd.DataFrame) -> pd.DataFrame:
    """
    Chuẩn hóa đơn vị giá về nghìn VNĐ (ví dụ: 25500 -> 25.5) để đồng nhất tính toán.
    """
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
    """
    Lấy dữ liệu lịch sử OHLCV của 1 mã cổ phiếu hoặc chỉ số (VNINDEX) qua vnstock.
    Tự động tương thích với các phiên bản vnstock 3.x / 4.x / 5.x.
    """
    end_date = datetime.now(VN_TZ).strftime("%Y-%m-%d")
    start_date = (datetime.now(VN_TZ) - timedelta(days=days)).strftime("%Y-%m-%d")

    for attempt in range(2):
        try:
            # Cách 1: Chuẩn giao diện hợp nhất Quote (vnstock >= 3.2.x / 4.x / 5.x)
            from vnstock import Quote
            quote = Quote(symbol=symbol, source=DATA_SOURCE)
            df = quote.history(start=start_date, end=end_date, interval="1D")
            if df is not None and not df.empty:
                return normalize_price_scale(df)
        except Exception:
            try:
                # Cách 2: Giao diện Vnstock().stock()
                from vnstock import Vnstock
                stock = Vnstock().stock(symbol=symbol, source=DATA_SOURCE)
                df = stock.quote.history(start=start_date, end=end_date, interval="1D")
                if df is not None and not df.empty:
                    return normalize_price_scale(df)
            except Exception as e:
                if attempt == 1:
                    logger.debug(f"Không tải được dữ liệu mã {symbol}: {e}")
                time.sleep(0.8)

    return pd.DataFrame()


def get_stock_universe() -> list[str]:
    """
    Lấy danh sách mã cổ phiếu cần quét.
    """
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
            # Lấy rổ VN100 & HNX30 kết hợp danh sách thanh khoản cao
            for group in ["VN100", "HNX30"]:
                try:
                    df_grp = listing.symbols_by_group(group)
                    if isinstance(df_grp, pd.Series):
                        symbols.update(df_grp.dropna().astype(str).str.upper().tolist())
                    elif isinstance(df_grp, pd.DataFrame) and "symbol" in df_grp.columns:
                        symbols.update(df_grp["symbol"].dropna().astype(str).str.upper().tolist())
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"Dùng danh sách mã thanh khoản mặc định do lỗi Listing: {e}")

    return sorted(list(symbols))


# ==============================================================================
# 3. TÍNH TOÁN CHỈ BÁO KỸ THUẬT & CHẤM ĐIỂM ĐỊNH LƯỢNG (4 TẦNG)
# ==============================================================================
def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tính toán bộ chỉ báo kỹ thuật: MA20/50/200, Vol MA20, RSI(14), MACD, Bollinger Bands, ATR(14).
    """
    df = df.sort_values("time").reset_index(drop=True)

    # 1. Đường trung bình động Giá & Khối lượng
    df["MA20"] = df["close"].rolling(window=20).mean()
    df["MA50"] = df["close"].rolling(window=50).mean()
    df["MA200"] = df["close"].rolling(window=200).mean()
    df["Vol_MA20"] = df["volume"].rolling(window=20).mean()
    df["R_Vol"] = df["volume"] / df["Vol_MA20"].replace(0, np.nan)

    # 2. Hiệu suất giá 20 phiên & Đỉnh 20 phiên (không tính phiên hiện tại)
    df["Return_20d"] = df["close"].pct_change(periods=20) * 100.0
    df["High_20d"] = df["high"].shift(1).rolling(window=20).max()

    # 3. Biên độ dao động 5 phiên gần nhất (%)
    high_5d = df["high"].rolling(window=5).max()
    low_5d = df["low"].rolling(window=5).min()
    df["Range_5d_Pct"] = ((high_5d - low_5d) / low_5d.replace(0, np.nan)) * 100.0

    # 4. Chỉ báo RSI(14) theo phương pháp Wilder (EMA alpha = 1/14)
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["RSI14"] = 100.0 - (100.0 / (1.0 + rs))

    # 5. Chỉ báo MACD (12, 26, 9)
    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_Hist"] = df["MACD"] - df["MACD_Signal"]

    # 6. Bollinger Bands (20, 2) & Độ rộng dải băng (BB Bandwidth)
    std20 = df["close"].rolling(window=20).std()
    df["BB_Upper"] = df["MA20"] + 2.0 * std20
    df["BB_Lower"] = df["MA20"] - 2.0 * std20
    df["BB_Width"] = ((df["BB_Upper"] - df["BB_Lower"]) / df["MA20"].replace(0, np.nan)) * 100.0

    # 7. ATR(14) phục vụ quản trị rủi ro (Cắt lỗ & Chốt lời động)
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift(1)).abs()
    low_close = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df["ATR14"] = tr.rolling(window=14).mean()

    return df


def evaluate_stock(symbol: str, df: pd.DataFrame, vnindex_ret_20d: float) -> dict | None:
    """
    Áp dụng phễu 4 tầng để phân loại cổ phiếu và chấm điểm trên thang 100.
    """
    if df is None or len(df) < 60:
        return None

    df = calculate_indicators(df)
    last = df.iloc[-1]
    prev = df.iloc[-2]

    # Giá trị giao dịch trung bình 20 phiên (tỷ VNĐ)
    # close đơn vị nghìn VNĐ * volume cổ phiếu / 1,000,000 = tỷ VNĐ
    avg_trading_val_bil = (last["close"] * last["Vol_MA20"]) / 1_000_000.0

    # --- TẦNG 1: PHỄU THANH KHOẢN & THỊ GIÁ ---
    if last["close"] < MIN_PRICE_VND:
        return None
    if last["Vol_MA20"] < MIN_VOL_MA20 and avg_trading_val_bil < MIN_TRADING_VAL_BILLION:
        return None

    # --- TẦNG 2: SỨC MẠNH GIÁ TƯƠNG ĐỐI (RS 20 PHIÊN SO VỚI VN-INDEX) ---
    rs_diff = float(last["Return_20d"] - vnindex_ret_20d)
    ma50_slope_up = last["MA50"] >= df["MA50"].iloc[-5]

    # Vị thế đóng cửa trong phiên (0.0 -> 1.0) & % thay đổi giá
    candle_range = max(float(last["high"] - last["low"]), 0.001)
    close_pos = float((last["close"] - last["low"]) / candle_range)
    daily_change_pct = float(((last["close"] - prev["close"]) / prev["close"]) * 100.0)

    # --- TẦNG 3A: NHÁNH "MÃ TIỀM NĂNG" (TÍCH LŨY SIẾT NỀN / CẠN CUNG) ---
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

    # Pocket Pivot tại nền (Nến bật sớm từ MA20 khi nền đang siết chặt)
    is_pocket_pivot = (
        last["close"] > last["MA50"]
        and prev["BB_Width"] <= 15.0
        and 1.2 <= daily_change_pct < 2.5
        and 1.15 <= last["R_Vol"] < 1.5
        and close_pos >= 0.65
        and rs_diff >= 0.0
    )

    # --- TẦNG 3B: NHÁNH "MÃ MẠNH" (LEADER BÙNG NỔ DÒNG TIỀN) ---
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

    # --- TẦNG 4: CẢNH BÁO BÁN / CHỐT LỜI / GÃY NỀN ---
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
        break_20d = " (Vượt đỉnh 20P)" if last["close"] >= last["High_20d"] else ""
        action_label = f"🔥 MUA BREAKOUT{break_20d}"
    elif is_potential_acc or is_pocket_pivot:
        category = "POTENTIAL"
        action_label = "💎 GOM NỀN (POCKET PIVOT)" if is_pocket_pivot else "🎯 TÍCH LŨY CẠN CUNG"
    elif is_sell_signal:
        category = "SELL_WARNING"
        if last["close"] < last["MA20"]:
            action_label = "⚠️ CẮT LỖ / GÃY MA20 VOL LỚN"
        else:
            action_label = "🔔 CHỐT LỜI (QUÁ MUA)"
    else:
        return None

    # --- CHẤM ĐIỂM ĐỊNH LƯỢNG (THANG 100 ĐIỂM) ---
    # 1. Điểm Sức mạnh RS (Tối đa 35đ)
    score_rs = float(np.clip((rs_diff + 4.0) * 3.2, 0.0, 35.0))

    # 2. Điểm Dòng tiền / Độ chặt nền (Tối đa 35đ)
    if category == "STRONG":
        score_vol = float(np.clip(last["R_Vol"] * 14.5 * close_pos, 0.0, 35.0))
    elif category == "POTENTIAL":
        tightness_bonus = max(0.0, (16.0 - float(last["BB_Width"])) * 2.2)
        score_vol = float(np.clip(tightness_bonus + close_pos * 10.0, 0.0, 35.0))
    else:
        score_vol = float(np.clip(last["R_Vol"] * 12.0, 0.0, 35.0))

    # 3. Điểm Cấu trúc Xu hướng (Tối đa 30đ)
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

    # Quản trị rủi ro theo ATR(14)
    atr_val = float(last["ATR14"]) if not np.isnan(last["ATR14"]) else float(last["close"] * 0.03)
    stop_loss = round(float(last["close"]) - 2.0 * atr_val, 2)
    target_price = round(float(last["close"]) + 3.0 * atr_val, 2)

    return {
        "symbol": symbol,
        "category": category,
        "action": action_label,
        "score": total_score,
        "close": round(float(last["close"]), 2),
        "change_pct": round(daily_change_pct, 2),
        "r_vol": round(float(last["R_Vol"]), 2),
        "rs_diff": round(rs_diff, 1),
        "rsi": round(float(last["RSI14"]), 1),
        "bb_width": round(float(last["BB_Width"]), 1),
        "stop_loss": stop_loss,
        "target": target_price,
    }


# ==============================================================================
# 4. ĐỊNH DẠNG BÁO CÁO & GỬI TIN NHẮN TELEGRAM BOT
# ==============================================================================
def format_telegram_report(
    vnindex_info: dict,
    strong_list: list[dict],
    potential_list: list[dict],
    sell_list: list[dict],
    total_scanned: int,
) -> str:
    """
    Tạo nội dung tin nhắn báo cáo định dạng HTML gửi qua Telegram Bot.
    """
    now_str = datetime.now(VN_TZ).strftime("%H:%M - %d/%m/%Y")
    vni_close = vnindex_info.get("close", 0.0)
    vni_chg = vnindex_info.get("change_pct", 0.0)
    vni_ret20 = vnindex_info.get("ret_20d", 0.0)
    vni_icon = "🟢" if vni_chg >= 0 else "🔴"

    lines = [
        f"📊 <b>BÁO CÁO QUÉT CỔ PHIẾU ĐỊNH LƯỢNG</b>",
        f"🕒 <i>Thời gian: {now_str} (Đã quét: {total_scanned} mã)</i>",
        f"{vni_icon} <b>VN-INDEX:</b> {vni_close:,.2f} ({vni_chg:+.2f}%) | Hiệu suất 20P: {vni_ret20:+.2f}%",
        "━━━━━━━━━━━━━━━━━━",
    ]

    # 1. Nhóm Cổ phiếu Mạnh (Breakout dòng tiền)
    lines.append("🚀 <b>TOP MÃ MẠNH (BÙNG NỔ DÒNG TIỀN - MUA BREAKOUT)</b>")
    if strong_list:
        for idx, item in enumerate(strong_list[:8], 1):
            lines.append(
                f"{idx}. <b>#{item['symbol']}</b> [{item['score']}đ] — Giá: <b>{item['close']}</b> ({item['change_pct']:+.2f}%)
"
                f"   • Vol: <b>{item['r_vol']}x</b> MA20 | RS vs VNI: <b>{item['rs_diff']:+.1f}%</b> | RSI: {item['rsi']}
"
                f"   • Cắt lỗ (2ATR): <code>{item['stop_loss']}</code> | Mục tiêu: <code>{item['target']}</code>"
            )
    else:
        lines.append("<i>(Chưa có mã đạt chuẩn nổ Vol & vượt nền trong phiên)</i>")

    lines.append("━━━━━━━━━━━━━━━━━━")

    # 2. Nhóm Cổ phiếu Tiềm Năng (Tích lũy cạn cung / Siết nền)
    lines.append("💎 <b>TOP MÃ TIỀM NĂNG (SIẾT NỀN CẠN CUNG - MUA GOM)</b>")
    if potential_list:
        for idx, item in enumerate(potential_list[:8], 1):
            lines.append(
                f"{idx}. <b>#{item['symbol']}</b> [{item['score']}đ] — Giá: <b>{item['close']}</b> ({item['change_pct']:+.2f}%)
"
                f"   • Vol: <b>{item['r_vol']}x</b> MA20 | Độ siết BB: <b>{item['bb_width']}%</b> | RS: <b>{item['rs_diff']:+.1f}%</b>
"
                f"   • Cắt lỗ (2ATR): <code>{item['stop_loss']}</code> | Mục tiêu: <code>{item['target']}</code>"
            )
    else:
        lines.append("<i>(Không có mã đạt tiêu chuẩn siết nền cạn cung)</i>")

    # 3. Nhóm Cảnh báo Bán / Chốt lời
    if sell_list:
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append("⚠️ <b>CẢNH BÁO CHỐT LỜI / GÃY NỀN MA20</b>")
        for item in sell_list[:5]:
            lines.append(
                f"• <b>#{item['symbol']}</b> ({item['close']} | {item['change_pct']:+.2f}%): {item['action']} (RSI: {item['rsi']}, Vol: {item['r_vol']}x)"
            )

    lines.append("━━━━━━━━━━━━━━━━━━")
    lines.append("💡 <i>Quy tắc: Giải ngân 30-50% tại nền (Mã Tiềm Năng), gia tăng đủ vị thế khi nổ Vol >= 1.5x MA20 (Mã Mạnh).</i>")
    return "
".join(lines)


def send_telegram_message(text: str) -> bool:
    """
    Đẩy báo cáo về Telegram Bot (tự động chia nhỏ nếu tin nhắn dài quá 4000 ký tự).
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Chưa cấu hình TELEGRAM_BOT_TOKEN hoặc TELEGRAM_CHAT_ID. In kết quả ra màn hình console:")
        print("
" + text + "
")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code == 200:
            logger.info("Đã gửi báo cáo thành công về Telegram!")
            return True
        else:
            logger.error(f"Lỗi gửi Telegram ({resp.status_code}): {resp.text}")
            return False
    except Exception as e:
        logger.error(f"Ngoại lệ khi kết nối Telegram API: {e}")
        return False


# ==============================================================================
# 5. HÀM CHẠY CHÍNH (MAIN PIPELINE)
# ==============================================================================
def run_scanner():
    logger.info("Bắt đầu chạy Hệ thống Quét Cổ phiếu Định lượng...")

    # Bước 1: Lấy dữ liệu VN-INDEX để làm chuẩn so sánh Sức mạnh tương đối (RS)
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
        logger.warning("Không lấy được dữ liệu VNINDEX, đặt mặc định hiệu suất 20 phiên = 0%")
        vnindex_ret_20d = 0.0
        vnindex_info = {"close": 0.0, "change_pct": 0.0, "ret_20d": 0.0}

    # Bước 2: Lấy danh sách mã cổ phiếu cần quét
    symbols = get_stock_universe()
    logger.info(f"Tổng số mã trong danh sách quét: {len(symbols)} mã")

    strong_stocks = []
    potential_stocks = []
    sell_warnings = []
    scanned_count = 0

    # Bước 3: Quét từng mã và phân loại
    for symbol in symbols:
        df = fetch_historical_data(symbol, days=260)
        time.sleep(API_DELAY_SECONDS)

        if df.empty:
            continue

        scanned_count += 1
        res = evaluate_stock(symbol, df, vnindex_ret_20d)
        if not res:
            continue

        if res["category"] == "STRONG":
            strong_stocks.append(res)
        elif res["category"] == "POTENTIAL":
            potential_stocks.append(res)
        elif res["category"] == "SELL_WARNING":
            sell_warnings.append(res)

    # Bước 4: Sắp xếp theo Điểm định lượng từ cao xuống thấp
    strong_stocks.sort(key=lambda x: (x["score"], x["r_vol"]), reverse=True)
    potential_stocks.sort(key=lambda x: (x["score"], -x["bb_width"]), reverse=True)
    sell_warnings.sort(key=lambda x: x["r_vol"], reverse=True)

    # Xuất kết quả ra file CSV lưu trữ Artifact nếu cần đối chiếu
    all_results = strong_stocks + potential_stocks + sell_warnings
    if all_results:
        pd.DataFrame(all_results).to_csv("scan_results_latest.csv", index=False, encoding="utf-8-sig")

    # Bước 5: Tạo báo cáo và đẩy về Telegram
    report_msg = format_telegram_report(
        vnindex_info=vnindex_info,
        strong_list=strong_stocks,
        potential_list=potential_stocks,
        sell_list=sell_warnings,
        total_scanned=scanned_count,
    )
    send_telegram_message(report_msg)


if __name__ == "__main__":
    run_scanner()
