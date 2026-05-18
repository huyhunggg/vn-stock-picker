"""
VN Stock Picker - update_data.py

Script này tạo/cập nhật data.json cho GitHub Pages.

Cách chạy local:
    pip install -r requirements.txt
    python update_data.py

Trên GitHub Actions:
    Workflow .github/workflows/update-data.yml sẽ tự chạy và commit data.json.

Ghi chú:
- Script ưu tiên dùng vnstock để lấy dữ liệu public.
- Nếu vnstock lỗi hoặc nguồn dữ liệu thay đổi, script vẫn tạo data.json từ dữ liệu mẫu để web không bị chết.
- Đây là công cụ nghiên cứu, không phải khuyến nghị đầu tư cá nhân hóa.
"""

from __future__ import annotations

import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

VN_TZ = timezone(timedelta(hours=7))

WATCHLIST = [
    "HPG", "MWG", "FPT", "VCB", "MBB", "TCB", "ACB", "SSI", "HCM", "VND",
    "VHM", "KDH", "VIC", "VRE", "GAS", "PVD", "DGC", "GMD", "CTR", "FRT",
    "VNM", "MSN", "DGW", "VJC"
]

SECTORS = {
    "HPG": "Thép / Công nghiệp",
    "MWG": "Bán lẻ",
    "FPT": "Công nghệ",
    "VCB": "Ngân hàng",
    "MBB": "Ngân hàng",
    "TCB": "Ngân hàng",
    "ACB": "Ngân hàng",
    "SSI": "Chứng khoán",
    "HCM": "Chứng khoán",
    "VND": "Chứng khoán",
    "VHM": "Bất động sản",
    "KDH": "Bất động sản",
    "VIC": "Bất động sản / Tập đoàn",
    "VRE": "Bất động sản bán lẻ",
    "GAS": "Dầu khí / Năng lượng",
    "PVD": "Dầu khí",
    "DGC": "Hóa chất",
    "GMD": "Logistics / Cảng biển",
    "CTR": "Hạ tầng viễn thông",
    "FRT": "Bán lẻ",
    "VNM": "Tiêu dùng phòng thủ",
    "MSN": "Tiêu dùng",
    "DGW": "Phân phối / Công nghệ",
    "VJC": "Hàng không",
}

NAMES = {
    "HPG": "Tập đoàn Hòa Phát",
    "MWG": "Thế Giới Di Động",
    "FPT": "FPT Corp",
    "VCB": "Vietcombank",
    "MBB": "Ngân hàng Quân đội",
    "TCB": "Techcombank",
    "ACB": "Ngân hàng Á Châu",
    "SSI": "Chứng khoán SSI",
    "HCM": "Chứng khoán HSC",
    "VND": "Chứng khoán VNDIRECT",
    "VHM": "Vinhomes",
    "KDH": "Khang Điền",
    "VIC": "Vingroup",
    "VRE": "Vincom Retail",
    "GAS": "PV GAS",
    "PVD": "PV Drilling",
    "DGC": "Hóa chất Đức Giang",
    "GMD": "Gemadept",
    "CTR": "Viettel Construction",
    "FRT": "FPT Retail",
    "VNM": "Vinamilk",
    "MSN": "Masan Group",
    "DGW": "Digiworld",
    "VJC": "Vietjet Air",
}

def safe_float(x: Any, ndigits: int = 2) -> float | None:
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        return round(float(x), ndigits)
    except Exception:
        return None

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss.replace(0, pd.NA)
    return 100 - (100 / (1 + rs))

def macd(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    ema12 = series.ewm(span=12, adjust=False).mean()
    ema26 = series.ewm(span=26, adjust=False).mean()
    line = ema12 - ema26
    signal = line.ewm(span=9, adjust=False).mean()
    return line, signal

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    rename = {}
    for want in ["time", "date", "open", "high", "low", "close", "volume"]:
        if want in cols:
            rename[cols[want]] = want
    out = df.rename(columns=rename).copy()
    if "time" not in out.columns and "date" in out.columns:
        out["time"] = out["date"]
    required = ["close", "volume"]
    for col in required:
        if col not in out.columns:
            raise ValueError(f"Thiếu cột {col} trong dữ liệu")
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce")
    out = out.dropna(subset=["close"]).reset_index(drop=True)
    return out

def fetch_history(symbol: str) -> pd.DataFrame:
    # Import trong hàm để nếu vnstock lỗi import, script vẫn fallback được.
    from vnstock import Vnstock

    end = datetime.now(VN_TZ).strftime("%Y-%m-%d")
    start = (datetime.now(VN_TZ) - timedelta(days=430)).strftime("%Y-%m-%d")

    # VCI thường dùng ổn cho dữ liệu lịch sử. Nếu muốn đổi nguồn, sửa tại đây.
    stock = Vnstock().stock(symbol=symbol, source="VCI")
    df = stock.quote.history(start=start, end=end, interval="1D")
    return normalize_columns(df)

def score_stock(symbol: str, df: pd.DataFrame) -> dict[str, Any]:
    close = df["close"]
    volume = df["volume"]

    df["ma20"] = close.rolling(20).mean()
    df["ma50"] = close.rolling(50).mean()
    df["ma200"] = close.rolling(200).mean()
    df["rsi14"] = rsi(close)
    df["macd"], df["macd_signal"] = macd(close)
    df["vol20"] = volume.rolling(20).mean()
    df["high20"] = close.rolling(20).max()

    last = df.iloc[-1]
    prev20 = df.iloc[-21]["close"] if len(df) > 21 else df.iloc[0]["close"]
    prev60 = df.iloc[-61]["close"] if len(df) > 61 else df.iloc[0]["close"]

    c = float(last["close"])
    ma20 = safe_float(last.get("ma20"))
    ma50 = safe_float(last.get("ma50"))
    ma200 = safe_float(last.get("ma200"))
    rsi14 = safe_float(last.get("rsi14"))
    macd_line = safe_float(last.get("macd"))
    macd_signal = safe_float(last.get("macd_signal"))
    vol20 = float(last["vol20"]) if pd.notna(last.get("vol20")) else 0
    v = float(last["volume"]) if pd.notna(last.get("volume")) else 0
    high20 = float(last["high20"]) if pd.notna(last.get("high20")) else c

    ret20 = round((c / float(prev20) - 1) * 100, 2) if prev20 else 0
    ret60 = round((c / float(prev60) - 1) * 100, 2) if prev60 else 0

    score = 50

    # Trend
    if ma20 and c > ma20: score += 8
    if ma50 and c > ma50: score += 8
    if ma200 and c > ma200: score += 8

    # Momentum hợp lý
    if rsi14 is not None:
        if 45 <= rsi14 <= 68: score += 10
        elif 68 < rsi14 <= 75: score += 4
        elif rsi14 > 78: score -= 8
        elif rsi14 < 35: score -= 5

    if macd_line is not None and macd_signal is not None and macd_line > macd_signal:
        score += 8

    # Breakout / gần đỉnh 20 phiên
    if high20 and c >= high20 * 0.97:
        score += 8

    # Volume
    vol_ratio = v / vol20 if vol20 else 1
    if vol_ratio >= 1.5: score += 7
    elif vol_ratio >= 1.1: score += 4

    # Risk penalty
    if ret20 > 18: score -= 8
    if ma50 and c < ma50: score -= 10

    score = max(0, min(100, round(score, 1)))

    if score >= 82:
        action = "MUA TỪNG PHẦN"
        allocation = "15% - 25%"
    elif score >= 74:
        action = "CANH MUA / MUA KHI XÁC NHẬN"
        allocation = "10% - 15%"
    elif score >= 65:
        action = "THEO DÕI MUA"
        allocation = "5% - 10%"
    else:
        action = "CHỜ THÊM"
        allocation = "0% - 5%"

    volume_status = "Rất cao" if vol_ratio >= 1.5 else "Tốt" if vol_ratio >= 1.1 else "Trung bình"

    reason_parts = []
    if ma20 and c > ma20: reason_parts.append("giá trên MA20")
    if ma50 and c > ma50: reason_parts.append("giá trên MA50")
    if rsi14 and 45 <= rsi14 <= 68: reason_parts.append("RSI ở vùng hợp lý")
    if macd_line and macd_signal and macd_line > macd_signal: reason_parts.append("MACD tích cực")
    if vol_ratio >= 1.1: reason_parts.append("thanh khoản cải thiện")
    reason = f"{symbol} đạt {score}/100 điểm: " + (", ".join(reason_parts) if reason_parts else "chưa có nhiều tín hiệu xác nhận") + "."

    if ma20:
        buy_zone = f"Canh quanh MA20 ~ {int(ma20):,} hoặc khi vượt nền kèm thanh khoản.".replace(",", ".")
    else:
        buy_zone = "Canh mua khi hình thành nền giá rõ và thanh khoản xác nhận."

    return {
        "ticker": symbol,
        "name": NAMES.get(symbol, symbol),
        "sector": SECTORS.get(symbol, "Khác"),
        "score": score,
        "action": action,
        "risk": "Cao" if symbol in {"SSI", "HCM", "VND", "PVD", "VHM", "KDH", "VJC"} else "Trung bình",
        "close": safe_float(c, 0),
        "rsi14": rsi14,
        "ret20": ret20,
        "ret60": ret60,
        "volume_status": volume_status,
        "ma20": ma20,
        "ma50": ma50,
        "ma200": ma200,
        "macd": macd_line,
        "macd_signal": macd_signal,
        "reason": reason,
        "buyZone": buy_zone,
        "stopLoss": "-7% đến -10% tùy độ biến động",
        "takeProfit": "+12% đến +25% hoặc dùng trailing stop",
        "allocation": allocation,
        "catalysts": [
            "Xu hướng kỹ thuật",
            "Thanh khoản",
            SECTORS.get(symbol, "Dòng tiền ngành"),
        ],
        "cautions": [
            "Không mua đuổi sau phiên tăng mạnh",
            "Cắt lỗ đúng kỷ luật nếu thủng hỗ trợ",
            "Kiểm tra tin tức và thị trường chung trước khi giải ngân",
        ],
    }

def fallback_data() -> dict[str, Any]:
    fallback = {
        "meta": {
            "updated_at": datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S VN"),
            "source": "fallback sample",
            "note": "Vnstock/API lỗi nên dùng dữ liệu mẫu để web không bị trống.",
        },
        "stocks": [
            {
                "ticker": "HPG", "name": "Tập đoàn Hòa Phát", "sector": "Thép / Công nghiệp",
                "score": 88, "action": "MUA TỪNG PHẦN", "risk": "Trung bình",
                "close": 29500, "rsi14": 61, "ret20": 8.4, "ret60": 15.2,
                "volume_status": "Cao", "ma20": 28600, "ma50": 27400, "ma200": 25600,
                "macd": 0.8, "macd_signal": 0.5,
                "reason": "Dữ liệu mẫu: xu hướng phục hồi tốt, thanh khoản tích cực.",
                "buyZone": "Canh mua quanh nền tích lũy hoặc khi retest MA20.",
                "stopLoss": "-7% đến -10%", "takeProfit": "+15% đến +25%", "allocation": "20% - 25%",
                "catalysts": ["Chu kỳ thép", "Thanh khoản", "Dòng tiền lớn"],
                "cautions": ["Không mua đuổi", "Biến động giá thép", "Theo dõi thị trường chung"],
            },
            {
                "ticker": "MWG", "name": "Thế Giới Di Động", "sector": "Bán lẻ",
                "score": 84, "action": "MUA KHI TÍCH LŨY", "risk": "Trung bình",
                "close": 68200, "rsi14": 58, "ret20": 6.1, "ret60": 12.5,
                "volume_status": "Tốt", "ma20": 66100, "ma50": 63700, "ma200": 58800,
                "macd": 1.2, "macd_signal": 0.9,
                "reason": "Dữ liệu mẫu: lợi nhuận cải thiện, phù hợp mua khi tích lũy.",
                "buyZone": "Mua quanh nền tích lũy sau nhịp tăng.",
                "stopLoss": "-7% đến -8%", "takeProfit": "+15% đến +22%", "allocation": "15% - 20%",
                "catalysts": ["BHX cải thiện", "Biên lợi nhuận", "Tiêu dùng phục hồi"],
                "cautions": ["Định giá phản ánh kỳ vọng", "Sức mua tiêu dùng", "Không mua đuổi"],
            }
        ],
    }
    return fallback

def main() -> None:
    results: list[dict[str, Any]] = []
    errors: dict[str, str] = {}

    for symbol in WATCHLIST:
        try:
            df = fetch_history(symbol)
            if len(df) < 60:
                raise ValueError("Dữ liệu quá ít")
            item = score_stock(symbol, df)
            results.append(item)
            print(f"OK {symbol}: {item['score']}")
        except Exception as exc:
            errors[symbol] = str(exc)
            print(f"ERROR {symbol}: {exc}")

    if not results:
        data = fallback_data()
    else:
        results = sorted(results, key=lambda x: x.get("score", 0), reverse=True)
        data = {
            "meta": {
                "updated_at": datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S VN"),
                "source": "vnstock public data",
                "universe": len(WATCHLIST),
                "success": len(results),
                "errors": errors,
                "note": "Dữ liệu phục vụ nghiên cứu, không phải khuyến nghị đầu tư cá nhân hóa.",
            },
            "stocks": results,
        }

    Path("data.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote data.json with {len(data['stocks'])} stocks")

if __name__ == "__main__":
    main()
