# VN Stock Picker Dashboard

Dashboard lọc mã cổ phiếu Việt Nam theo điểm số, chạy bằng **GitHub Pages** và tự cập nhật dữ liệu bằng **GitHub Actions**.

## Cấu trúc

```txt
index.html                  # Giao diện web
data.json                   # Dữ liệu dashboard
update_data.py              # Script lấy dữ liệu và tính điểm
requirements.txt            # Thư viện Python
.github/workflows/update-data.yml
```

## Cách dùng nhanh trên GitHub

1. Tạo repo mới trên GitHub, ví dụ: `vn-stock-picker`.
2. Upload toàn bộ file trong project này lên repo.
3. Vào **Settings → Pages**.
4. Chọn:
   - Source: `Deploy from a branch`
   - Branch: `main`
   - Folder: `/root`
5. Bấm Save.
6. Mở link dạng:

```txt
https://username.github.io/vn-stock-picker/
```

## Chạy cập nhật thủ công

Vào tab **Actions** → chọn **Update stock data** → bấm **Run workflow**.

Workflow cũng tự chạy mỗi ngày lúc **08:30 sáng giờ Việt Nam**, từ thứ 2 đến thứ 6.

## Chạy local

```bash
pip install -r requirements.txt
python update_data.py
```

Sau đó mở `index.html`.

## Thêm mã cổ phiếu

Mở `update_data.py`, sửa danh sách:

```python
WATCHLIST = [
    "HPG", "MWG", "FPT", ...
]
```

Có thể bổ sung ngành ở `SECTORS` và tên công ty ở `NAMES`.

## Lưu ý

- Đây là project nghiên cứu, không phải khuyến nghị đầu tư cá nhân hóa.
- Dữ liệu lấy qua thư viện public có thể thay đổi, lỗi hoặc bị giới hạn.
- Nếu nguồn dữ liệu lỗi, script sẽ tạo dữ liệu mẫu để web không bị trống.
