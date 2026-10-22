# HDoi và schema DataDongBo

Worker dùng `BUSINESS_DATA_PROVIDER=datadongbo`, `DATA_DONG_BO_URL` và `DATA_DONG_BO_TOKEN` cùng cấu hình backend. Không dùng token ở frontend.

Triển khai DataDongBo và chạy kiểm tra/nâng cấp schema trước khi mở lại ghi. Lệnh maintenance ở dự án DataDongBo: `node scripts/upgrade-cham-cong-schema.mjs` (chỉ kiểm tra), thêm `--apply` để áp dụng trong cửa sổ bảo trì. Không sửa/xóa job cũ để đổi headers hay đổi provider.

Processor giữ nguyên headers của từng job và requestId cố định khi thử lại. DataDongBo mới hỗ trợ job schema cũ cho các cột xe bổ sung cuối bảng; Seal của container khác không được giữ nhầm khi job đổi container. Job thiếu binding provider, sai ID hoặc cấu trúc không hợp lệ bị chặn trước transport. Chỉ xác nhận hiệu ứng khi receipt MySQL có `durable:true`; chưa đồng nghĩa Google Sheets đã đồng bộ.

Các job lỗi trước đó có thể cần đối soát rồi thử lại bằng cùng job/requestId. Không tự tạo mã mới hoặc bỏ kiểm tra schema để vượt lỗi.
