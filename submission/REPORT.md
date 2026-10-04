# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Phạm Khắc Tú / 2A202602866

**Repo:** https://github.com/TuTu99999/K4-Track02-Day17-PhamKhacTu-2A202602866-DataPipelineEngineering

**Commit mã nguồn đã kiểm tra:** `a5ea55a128ee681f845ca4172dc7306fef2f3c19`

**AI đã dùng và phạm vi hỗ trợ:** OpenAI Codex hỗ trợ đọc repo, chẩn đoán ba lỗi, sửa logic trong `pipeline/`, chạy kiểm chứng và soạn bản nháp báo cáo; tôi đã review diff và output thực tế.

**Nguồn tham khảo khác (nếu có):** Không.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | 24 hàng/12 ID; T-91 có ba trạng thái; rerun đổi checksum. | Feature lệch full recompute; u05 ngày 08-12 chỉ có `(2,1,0)` thay vì `(5,3,1)`. | T-97 vẫn còn ở Silver, snapshot mới nhất và RAG. |
| **Nguyên nhân gốc** | Dùng `INSERT`, không upsert theo khoá/LSN. | Chỉ tính lại ngày ingest dù event đến trễ ba ngày. | Delete có `after=null`, nhưng staging chỉ lấy khoá từ `after`. |
| **Cách sửa** | `silver.py`: `MERGE ON ticket_id`, chỉ update khi LSN nguồn lớn hơn. | `config.py`: `LOOKBACK_DAYS=3`, bằng `ceil(P99)` đo từ Bronze. | `staging.py`: `coalesce(after.ticket_id,before.ticket_id)` để giữ delete. |
| **Khái niệm** | Keyed upsert, idempotency, out-of-order. | Event time, late data, overwrite-partition. | CDC `before/after/op`, tombstone, xoá lan truyền. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`.
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f`.
- `make parity`: **PARITY**.

## 3. Lựa chọn công cụ / kỹ thuật

- **MERGE / overwrite-partition:** ticket là mutable entity nên upsert trạng thái theo khoá/LSN; feature ngày có late event nên tính lại cửa sổ partition.
- **Tombstone:** giữ bằng chứng xoá cho downstream, xoá PII và ngăn replay cũ hồi sinh ticket.
- **Snapshot “as of”:** mỗi version tái lập đúng dữ liệu đã biết lúc đó, tránh leakage và giữ thí nghiệm reproducible.
- **DuckDB/dbt:** dữ liệu nhỏ, local nên không cần chi phí Spark. dbt dùng `unique_key` và `merge_update_condition` cho MERGE; `batch_size='day'`, `lookback=3` cho microbatch event-time.

## 4. Hai câu hỏi suy ngẫm

1. Quyền xoá ưu tiên hơn bất biến vật lý. Snapshot chỉ nên giữ surrogate key/token; PII nằm trong kho riêng có TTL hoặc mã hoá theo subject. Khi xoá, phát tombstone đến mọi dataset/index, xoá payload hoặc khoá mã hoá, và giữ audit metadata không chứa PII. Snapshot đã chứa PII phải được redact/rebuild có ghi lineage.
2. Tôi đặt chốt PII tại Bronze → Silver: regex kết hợp NER/DLP tiếng Việt; kết quả chắc chắn được mask/tokenize, trường hợp mơ hồ vào quarantine. Tôi đo precision/recall trên tập gán nhãn, `PII leak rate` sau Bronze, false-positive và tỷ lệ quarantine theo ngày; lệch baseline sẽ cảnh báo.

## 5. Output

```text
PS> .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

PS> .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 3.63s

PS> .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

PS> .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

PS> cd dbt_project
PS> $env:DBT_PROFILES_DIR='.'
PS> ..\.venv\Scripts\dbt.exe --no-use-colors build --event-time-start 2026-08-10 --event-time-end 2026-08-17
Running with dbt=1.12.5
Registered adapter: duckdb=1.11.0
Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 2.03 seconds (2.03s).
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

PS> cd ..
PS> .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
