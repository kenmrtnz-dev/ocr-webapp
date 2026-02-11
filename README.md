# OCR Webapp

Bank statement extraction app with a FastAPI backend, Celery worker pipeline, and browser UI.

The app processes PDF statements and outputs structured rows:
- `date`
- `description`
- `debit`
- `credit`
- `balance`

It supports:
- Text-layer parsing for digital PDFs
- OCR parsing for scanned PDFs
- Profile-based bank parsing (`bank_profiles.json`)
- Optional AI analyzer (Gemini) to auto-create new bank profiles when no profile matches
- Visual page preview with row bounding boxes
- In-browser row editing and PDF export of summary + tables

---

## 1. Project Structure

```text
ocr-webapp-1/
  backend/
    app/
      main.py                 # FastAPI routes + UI serving
      celery_app.py           # Async processing pipeline
      statement_parser.py     # Core parser and normalization
      bank_profiles.py        # Profile loading/detection
      bank_profiles.json      # Profile config and detection rules
      profile_analyzer.py     # AI unknown-bank analyzer + profile auto-apply
      ocr_engine.py           # OCR backend wrapper(s)
      pdf_text_extract.py     # Text-layer extraction
      image_cleaner.py        # Page cleanup/preprocessing
      static/                 # Frontend JS/CSS
      templates/index.html    # Main UI page
    requirements.txt
    Dockerfile
  docker-compose.yml
  .env.example
```

---

## 2. Requirements

### Option A: Docker (recommended)
- Docker Desktop
- Docker Compose

### Option B: Local runtime
- Python 3.11
- Poppler (`pdf2image` dependency)
- Tesseract (if you use it)
- Redis (for Celery broker/backend)

---

## 3. Quick Start (Docker)

1. Copy env file:

```bash
cp .env.example .env
```

2. Set required keys in `.env`:
- `GEMINI_API_KEY` (if AI analyzer is enabled)

3. Build and run:

```bash
docker compose up --build
```

4. Open:
- UI: `http://localhost:8000`
- Health: `http://localhost:8000/health`

---

## 4. Configuration

The app reads config from `.env`.

### Core
- `DATA_DIR=/data`
- `CELERY_BROKER_URL=redis://redis:6379/0`
- `CELERY_RESULT_BACKEND=redis://redis:6379/0`
- `CELERY_CONCURRENCY=4`
- `CELERY_MAX_TASKS_PER_CHILD=25`

### Processing / Preview
- `PREVIEW_DPI` (default in code: `130`)
- `PREVIEW_DRAFT_DPI` (default in code: `100`)
- `PREVIEW_MAX_PIXELS` (default in code: `6000000`)

### AI Profile Analyzer (Gemini)
- `AI_ANALYZER_ENABLED=true`
- `AI_ANALYZER_PROVIDER=gemini`
- `AI_ANALYZER_MODEL=gemini-2.5-flash`
- `AI_ANALYZER_TIMEOUT_SEC=20`
- `AI_ANALYZER_SAMPLE_PAGES=3`
- `AI_ANALYZER_MIN_ROWS=3`
- `AI_ANALYZER_MIN_DATE_RATIO=0.80`
- `AI_ANALYZER_MIN_BAL_RATIO=0.80`
- `GEMINI_API_KEY=...`

### Bank profiles config path
- `BANK_PROFILES_CONFIG` (optional)
  - If not set, app uses `/data/config/bank_profiles.json` and seeds from packaged config when needed.

---

## 5. How Processing Works

1. Upload PDF (`/jobs`) with parse mode:
- `text` (default): parse text layer
- `ocr`: OCR each page

2. Worker pipeline:
- load PDF text layouts (if available)
- optional analyzer pass (unknown-bank case only)
- parse page rows via profile-aware parser
- filter to transaction rows
- write outputs and diagnostics

3. Results are saved under:
- `/data/jobs/<job_id>/...`

---

## 6. API Endpoints

### UI / Health
- `GET /` -> web UI
- `GET /health` -> `{ "ok": true }`

### Jobs
- `POST /jobs` -> start processing immediately
  - form fields: `file` (PDF), `mode` (`text|ocr`)
- `POST /jobs/draft` -> create draft and preprocess pages
- `POST /jobs/{job_id}/start` -> start processing from draft
- `GET /jobs/{job_id}` -> job status (`queued|processing|done|failed|draft`)

### Outputs
- `GET /jobs/{job_id}/cleaned`
- `GET /jobs/{job_id}/cleaned/{filename}`
- `GET /jobs/{job_id}/preview/{page}`
- `GET /jobs/{job_id}/ocr/{page}`
- `GET /jobs/{job_id}/parsed/{page}`
- `GET /jobs/{job_id}/parsed`
- `GET /jobs/{job_id}/rows/{page}/bounds`
- `GET /jobs/{job_id}/bounds`
- `GET /jobs/{job_id}/diagnostics`

### Page editing
- `POST /jobs/{job_id}/pages/{page}/flatten`
- `POST /jobs/{job_id}/pages/{page}/flatten/reset`

### Deprecated row-level endpoints
Return `410 Gone`:
- `/jobs/{job_id}/rows/{page}/{row}`
- `/jobs/{job_id}/ocr/rows`
- `/jobs/{job_id}/rows/{page}`

---

## 7. Output Artifacts

Inside `/data/jobs/<job_id>/result`:
- `parsed_rows.json` -> parsed rows by page
- `bounds.json` -> row bounding boxes by page
- `parse_diagnostics.json` -> parser/source metadata
- `profile_update.json` -> analyzer outcome when analyzer is triggered

Other folders:
- `input/document.pdf`
- `pages/` (rendered pages)
- `cleaned/` (processed page images)
- `ocr/` (page OCR JSON)
- `status.json` (live status used by UI polling)

---

## 8. Frontend Features

- Upload + parse mode toggle (`Text` / `OCR`)
- Progress bar + elapsed time + analyzer status
- Page preview with zoom/pan + row bounding boxes
- Editable extracted table (insert/delete row)
- Summary panel + monthly summary
- Export to PDF:
  - Account Summary
  - Monthly Summary
  - Transactions table

---

## 9. Bank Profiles

Profiles define:
- header tokens (date/description/debit/credit/balance)
- date parse order (`mdy|dmy|ymd`)
- noise tokens
- account name/number regex patterns
- detection rules (`contains_any`, `contains_all`)

Main files:
- `backend/app/bank_profiles.json`
- `backend/app/bank_profiles.py`

---

## 10. AI Analyzer Behavior

When enabled:
1. Sample first pages with text layout.
2. If all sampled pages detect as `GENERIC`, analyzer runs.
3. Gemini proposes a new profile JSON.
4. Proposal is validated with parser quality gates.
5. On success, profile is atomically appended to config and reloaded.
6. On failure (missing key/network/invalid output), processing continues (fail-open).

---

## 11. Troubleshooting

### `.env` safety
- Do **not** commit `.env`.
- Keep secrets only in local `.env` and rotate keys if exposed.

### Analyzer stuck at Idle
- Check `AI_ANALYZER_ENABLED=true`
- Check `GEMINI_API_KEY`
- Check job diagnostics: `/jobs/{job_id}/diagnostics`

### OCR is slow
- Use `mode=text` for digital PDFs
- Reduce DPI (`PREVIEW_DPI`, `PREVIEW_DRAFT_DPI`)
- Increase worker concurrency if CPU allows

### Preview images missing
- Check `/jobs/{job_id}/cleaned`
- Ensure page rendering succeeded (`pdf2image` + poppler present)

### Worker not processing
- Verify Redis is running
- Check worker logs
- Confirm `CELERY_BROKER_URL` and `CELERY_RESULT_BACKEND`

---

## 12. Development Notes

- Backend app mount in `docker-compose.yml` is read-only (`./backend/app:/app/app:ro`).
- Runtime-write artifacts should go under `/data` volume.
- Keep API response shapes backward-compatible for UI polling and rendering.

---

## 13. License

No license file is currently defined in this repository.

