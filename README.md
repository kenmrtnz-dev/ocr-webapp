# OCR Webapp

Bank statement extraction platform with FastAPI, Celery, and a browser UI for three roles:
- Agent
- Credit Evaluator
- Admin

Core extracted fields:
- `date`
- `description`
- `debit`
- `credit`
- `balance`

## Overview
The app supports both digital and scanned statements:
- Text parsing for digital PDFs
- OCR parsing for scanned PDFs
- Profile-driven bank parsing (`bank_profiles.json`)
- Optional Gemini-based profile analyzer for unknown formats

Main UI capabilities:
- Agent upload workflow with multi-file submission
- Evaluator review/editor with preview + table + summary
- Export to PDF and Excel
- Admin user/role management and submission-data reset

## Project Structure
```text
ocr-webapp-1/
  backend/
    app/
      main.py
      celery_app.py
      statement_parser.py
      bank_profiles.py
      bank_profiles.json
      profile_analyzer.py
      ocr_engine.py
      pdf_text_extract.py
      image_cleaner.py
      auth_service.py
      db.py
      workflow_models.py
      workflow_service.py
      blob_store.py
      static/
        app.js
        login.js
        admin.js
        styles.css
      templates/
        login.html
        agent.html
        evaluator.html
        admin.html
        index.html
    requirements.txt
    Dockerfile
  docker-compose.yml
  .env.example
```

## Requirements
### Docker (recommended)
- Docker Desktop
- Docker Compose

### Local runtime
- Python 3.11+
- Poppler (`pdf2image`)
- Redis
- PostgreSQL (or SQLite fallback if configured)

## Quick Start (Docker)
1. Copy env file:
```bash
cp .env.example .env
```

2. Set required values in `.env`.

3. Build and run:
```bash
docker compose up --build
```

4. Open:
- App: `http://localhost:8000`
- Health: `http://localhost:8000/health`

## Default Login Accounts
Configured by env and seeded at startup:
- Agent: `agent@example.com` / `agent123`
- Evaluator: `evaluator@example.com` / `evaluator123`
- Admin: `admin@example.com` / `admin123`

Change via `.env`:
- `DEFAULT_AGENT_EMAIL`, `DEFAULT_AGENT_PASSWORD`
- `DEFAULT_EVALUATOR_EMAIL`, `DEFAULT_EVALUATOR_PASSWORD`
- `DEFAULT_ADMIN_EMAIL`, `DEFAULT_ADMIN_PASSWORD`

## Portals
- `/login` -> login page
- `/agent` -> Agent portal
- `/evaluator` -> Credit Evaluator portal
- `/admin` -> Admin portal
- `/app` -> unified/legacy page (compat)

## Workflow Lifecycle
Submission statuses:
- `for_review`
- `processing`
- `summary_generated`
- `failed`

Current flow:
1. Agent uploads files.
2. Submission starts as `for_review`.
3. Evaluator assigns and clicks `Open`.
4. Processing starts on open and status switches to `processing`.
5. After export (PDF/Excel), status becomes `summary_generated`.

## Agent Workflow
Agent can:
- Upload one or multiple PDFs
- Set borrower name and lead reference
- See submitted files table
- Search by borrower name

Agent cannot:
- Edit parsed rows
- Run evaluator analysis actions

## Evaluator Workflow
Evaluator can:
- View assigned/unassigned queue
- Assign submissions
- Open submission to trigger processing
- Review preview and extracted table
- Edit rows in-table
- Export to PDF/Excel

## Admin Workflow
Admin can:
- List users
- Add users with role (`agent`, `credit_evaluator`, `admin`)
- Deactivate users
- Reactivate users
- Clear all submitted workflow data

Admin clear action removes:
- submissions
- jobs
- transactions
- reports
- audit logs
- `/data/jobs/*`
- `/data/reports/*`

## API Reference
### Auth
- `POST /auth/login`
- `GET /auth/me`

### Agent
- `POST /agent/submissions`
- `GET /agent/submissions`
- `GET /agent/submissions/{submission_id}`
- `GET /agent/submissions/{submission_id}/status`

### Evaluator
- `GET /evaluator/submissions?include_unassigned=true|false`
- `POST /evaluator/submissions/{submission_id}/assign`
- `GET /evaluator/submissions/{submission_id}`
- `PATCH /evaluator/submissions/{submission_id}/transactions`
- `POST /evaluator/submissions/{submission_id}/analyze`
- `POST /evaluator/submissions/{submission_id}/reports`
- `GET /evaluator/reports/{report_id}/download`
- `POST /evaluator/submissions/{submission_id}/mark-summary-generated`

### Admin
- `GET /admin/users`
- `POST /admin/users`
- `DELETE /admin/users/{user_id}` (deactivate)
- `PATCH /admin/users/{user_id}/active` (activate/deactivate)
- `POST /admin/clear-submissions`

### Jobs / OCR Outputs
- `POST /jobs`
- `POST /jobs/draft`
- `POST /jobs/{job_id}/start`
- `GET /jobs/{job_id}`
- `GET /jobs/{job_id}/cleaned`
- `GET /jobs/{job_id}/cleaned/{filename}`
- `GET /jobs/{job_id}/preview/{page}`
- `GET /jobs/{job_id}/ocr/{page}`
- `GET /jobs/{job_id}/parsed/{page}`
- `GET /jobs/{job_id}/parsed`
- `GET /jobs/{job_id}/rows/{page}/bounds`
- `GET /jobs/{job_id}/bounds`
- `GET /jobs/{job_id}/diagnostics`
- `POST /jobs/{job_id}/pages/{page}/flatten`
- `POST /jobs/{job_id}/pages/{page}/flatten/reset`

Deprecated row-level endpoints return `410`.

## Configuration
Common env vars:
- `DATA_DIR`
- `DATABASE_URL`
- `CELERY_BROKER_URL`
- `CELERY_RESULT_BACKEND`
- `CELERY_CONCURRENCY`
- `CELERY_MAX_TASKS_PER_CHILD`
- `JWT_SECRET`
- `JWT_ISS`
- `JWT_EXP_SECONDS`

Preview/processing controls:
- `PREVIEW_DPI`
- `PREVIEW_DRAFT_DPI`
- `PREVIEW_MAX_PIXELS`

AI analyzer controls:
- `AI_ANALYZER_ENABLED`
- `AI_ANALYZER_PROVIDER` (`gemini`)
- `AI_ANALYZER_MODEL`
- `AI_ANALYZER_TIMEOUT_SEC`
- `AI_ANALYZER_SAMPLE_PAGES`
- `AI_ANALYZER_MIN_ROWS`
- `AI_ANALYZER_MIN_DATE_RATIO`
- `AI_ANALYZER_MIN_BAL_RATIO`
- `GEMINI_API_KEY`

Bank profile config path:
- `BANK_PROFILES_CONFIG`

## Data and Artifacts
Runtime artifacts are stored in `DATA_DIR`:
- `jobs/<job_id>/input/document.pdf`
- `jobs/<job_id>/pages`
- `jobs/<job_id>/cleaned`
- `jobs/<job_id>/preview`
- `jobs/<job_id>/ocr`
- `jobs/<job_id>/result/parsed_rows.json`
- `jobs/<job_id>/result/bounds.json`
- `jobs/<job_id>/result/parse_diagnostics.json`
- `jobs/<job_id>/status.json`
- `reports/...`

## Troubleshooting
- If UI doesn’t reflect backend edits, restart API/worker and hard-refresh browser.
- If OCR appears stuck, check `/jobs/{job_id}` and worker logs.
- If analyzer remains idle, validate `AI_ANALYZER_ENABLED` and `GEMINI_API_KEY`.
- Never commit `.env`; rotate keys if exposed.
