import os, json
import cv2
from celery import Celery
from pdf2image import convert_from_path
from app.image_cleaner import clean_page
from app.ocr_engine import ocr_image
from app.row_detector import detect_rows
from app.row_cropper import crop_rows
from app.ocr_rows import ocr_row
from app.column_parser import parse_row


DATA_DIR = os.getenv("DATA_DIR", "/data")

celery = Celery(
    "ocr_worker",
    broker=os.getenv("CELERY_BROKER_URL"),
    backend=os.getenv("CELERY_RESULT_BACKEND"),
)

@celery.task
def process_pdf(job_id: str):
    print(f"[WORKER] Starting job {job_id}")

    job_dir = os.path.join(DATA_DIR, "jobs", job_id)
    input_pdf = os.path.join(job_dir, "input", "document.pdf")

    # 🔒 GUARANTEE base dir exists
    os.makedirs(job_dir, exist_ok=True)

    pages_dir = os.path.join(job_dir, "pages")
    cleaned_dir = os.path.join(job_dir, "cleaned")

    os.makedirs(pages_dir, exist_ok=True)
    os.makedirs(cleaned_dir, exist_ok=True)

    # ===============================
    # STEP 1: PDF → PAGE IMAGES
    # ===============================
    update_status(job_dir, "processing", step="pdf_to_images")

    pages = convert_from_path(
        input_pdf,
        dpi=200,
        fmt="png"
    )

    for i, page in enumerate(pages, start=1):
        page_path = os.path.join(pages_dir, f"page_{i:03}.png")
        page.save(page_path)

    # ===============================
    # STEP 2: CLEAN PAGE IMAGES
    # ===============================
    update_status(job_dir, "processing", step="image_cleaning")

    for page_file in sorted(os.listdir(pages_dir)):
        src = os.path.join(pages_dir, page_file)
        dst = os.path.join(cleaned_dir, page_file)

        cleaned = clean_page(src)
        cv2.imwrite(dst, cleaned)
    

    rows_base = os.path.join(job_dir, "rows")
    ocr_dir = os.path.join(job_dir, "ocr")
    os.makedirs(rows_base, exist_ok=True)
    os.makedirs(ocr_dir, exist_ok=True)

    update_status(job_dir, "processing", step="row_detection")

    row_ocr_output = {}

    for page_file in sorted(os.listdir(cleaned_dir)):
        page_name = page_file.replace(".png", "")
        page_path = os.path.join(cleaned_dir, page_file)

        page_rows_dir = os.path.join(rows_base, page_name)

        # 1️⃣ detect row boundaries
        bounds = detect_rows(page_path)

        # 2️⃣ crop rows
        crop_rows(page_path, bounds, page_rows_dir)

    # 3️⃣ OCR each row
    rows_text = []
    for row_file in sorted(os.listdir(page_rows_dir)):
        row_path = os.path.join(page_rows_dir, row_file)
        text = ocr_row(row_path)
        rows_text.append({
            "row": row_file,
            "text": text
        })

    row_ocr_output[page_name] = rows_text

    # Save row OCR
    with open(os.path.join(ocr_dir, "rows.json"), "w") as f:
        json.dump(row_ocr_output, f, indent=2)

    parsed_output = {}

    for page, rows in row_ocr_output.items():
        parsed_rows = []
        for r in rows:
            parsed = parse_row(r["text"])
            parsed["row_id"] = r["row"].replace("row_", "").replace(".png", "")
            parsed_rows.append(parsed)

        parsed_output[page] = parsed_rows

    result_dir = os.path.join(job_dir, "result")
    os.makedirs(result_dir, exist_ok=True)
    with open(os.path.join(result_dir, "parsed_rows.json"), "w") as f:
        json.dump(parsed_output, f, indent=2)
    
    # in process_pdf (celery_app.py)
    bounds_output = {}
    for page_file in sorted(os.listdir(cleaned_dir)):
        page_name = page_file.replace(".png", "")
        page_path = os.path.join(cleaned_dir, page_file)
        row_bounds = detect_rows(page_path)
        bounds_output[page_name] = [
            {"row_id": idx + 1, "y1": y1, "y2": y2}
            for idx, (y1, y2) in enumerate(row_bounds)
        ]
        crop_rows(page_path, row_bounds, os.path.join(rows_base, page_name))
    result_dir = os.path.join(job_dir, "result")
    os.makedirs(result_dir, exist_ok=True)
    with open(os.path.join(result_dir, "bounds.json"), "w") as f:
        json.dump(bounds_output, f, indent=2)


    # ===============================
    # STEP 3: FINISH JOB
    # ===============================
    update_status(job_dir, "done", pages=len(pages))

    print(f"[WORKER] Finished job {job_id}")

    return {"pages": len(pages)}


# --------------------------------
# ATOMIC STATUS UPDATE (CRITICAL)
# --------------------------------
def update_status(job_dir, status, **extra):
    tmp_path = os.path.join(job_dir, "status.tmp")
    final_path = os.path.join(job_dir, "status.json")

    data = {"status": status, **extra}

    with open(tmp_path, "w") as f:
        json.dump(data, f)

    # Atomic replace (Linux-safe)
    os.replace(tmp_path, final_path)
