import math
import os
import tempfile

import cv2

from app.ocr_engine import ocr_image


def ocr_row(image_path, backend="paddleocr"):
    prepared_path = image_path
    pad_meta = None
    temp_path = None

    try:
        prepared_path, temp_path, pad_meta = _prepare_row_image(image_path)
        items = ocr_image(
            prepared_path,
            backend=backend,
            max_width=900,
            max_pixels=1_200_000,
            chunk_height=620,
            chunk_overlap=60,
        )

        if pad_meta is not None:
            pad_left, pad_top, orig_w, orig_h = pad_meta
            adjusted = []
            for item in items:
                bbox = item.get("bbox") or []
                if len(bbox) != 4:
                    continue
                new_bbox = []
                for pt in bbox:
                    x = max(0.0, min(float(orig_w), float(pt[0]) - float(pad_left)))
                    y = max(0.0, min(float(orig_h), float(pt[1]) - float(pad_top)))
                    new_bbox.append([x, y])
                copied = dict(item)
                copied["bbox"] = new_bbox
                adjusted.append(copied)
            items = adjusted

        text = " ".join((item.get("text") or "").strip() for item in items if item.get("text")).strip()
        return text, items
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass


def _prepare_row_image(image_path, max_ratio=3.0):
    img = cv2.imread(image_path)
    if img is None:
        return image_path, None, None

    h, w = img.shape[:2]
    if h <= 0 or w <= 0:
        return image_path, None, None

    if (w / float(h)) <= max_ratio and (h / float(w)) <= max_ratio:
        return image_path, None, None

    pad_top = pad_bottom = pad_left = pad_right = 0

    if w / float(h) > max_ratio:
        target_h = int(math.ceil(w / max_ratio))
        pad_total = max(0, target_h - h)
        pad_top = pad_total // 2
        pad_bottom = pad_total - pad_top
    else:
        target_w = int(math.ceil(h / max_ratio))
        pad_total = max(0, target_w - w)
        pad_left = pad_total // 2
        pad_right = pad_total - pad_left

    padded = cv2.copyMakeBorder(
        img,
        pad_top,
        pad_bottom,
        pad_left,
        pad_right,
        cv2.BORDER_CONSTANT,
        value=(255, 255, 255),
    )

    tmp = tempfile.NamedTemporaryFile(prefix="rowpad_", suffix=".png", delete=False)
    tmp_path = tmp.name
    tmp.close()
    cv2.imwrite(tmp_path, padded)
    return tmp_path, tmp_path, (pad_left, pad_top, w, h)
