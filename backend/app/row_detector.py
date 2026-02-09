import cv2
import numpy as np

def detect_rows(image_path, min_height=20, padding=4):
    img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
    if img is None:
        return []

    # Inverted binary map of dark text pixels.
    bw = cv2.adaptiveThreshold(
        img,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY_INV,
        31,
        12,
    )

    h, w = bw.shape[:2]
    if h <= 0 or w <= 0:
        return []

    # Remove tiny speckles that connect unrelated rows.
    speck_kernel = np.ones((2, 2), dtype=np.uint8)
    bw = cv2.morphologyEx(bw, cv2.MORPH_OPEN, speck_kernel, iterations=1)

    # Count non-zero text pixels for each y-row.
    row_counts = np.count_nonzero(bw, axis=1).astype(np.float32)

    # Dynamic threshold with floor: keep rows with enough ink.
    mean_count = float(np.mean(row_counts))
    thresh = max(6.0, mean_count * 0.35)
    active = row_counts >= thresh

    runs = _active_runs(active)
    if not runs:
        return []

    # Merge runs separated by small whitespace gaps.
    merged = []
    for start, end in runs:
        if not merged:
            merged.append([start, end])
            continue
        prev = merged[-1]
        gap = start - prev[1]
        if gap <= 7:
            prev[1] = end
        else:
            merged.append([start, end])

    rows = []
    for start, end in merged:
        if (end - start) < min_height:
            continue
        top = max(0, int(start) - int(padding))
        bottom = min(h, int(end) + int(padding))
        rows.append((top, bottom))

    return rows


def _active_runs(active_mask):
    runs = []
    in_run = False
    start = 0
    for y, is_active in enumerate(active_mask):
        if is_active and not in_run:
            start = y
            in_run = True
        elif not is_active and in_run:
            runs.append((start, y))
            in_run = False
    if in_run:
        runs.append((start, len(active_mask)))
    return runs
