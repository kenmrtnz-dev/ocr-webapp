import cv2
import numpy as np

def detect_rows(image_path, min_height=25, padding=5):
    img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)

    # Binary image
    thresh = cv2.adaptiveThreshold(
        img,
        255,
        cv2.ADAPTIVE_THRESH_MEAN_C,
        cv2.THRESH_BINARY_INV,
        31,
        15
    )

    # Horizontal projection
    projection = np.sum(thresh, axis=1)

    rows = []
    in_row = False
    start = 0

    for y, value in enumerate(projection):
        if value > 0 and not in_row:
            start = y
            in_row = True
        elif value == 0 and in_row:
            end = y
            if end - start >= min_height:
                rows.append((start, end))
            in_row = False

    return rows
