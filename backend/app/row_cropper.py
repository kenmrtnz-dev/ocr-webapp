import cv2
import os

def crop_rows(page_image_path, row_bounds, out_dir, padding=5):
    img = cv2.imread(page_image_path)

    h, w, _ = img.shape
    os.makedirs(out_dir, exist_ok=True)

    for idx, (y1, y2) in enumerate(row_bounds, start=1):
        top = max(0, y1 - padding)
        bottom = min(h, y2 + padding)

        row_img = img[top:bottom, 0:w]
        out_path = os.path.join(out_dir, f"row_{idx:03}.png")
        cv2.imwrite(out_path, row_img)
