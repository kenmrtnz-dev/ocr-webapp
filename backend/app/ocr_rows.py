import easyocr
import cv2

_reader = None

def get_reader():
    global _reader
    if _reader is None:
        _reader = easyocr.Reader(['en'], gpu=False)
    return _reader

def ocr_row(image_path):
    reader = get_reader()

    img = cv2.imread(image_path)
    h, w = img.shape[:2]

    if w > 1200:
        scale = 1200 / w
        img = cv2.resize(img, None, fx=scale, fy=scale)

    results = reader.readtext(img, detail=0)
    return " ".join(results).strip()
