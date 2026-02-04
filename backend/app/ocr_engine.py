import easyocr
import json

_reader = None

def get_reader():
    global _reader
    if _reader is None:
        _reader = easyocr.Reader(['en'], gpu=False)
    return _reader


def ocr_image(image_path):
    reader = get_reader()

    results = reader.readtext(image_path)

    ocr_items = []
    for idx, (bbox, text, conf) in enumerate(results, start=1):
        ocr_items.append({
            "id": idx,
            "text": text,
            "confidence": float(conf),
            "bbox": bbox  # 4 points
        })

    return ocr_items
