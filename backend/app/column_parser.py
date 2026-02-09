import re
from typing import Optional

from app.statement_parser import normalize_date

DATE_RE = re.compile(
    r"\b(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{1,2}\s+[A-Za-z]{3}\s+\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2})\b"
)
AMOUNT_RE = re.compile(r"(?<![A-Za-z0-9])\(?-?\$?\s*[\d][\d,.OSos]*\)?(?![A-Za-z0-9])")
DEBIT_HINT_RE = re.compile(r"\b(debit|debits|withdraw|wd|db)\b", re.IGNORECASE)
CREDIT_HINT_RE = re.compile(r"\b(credit|credits|deposit|cr)\b", re.IGNORECASE)
DATE_CHUNK_RE = re.compile(r"\b(?:\d{1,2}/\d{1,2}/\d{2,4}|\d{8})\b")

DEBIT_KEYWORDS = (
    "debit", "debits", "withdraw", "sending", "service charge",
    "fee", "payment", "issued", "encashment", "debt", "debit pullout",
    "dedit", "debt", "charg", "servce", "stapay", "instapay sending",
)
CREDIT_KEYWORDS = (
    "credit", "credits", "deposit", "receiving", "reversal",
    "memo", "fund transfer credit",
    "credt", "deposh", "receving", "recetving", "credit mermo",
)

def parse_row(text: str):
    rows = parse_row_entries(text)
    if rows:
        return rows[0]
    return {
        "date": None,
        "debit": None,
        "credit": None,
        "balance": None,
    }


def parse_row_entries(text: str):
    source = _normalize_source(text)
    if not source:
        return []

    chunks = _split_into_chunks(source)
    parsed_rows = []
    for chunk in chunks:
        parsed_rows.extend(_parse_chunk_entries(chunk))
    return parsed_rows


def _parse_chunk_entries(source: str):
    # DATE
    date_match = DATE_RE.search(source)
    if not date_match:
        return []

    date_value = date_match.group()
    normalized_iso = normalize_date(date_value, ["mdy", "dmy", "ymd"])
    date_display = _to_display_date(normalized_iso) if normalized_iso else date_value

    amounts = _extract_amounts(source)
    if len(amounts) < 2:
        return []

    pairs = []
    if len(amounts) >= 4:
        start = 0 if (len(amounts) % 2 == 0) else 1
        for i in range(start, len(amounts) - 1, 2):
            pairs.append((amounts[i], amounts[i + 1]))
    else:
        pairs.append((amounts[-2], amounts[-1]))

    out = []
    for flow, balance in pairs:
        result = {
            "date": date_display,
            "debit": None,
            "credit": None,
            "balance": balance,
        }
        lower = source.lower()
        if flow.startswith("-") or DEBIT_HINT_RE.search(source):
            result["debit"] = flow
        elif CREDIT_HINT_RE.search(source):
            result["credit"] = flow
        elif any(k in lower for k in CREDIT_KEYWORDS):
            result["credit"] = flow
        else:
            result["debit"] = flow
        out.append(result)
    return out


def _parse_single_chunk(source: str):
    result = {
        "date": None,
        "debit": None,
        "credit": None,
        "balance": None,
    }
    date_match = DATE_RE.search(source)
    if date_match:
        normalized_iso = normalize_date(date_match.group(), ["mdy", "dmy", "ymd"])
        result["date"] = _to_display_date(normalized_iso) if normalized_iso else date_match.group()

    # AMOUNTS (right-aligned heuristic; last parsed amount is treated as balance).
    amounts = _extract_amounts(source)

    if len(amounts) >= 2:
        result["balance"] = amounts[-1]
        flow = amounts[-2]
        if flow.startswith("-") or DEBIT_HINT_RE.search(source):
            result["debit"] = flow
        elif CREDIT_HINT_RE.search(source):
            result["credit"] = flow
        else:
            lower = source.lower()
            if any(k in lower for k in CREDIT_KEYWORDS):
                result["credit"] = flow
            elif any(k in lower for k in DEBIT_KEYWORDS):
                result["debit"] = flow
            else:
                # Default to debit when no directional hint is present.
                result["debit"] = flow
    elif len(amounts) == 1:
        result["balance"] = amounts[0]

    if not result["date"] or not result["balance"]:
        return None
    return result


def _normalize_source(text: str) -> str:
    source = (text or "").strip()
    if not source:
        return ""
    source = re.sub(r"\s+", " ", source)
    # OCR often misses slash separators in dates like 07172025.
    source = re.sub(r"\b(\d{2})(\d{2})(\d{4})\b", r"\1/\2/\3", source)
    return source


def _split_into_chunks(source: str):
    matches = list(DATE_CHUNK_RE.finditer(source))
    if not matches:
        return [source]

    chunks = []
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(source)
        chunk = source[start:end].strip()
        if chunk:
            chunks.append(chunk)
    return chunks


def _extract_amounts(source: str):
    amounts = []
    for match in AMOUNT_RE.findall(source):
        normalized = _normalize_ocr_amount_token(match)
        if normalized is None:
            continue
        amounts.append(normalized)
    return amounts


def _normalize_ocr_amount_token(token: str) -> Optional[str]:
    raw = (token or "").strip()
    if not raw:
        return None

    raw = raw.replace("S", "5").replace("s", "5").replace("O", "0").replace("o", "0")

    is_negative = "(" in raw and ")" in raw
    if "-" in raw:
        is_negative = True

    # Keep digits and common separators.
    stripped = re.sub(r"[^0-9,.\-]", "", raw)
    if not stripped:
        return None

    # Remove sign markers from parsing stream.
    core = stripped.replace("-", "")
    digits_only = re.sub(r"[^0-9]", "", core)
    if not digits_only:
        return None

    # Use last separator as decimal point if exactly 2 trailing digits.
    sep_positions = [i for i, ch in enumerate(core) if ch in {".", ","}]
    has_sep = bool(sep_positions)
    value = None
    if sep_positions:
        last_sep = sep_positions[-1]
        tail = re.sub(r"[^0-9]", "", core[last_sep + 1 :])
        if len(tail) == 2:
            head = re.sub(r"[^0-9]", "", core[:last_sep]) or "0"
            value = float(f"{int(head)}.{tail}")
        elif len(tail) == 1:
            head = re.sub(r"[^0-9]", "", core[:last_sep]) or "0"
            value = float(f"{int(head)}.{tail}0")

    if value is None:
        # Unseparated long integers are usually reference numbers, not amounts.
        if not has_sep and len(digits_only) > 3:
            return None
        if len(digits_only) <= 2:
            return None
        # Fallback: plain integer amount.
        value = float(int(digits_only))

    if is_negative:
        value *= -1.0
    return f"{value:.2f}"


def _to_display_date(iso_date: Optional[str]) -> Optional[str]:
    if not iso_date:
        return None
    parts = iso_date.split("-")
    if len(parts) != 3:
        return None
    year, month, day = parts
    if len(year) != 4:
        return None
    return f"{month}/{day}/{year}"


DATE_FLEX_RE = re.compile(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b")
AMOUNT_TOKEN_RE = re.compile(r"^-?\$?\d+(?:,\d{3})*(?:\.\d{2})$")


def parse_page_table(ocr_items):
    """
    Convert page-level OCR tokens to a transaction table.
    Output row schema: row_id, date, description, debit, credit, balance.
    """
    tokens = _normalize_tokens(ocr_items)
    if not tokens:
        return []

    lines = _group_lines(tokens)
    parsed_rows = []

    for line in lines:
        parsed = _parse_line(line)
        if parsed is None:
            continue

        parsed["row_id"] = f"{len(parsed_rows) + 1:03}"
        parsed_rows.append(parsed)

    return parsed_rows


def parse_page_table_with_bounds(ocr_items, image_width, image_height):
    tokens = _normalize_tokens(ocr_items)
    if not tokens:
        return [], []

    rows = []
    bounds = []
    safe_w = max(float(image_width or 1.0), 1.0)
    safe_h = max(float(image_height or 1.0), 1.0)

    for line in _group_lines(tokens):
        parsed = _parse_line(line)
        if parsed is None:
            continue

        row_id = f"{len(rows) + 1:03}"
        parsed["row_id"] = row_id
        rows.append(parsed)

        x1 = min(t["x1"] for t in line)
        y1 = min(t["y1"] for t in line)
        x2 = max(t["x2"] for t in line)
        y2 = max(t["y2"] for t in line)

        bounds.append({
            "row_id": row_id,
            "x1": _clamp01(x1 / safe_w),
            "y1": _clamp01(y1 / safe_h),
            "x2": _clamp01(x2 / safe_w),
            "y2": _clamp01(y2 / safe_h),
        })

    return rows, bounds


def _normalize_tokens(ocr_items):
    normalized = []
    for item in ocr_items:
        bbox = item.get("bbox") or []
        text = (item.get("text") or "").strip()
        if len(bbox) != 4 or not text:
            continue

        xs = [point[0] for point in bbox]
        ys = [point[1] for point in bbox]

        x1, x2 = min(xs), max(xs)
        y1, y2 = min(ys), max(ys)

        normalized.append({
            "text": text,
            "x1": float(x1),
            "x2": float(x2),
            "y1": float(y1),
            "y2": float(y2),
            "cx": float((x1 + x2) / 2.0),
            "cy": float((y1 + y2) / 2.0),
            "h": float(max(1.0, y2 - y1)),
        })

    return normalized


def _group_lines(tokens):
    sorted_tokens = sorted(tokens, key=lambda t: (t["cy"], t["x1"]))
    median_h = sorted(t["h"] for t in sorted_tokens)[len(sorted_tokens) // 2]
    y_tolerance = max(8.0, median_h * 0.7)

    lines = []
    current = []
    current_y = None

    for token in sorted_tokens:
        if not current:
            current = [token]
            current_y = token["cy"]
            continue

        if abs(token["cy"] - current_y) <= y_tolerance:
            current.append(token)
            current_y = (current_y + token["cy"]) / 2.0
        else:
            lines.append(sorted(current, key=lambda t: t["x1"]))
            current = [token]
            current_y = token["cy"]

    if current:
        lines.append(sorted(current, key=lambda t: t["x1"]))

    return lines


def _parse_line(line_tokens):
    line_text = " ".join(t["text"] for t in line_tokens).strip()
    lowered = line_text.lower()
    if not line_text:
        return None

    # Skip common non-transaction lines.
    if (
        "date" in lowered and "description" in lowered
    ) or (
        "opening balance" in lowered and not DATE_FLEX_RE.search(line_text)
    ):
        return None

    date_match = DATE_FLEX_RE.search(line_text)
    if not date_match:
        return None

    numeric_tokens = []
    for token in line_tokens:
        cleaned = _clean_amount_text(token["text"])
        if cleaned and AMOUNT_TOKEN_RE.match(cleaned):
            numeric_tokens.append({
                "raw": cleaned,
                "value": _to_number(cleaned),
                "x": token["cx"],
            })

    if not numeric_tokens:
        inline_amounts = [
            match.group(0)
            for match in AMOUNT_RE.finditer(line_text)
        ]
        numeric_tokens = [
            {
                "raw": amount,
                "value": _to_number(amount),
                "x": float(idx),
            }
            for idx, amount in enumerate(inline_amounts, start=1)
        ]

    if not numeric_tokens:
        return None

    numeric_tokens.sort(key=lambda n: n["x"])
    balance = numeric_tokens[-1]
    txn = numeric_tokens[-2] if len(numeric_tokens) >= 2 else None

    date_text = date_match.group(0)
    description = line_text
    description = description.replace(date_text, " ")
    for number in numeric_tokens:
        description = description.replace(number["raw"], " ")
    description = " ".join(description.split()).strip() or None

    row = {
        "date": date_text,
        "debit": None,
        "credit": None,
        "balance": _format_amount(balance["value"]),
    }

    if txn is not None:
        if txn["value"] < 0:
            row["debit"] = _format_amount(txn["value"])
        else:
            row["credit"] = _format_amount(txn["value"])

    return row


def _clean_amount_text(text):
    cleaned = text.strip().replace(" ", "")
    cleaned = cleaned.replace("O", "0").replace("o", "0")
    cleaned = cleaned.replace("S", "5")
    cleaned = cleaned.replace("$", "")
    # keep only likely amount characters
    cleaned = re.sub(r"[^0-9,.\-]", "", cleaned)
    if cleaned.count(".") > 1:
        return None
    return cleaned


def _to_number(amount_text):
    return float(amount_text.replace(",", ""))


def _format_amount(value):
    if value is None:
        return None
    return f"{value:.2f}"


def _clamp01(value):
    return max(0.0, min(1.0, float(value)))
