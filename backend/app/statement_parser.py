import datetime as dt
import re
from typing import Dict, List, Optional, Tuple

from app.bank_profiles import BankProfile, PROFILES


DATE_PATTERNS = {
    "mdy": [
        re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b"),
        re.compile(r"\b(\d{1,2})-(\d{1,2})-(\d{2,4})\b"),
    ],
    "dmy": [
        re.compile(r"\b(\d{1,2})\s+([A-Za-z]{3})\s+(\d{2,4})\b"),
        re.compile(r"\b(\d{1,2})([A-Za-z]{3})(\d{2,4})\b"),
        re.compile(r"\b(\d{1,2})-(\d{1,2})-(\d{2,4})\b"),
    ],
    "ymd": [
        re.compile(r"\b(\d{4})/(\d{1,2})/(\d{1,2})\b"),
        re.compile(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b"),
    ],
}

MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
MONTH_ABBRS = [k.upper() for k in MONTHS.keys()]
OCR_DAY_DIGIT_MAP = {
    "O": "0", "Q": "0", "D": "0",
    "I": "1", "L": "1",
    "Z": "2",
    "S": "5",
    "B": "3",
    "T": "7", "Y": "7",
}
OCR_YEAR_DIGIT_MAP = {
    "O": "0", "Q": "0", "D": "0",
    "I": "1", "L": "1",
    "Z": "2",
    "S": "5",
    "B": "8",
    "T": "7", "Y": "7",
}
OCR_MONTH_CHAR_MAP = {
    "0": "O",
    "1": "I",
    "2": "Z",
    "5": "S",
    "8": "B",
    "6": "G",
    "4": "A",
    "7": "T",
}

AMOUNT_RE = re.compile(r"(?<![A-Za-z0-9])\(?-?\s*\$?\s*[\d,]+(?:\.\d{2})?\)?(?![A-Za-z0-9])")


def normalize_amount(value: str) -> Optional[str]:
    text = (value or "").strip()
    if not text:
        return None
    text = text.replace("₱", "").replace("PHP", "").replace("php", "")
    text = text.replace("—", "").replace("–", "").replace("-", "-")
    text = text.strip()
    if text in {"", "-", "--"}:
        return None

    neg = False
    if text.startswith("(") and text.endswith(")"):
        neg = True
        text = text[1:-1].strip()

    text = text.replace(" ", "")
    text = text.replace(",", "")
    text = re.sub(r"[^0-9.\-]", "", text)
    if text in {"", ".", "-"}:
        return None

    try:
        num = float(text)
    except ValueError:
        return None

    if neg and num > 0:
        num *= -1

    return f"{num:.2f}"


def normalize_date(value: str, order: List[str]) -> Optional[str]:
    text = (value or "").strip()
    if not text:
        return None

    # Trim timestamp tails like ', 11:10 A' or ' 11:10 AM'.
    text = re.sub(r",?\s+\d{1,2}:\d{2}(?::\d{2})?\s*[APMapm]{0,2}$", "", text).strip()
    text = re.sub(r"(?<=\d)[Oo](?=\d)", "0", text)

    for mode in order:
        for pattern in DATE_PATTERNS.get(mode, []):
            m = pattern.search(text)
            if not m:
                continue
            parsed = _match_to_date(m.groups(), mode)
            if parsed is not None:
                return parsed.isoformat()

    # OCR fallback: allow month/day without year.
    m = re.search(r"\b(\d{1,2})/(\d{1,2})\b", text)
    if m:
        try:
            guess = dt.date(dt.date.today().year, int(m.group(1)), int(m.group(2)))
            return guess.isoformat()
        except Exception:
            pass

    ocr_compact = _parse_ocr_compact_month_date(text)
    if ocr_compact is not None:
        return ocr_compact

    return None


def _match_to_date(groups: Tuple[str, ...], mode: str) -> Optional[dt.date]:
    try:
        if mode == "ymd":
            year = int(groups[0])
            month = int(groups[1])
            day = int(groups[2])
            return dt.date(year, month, day)

        if mode == "mdy":
            month = int(groups[0])
            day = int(groups[1])
            year = _normalize_year(groups[2])
            return dt.date(year, month, day)

        if mode == "dmy":
            day = int(groups[0])
            if groups[1].isalpha():
                month = MONTHS.get(groups[1].strip().lower()[:3])
                if not month:
                    return None
                year = _normalize_year(groups[2])
            else:
                month = int(groups[1])
                year = _normalize_year(groups[2])
            return dt.date(year, month, day)
    except Exception:
        return None

    return None


def _normalize_year(raw: str) -> int:
    year = int(raw)
    if year < 100:
        return 2000 + year
    return year


def _parse_ocr_compact_month_date(text: str) -> Optional[str]:
    tokens = re.findall(r"[A-Za-z0-9]{6,10}", text or "")
    for token in tokens:
        upper = token.upper()
        for i in range(0, len(upper) - 2):
            win = upper[i:i + 3]
            month_key = "".join(OCR_MONTH_CHAR_MAP.get(ch, ch) for ch in win)
            if month_key.lower()[:3] not in MONTHS:
                continue

            day_raw = upper[:i]
            year_raw = upper[i + 3:]
            if not day_raw or not year_raw:
                continue

            day_digits = []
            for ch in day_raw:
                mapped = OCR_DAY_DIGIT_MAP.get(ch, ch)
                if mapped.isdigit():
                    day_digits.append(mapped)
            year_digits = []
            for ch in year_raw:
                mapped = OCR_YEAR_DIGIT_MAP.get(ch, ch)
                if mapped.isdigit():
                    year_digits.append(mapped)

            if len(day_digits) == 0 or len(year_digits) < 2:
                continue

            day_txt = "".join(day_digits[-2:])
            year_txt = "".join(year_digits[:4])

            day = int(day_txt)
            if len(year_txt) >= 4:
                year = int(year_txt[:4])
            else:
                year = int(year_txt[:2])
                year += 2000
            month = MONTHS[month_key.lower()[:3]]
            try:
                if 1 <= day <= 31:
                    return dt.date(year, month, day).isoformat()
                # OCR fallback when day is corrupted but month/year is visible.
                return dt.date(year, month, 1).isoformat()
            except Exception:
                continue
    return None


def parse_words_page(
    words: List[Dict],
    page_width: float,
    page_height: float,
    profile: BankProfile,
) -> Tuple[List[Dict], List[Dict], Dict]:
    rows = []
    bounds = []

    grouped = _group_words_by_line(words)
    header = _find_header_anchors(grouped, profile)
    diagnostics = {
        "header_detected": bool(header),
        "header_y": header["y"] if header else None,
        "row_candidates": 0,
    }
    if not header:
        rows, bounds = _parse_rows_without_header(grouped, page_width, page_height, profile)
        diagnostics["fallback_mode"] = "no_header_line_parse"
        diagnostics["row_candidates"] = len(grouped)
        return rows, bounds, diagnostics

    date_x = header["date"]
    description_x = header.get("description")
    debit_x = header["debit"]
    credit_x = header["credit"]
    balance_x = header["balance"]

    for line in grouped:
        y = line["cy"]
        if y <= header["y"] + 2:
            continue

        line_text = " ".join(w["text"] for w in line["words"])
        if _is_noise(line_text, profile):
            continue

        diagnostics["row_candidates"] += 1
        date_txt = _nearest_text(line["words"], date_x)
        debit_txt, credit_txt, balance_txt = _assign_amount_columns(
            line["words"], debit_x, credit_x, balance_x
        )

        # Parse dates from the full line first so multi-token dates
        # like "02 MAY 24" are handled consistently.
        date_iso = normalize_date(line_text, profile.date_order)
        if date_iso is None and date_txt:
            date_iso = normalize_date(date_txt, profile.date_order)
        debit = debit_txt
        credit = credit_txt
        balance = balance_txt
        description = _extract_description_from_header_line(
            line["words"],
            line_text,
            profile,
            date_x,
            description_x,
            debit_x,
            credit_x,
            balance_x,
        )

        # Fallback amount inference from full line.
        if balance is None:
            line_amounts = _extract_line_amounts(line_text)
            if line_amounts:
                balance = line_amounts[-1]
                if len(line_amounts) >= 2 and debit is None and credit is None:
                    second = line_amounts[-2]
                    if second.startswith("-"):
                        debit = second
                    else:
                        credit = second

        if not (date_iso and balance):
            continue

        row_id = f"{len(rows) + 1:03}"
        rows.append({
            "row_id": row_id,
            "date": date_iso,
            "description": description,
            "debit": debit,
            "credit": credit,
            "balance": balance,
        })

        x1 = min(w["x1"] for w in line["words"])
        y1 = min(w["y1"] for w in line["words"])
        x2 = max(w["x2"] for w in line["words"])
        y2 = max(w["y2"] for w in line["words"])
        bounds.append({
            "row_id": row_id,
            "x1": _clamp01(x1 / max(page_width, 1.0)),
            "y1": _clamp01(y1 / max(page_height, 1.0)),
            "x2": _clamp01(x2 / max(page_width, 1.0)),
            "y2": _clamp01(y2 / max(page_height, 1.0)),
        })

    if not rows:
        f_rows, f_bounds = _parse_rows_without_header(grouped, page_width, page_height, profile)
        if f_rows:
            diagnostics["fallback_mode"] = "line_parse_after_empty_header"
            return f_rows, f_bounds, diagnostics

    return rows, bounds, diagnostics


def parse_page_with_profile_fallback(
    words: List[Dict],
    page_width: float,
    page_height: float,
    detected_profile: BankProfile,
) -> Tuple[List[Dict], List[Dict], Dict]:
    base_rows, base_bounds, base_diag = parse_words_page(
        words,
        page_width,
        page_height,
        detected_profile,
    )
    base_ratio = _rows_conversion_ratio(base_rows, base_diag)

    selected_rows = base_rows
    selected_bounds = base_bounds
    selected_diag = dict(base_diag)
    selected_profile = detected_profile.name
    fallback_applied = False
    fallback_reason = None

    if _should_retry_generic(base_rows, base_diag):
        generic_profile = PROFILES["GENERIC"]
        fb_rows, fb_bounds, fb_diag = parse_words_page(
            words,
            page_width,
            page_height,
            generic_profile,
        )
        fb_ratio = _rows_conversion_ratio(fb_rows, fb_diag)

        choose_fallback = False
        if len(fb_rows) > len(base_rows):
            choose_fallback = True
        elif len(fb_rows) == len(base_rows) and fb_ratio > base_ratio:
            choose_fallback = True

        if choose_fallback:
            selected_rows = fb_rows
            selected_bounds = fb_bounds
            selected_diag = dict(fb_diag)
            selected_profile = generic_profile.name
            fallback_applied = True
            fallback_reason = "low_yield_detected_profile"

    selected_diag["profile_detected"] = detected_profile.name
    selected_diag["profile_selected"] = selected_profile
    selected_diag["fallback_applied"] = fallback_applied
    if fallback_reason:
        selected_diag["fallback_reason"] = fallback_reason

    return selected_rows, selected_bounds, selected_diag


def evaluate_quality(rows: List[Dict]) -> Dict:
    total = len(rows)
    if total == 0:
        return {
            "rows": 0,
            "date_ratio": 0.0,
            "balance_ratio": 0.0,
            "flow_ratio": 0.0,
            "passes": False,
            "reasons": ["no_rows"],
        }

    date_ok = sum(1 for r in rows if r.get("date"))
    balance_ok = sum(1 for r in rows if r.get("balance"))
    flow_ok = sum(1 for r in rows if r.get("debit") or r.get("credit"))

    date_ratio = date_ok / total
    balance_ratio = balance_ok / total
    flow_ratio = flow_ok / total

    reasons = []
    if total < 3:
        reasons.append("few_rows")
    if date_ratio < 0.8:
        reasons.append("low_date_ratio")
    if balance_ratio < 0.8:
        reasons.append("low_balance_ratio")

    return {
        "rows": total,
        "date_ratio": round(date_ratio, 3),
        "balance_ratio": round(balance_ratio, 3),
        "flow_ratio": round(flow_ratio, 3),
        "passes": len(reasons) == 0,
        "reasons": reasons,
    }


def _rows_conversion_ratio(rows: List[Dict], diagnostics: Dict) -> float:
    row_candidates = int(diagnostics.get("row_candidates") or 0)
    if row_candidates <= 0:
        return float(len(rows))
    return len(rows) / float(max(row_candidates, 1))


def _should_retry_generic(rows: List[Dict], diagnostics: Dict) -> bool:
    row_candidates = int(diagnostics.get("row_candidates") or 0)
    rows_count = len(rows)
    if row_candidates < 20:
        return False
    if rows_count <= 5:
        return True
    ratio = rows_count / float(max(row_candidates, 1))
    return ratio < 0.35


def _group_words_by_line(words: List[Dict]) -> List[Dict]:
    if not words:
        return []
    sorted_words = sorted(words, key=lambda w: (((w["y1"] + w["y2"]) / 2.0), w["x1"]))
    heights = [max(1.0, w["y2"] - w["y1"]) for w in sorted_words]
    median_h = sorted(heights)[len(heights) // 2]
    y_tol = max(2.0, median_h * 0.7)

    lines = []
    current = []
    current_y = None
    for w in sorted_words:
        cy = (w["y1"] + w["y2"]) / 2.0
        if not current:
            current = [w]
            current_y = cy
            continue
        if abs(cy - current_y) <= y_tol:
            current.append(w)
            current_y = (current_y + cy) / 2.0
        else:
            current = sorted(current, key=lambda x: x["x1"])
            lines.append({"words": current, "cy": current_y})
            current = [w]
            current_y = cy
    if current:
        current = sorted(current, key=lambda x: x["x1"])
        lines.append({"words": current, "cy": current_y})

    return lines


def _find_header_anchors(grouped_lines: List[Dict], profile: BankProfile) -> Optional[Dict]:
    for line in grouped_lines[:80]:
        words = line["words"]
        text = " ".join(w["text"] for w in words).lower()

        date_x = _find_token_x(text, words, profile.date_tokens)
        description_x = _find_token_x(text, words, profile.description_tokens)
        debit_x = _find_token_x(text, words, profile.debit_tokens)
        credit_x = _find_token_x(text, words, profile.credit_tokens)
        balance_x = _find_token_x(text, words, profile.balance_tokens)

        if date_x is None or balance_x is None:
            continue
        if debit_x is None and credit_x is None:
            continue

        # Fill missing anchors with nearest reasonable defaults.
        if debit_x is None:
            debit_x = (date_x + balance_x) / 2.0
        if credit_x is None:
            credit_x = (debit_x + balance_x) / 2.0

        return {
            "y": line["cy"],
            "date": date_x,
            "description": description_x,
            "debit": debit_x,
            "credit": credit_x,
            "balance": balance_x,
        }

    return None


def _find_token_x(line_text: str, words: List[Dict], tokens: List[str]) -> Optional[float]:
    for token in tokens:
        if token not in line_text:
            continue
        token_parts = token.split()
        for i in range(len(words)):
            candidate = " ".join(w["text"].lower() for w in words[i:i + len(token_parts)])
            if candidate == token:
                left = words[i]["x1"]
                right = words[i + len(token_parts) - 1]["x2"]
                return (left + right) / 2.0
    return None


def _nearest_text(words: List[Dict], target_x: float) -> Optional[str]:
    if not words:
        return None
    # Use words left of the debit anchor as date candidate when date not isolated.
    candidates = sorted(words, key=lambda w: abs(((w["x1"] + w["x2"]) / 2.0) - target_x))
    for w in candidates[:3]:
        t = w["text"].strip()
        if t:
            return t
    return None


def _assign_amount_columns(
    words: List[Dict],
    debit_x: float,
    credit_x: float,
    balance_x: float,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    amount_words = []
    for w in words:
        norm = normalize_amount(w["text"])
        if norm is None:
            continue
        cx = (w["x1"] + w["x2"]) / 2.0
        amount_words.append({"cx": cx, "value": norm})

    if not amount_words:
        return None, None, None

    balance_idx = min(range(len(amount_words)), key=lambda i: abs(amount_words[i]["cx"] - balance_x))
    balance = amount_words[balance_idx]["value"]
    remaining = [a for i, a in enumerate(amount_words) if i != balance_idx]

    debit = None
    credit = None
    if not remaining:
        return debit, credit, balance

    d_idx = min(range(len(remaining)), key=lambda i: abs(remaining[i]["cx"] - debit_x))
    c_idx = min(range(len(remaining)), key=lambda i: abs(remaining[i]["cx"] - credit_x))

    if d_idx == c_idx:
        cand = remaining[d_idx]
        if abs(cand["cx"] - debit_x) <= abs(cand["cx"] - credit_x):
            debit = cand["value"]
        else:
            credit = cand["value"]
        return debit, credit, balance

    debit = remaining[d_idx]["value"]
    credit = remaining[c_idx]["value"]
    return debit, credit, balance


def _extract_description_from_header_line(
    words: List[Dict],
    line_text: str,
    profile: BankProfile,
    date_x: float,
    description_x: Optional[float],
    debit_x: float,
    credit_x: float,
    balance_x: float,
) -> Optional[str]:
    if not words:
        return _extract_description_without_header(line_text, profile)

    flow_left = min(debit_x, credit_x, balance_x)
    left_anchor = min(date_x, description_x if description_x is not None else date_x)
    if flow_left <= left_anchor:
        return _extract_description_without_header(line_text, profile)

    picked: List[str] = []
    for w in words:
        cx = (w["x1"] + w["x2"]) / 2.0
        if cx <= left_anchor + 2.0 or cx >= flow_left - 2.0:
            continue
        token = (w.get("text") or "").strip()
        if not token:
            continue
        if normalize_amount(token) is not None:
            continue
        if normalize_date(token, profile.date_order) is not None:
            continue
        picked.append(token)

    if picked:
        desc = re.sub(r"\s+", " ", " ".join(picked)).strip(" -:|,")
        if desc and not _is_noise(desc, profile):
            return desc

    from_words = _extract_description_from_words(words, profile)
    if from_words:
        return from_words

    return _extract_description_without_header(line_text, profile)


def _extract_description_without_header(line_text: str, profile: BankProfile) -> Optional[str]:
    text = (line_text or "").strip()
    if not text:
        return None

    search_order = profile.date_order + [m for m in ("mdy", "dmy", "ymd") if m not in profile.date_order]
    for mode in search_order:
        for pattern in DATE_PATTERNS.get(mode, []):
            m = pattern.search(text)
            if m:
                text = f"{text[:m.start()]} {text[m.end():]}"
                break
        else:
            continue
        break

    text = re.sub(r",?\s+\d{1,2}:\d{2}(?::\d{2})?\s*[APMapm]{0,2}$", "", text)
    text = AMOUNT_RE.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip(" -:|,")
    if not text:
        return None
    if _is_noise(text, profile):
        return None
    return text


def _extract_description_from_words(words: List[Dict], profile: BankProfile) -> Optional[str]:
    if not words:
        return None

    ignored_tokens = {
        *(t.lower() for t in profile.date_tokens),
        *(t.lower() for t in profile.description_tokens),
        *(t.lower() for t in profile.debit_tokens),
        *(t.lower() for t in profile.credit_tokens),
        *(t.lower() for t in profile.balance_tokens),
    }

    parts: List[str] = []
    for w in sorted(words, key=lambda item: item.get("x1", 0.0)):
        token = (w.get("text") or "").strip()
        if not token:
            continue
        lower = token.lower()
        if lower in ignored_tokens:
            continue
        if normalize_amount(token) is not None:
            continue
        if normalize_date(token, profile.date_order) is not None:
            continue
        parts.append(token)

    text = re.sub(r"\s+", " ", " ".join(parts)).strip(" -:|,")
    if not text:
        return None
    if _is_noise(text, profile):
        return None
    return text


def _extract_line_amounts(line_text: str) -> List[str]:
    out = []
    for m in AMOUNT_RE.findall(line_text):
        token = (m or "").strip()
        if not token:
            continue
        # Skip short integer fragments (often date/code OCR noise).
        plain = token.replace(",", "").replace("(", "").replace(")", "").replace("-", "").replace("$", "").strip()
        if "." not in token and plain.isdigit() and len(plain) <= 2:
            continue
        norm = normalize_amount(m)
        if norm is not None:
            out.append(norm)
    return out


def _parse_rows_without_header(
    grouped_lines: List[Dict],
    page_width: float,
    page_height: float,
    profile: BankProfile,
) -> Tuple[List[Dict], List[Dict]]:
    rows = []
    bounds = []
    i = 0
    while i < len(grouped_lines):
        line = grouped_lines[i]
        line_text = " ".join(w["text"] for w in line["words"])
        if _is_noise(line_text, profile):
            i += 1
            continue

        date_iso = normalize_date(line_text, profile.date_order)
        if date_iso is None:
            i += 1
            continue

        line_words = list(line["words"])
        amounts = _extract_line_amounts(line_text)
        j = i + 1
        # OCR often emits one transaction across multiple short lines.
        while len(amounts) < 2 and j < len(grouped_lines) and j <= i + 3:
            next_line = grouped_lines[j]
            next_text = " ".join(w["text"] for w in next_line["words"])
            if normalize_date(next_text, profile.date_order):
                break
            next_amounts = _extract_line_amounts(next_text)
            if next_amounts:
                amounts.extend(next_amounts)
                line_words.extend(next_line["words"])
            j += 1

        if not amounts:
            i += 1
            continue

        balance = amounts[-1]
        debit = None
        credit = None
        if len(amounts) >= 2:
            flow = amounts[-2]
            lower = line_text.lower()
            if flow.startswith("-") or any(t in lower for t in ["withdraw", "debit", "db"]):
                debit = flow
            elif any(t in lower for t in ["deposit", "credit", "cr"]):
                credit = flow
            else:
                debit = flow

        row_id = f"{len(rows) + 1:03}"
        rows.append({
            "row_id": row_id,
            "date": date_iso,
            "description": _extract_description_from_words(line_words, profile) or _extract_description_without_header(" ".join(w["text"] for w in line_words), profile),
            "debit": debit,
            "credit": credit,
            "balance": balance,
        })

        x1 = min(w["x1"] for w in line_words)
        y1 = min(w["y1"] for w in line_words)
        x2 = max(w["x2"] for w in line_words)
        y2 = max(w["y2"] for w in line_words)
        bounds.append({
            "row_id": row_id,
            "x1": _clamp01(x1 / max(page_width, 1.0)),
            "y1": _clamp01(y1 / max(page_height, 1.0)),
            "x2": _clamp01(x2 / max(page_width, 1.0)),
            "y2": _clamp01(y2 / max(page_height, 1.0)),
        })

        i = max(i + 1, j)

    return rows, bounds


def _is_noise(line_text: str, profile: BankProfile) -> bool:
    lower = (line_text or "").lower()
    if not lower.strip():
        return True
    for token in profile.noise_tokens:
        if token in lower:
            return True
    return False


def _clamp01(v: float) -> float:
    return max(0.0, min(1.0, float(v)))
