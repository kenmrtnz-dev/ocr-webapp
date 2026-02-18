import fcntl
import json
import os
import re
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, List, Optional, Tuple

from app.bank_profiles import (
    BankProfile,
    DetectionRule,
    DETECTION_RULES,
    PROFILES,
    get_profiles_config_path,
    reload_profiles,
)
from app.statement_parser import is_transaction_row, parse_words_page


def analyze_account_identity_from_text(page_text: str) -> Dict:
    provider = str(os.getenv("AI_ANALYZER_PROVIDER", "openai")).strip().lower() or "openai"
    model = str(os.getenv("AI_ANALYZER_MODEL", "")).strip() or _default_model(provider)

    response = {
        "provider": provider,
        "model": model,
        "result": "failed",
        "reason": "invalid_input",
        "account_name": None,
        "account_number": None,
    }

    text = str(page_text or "").strip()
    if not text:
        return response

    heuristic_name, heuristic_number = _extract_identity_heuristic(text)

    if provider not in {"gemini", "openai"}:
        response["reason"] = "unsupported_provider"
        response["result"] = "fallback_heuristic"
        response["account_name"] = heuristic_name
        response["account_number"] = heuristic_number
        return response

    prompt = (
        "Extract account identity from this bank statement page text.\n"
        "Return one JSON object only, no markdown, no explanations.\n"
        "Required keys: account_name, account_number.\n"
        "Rules:\n"
        "- Use null when not confidently found.\n"
        "- account_name should be the account holder name only.\n"
        "- account_number should keep masking/dashes if present in source.\n"
        f"Page text: {text[:9000]}\n"
        "Output JSON now."
    )

    if provider == "openai":
        parsed, reason = _call_openai_json_prompt(prompt, model)
    else:
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0,
                "responseMimeType": "application/json",
            },
        }
        parsed, reason = _call_gemini_json(payload, model)
    if not parsed:
        response["reason"] = reason or "invalid_llm_output"
        response["result"] = "fallback_heuristic"
        response["account_name"] = heuristic_name
        response["account_number"] = heuristic_number
        return response

    name_raw = parsed.get("account_name")
    number_raw = parsed.get("account_number")
    name = str(name_raw).strip() if isinstance(name_raw, str) else None
    number = str(number_raw).strip() if isinstance(number_raw, str) else None
    if name and name.lower() in {"none", "null", "n/a", "na"}:
        name = None
    if number and number.lower() in {"none", "null", "n/a", "na"}:
        number = None

    response["account_name"] = name or heuristic_name
    response["account_number"] = number or heuristic_number
    if response["account_name"] or response["account_number"]:
        response["result"] = "applied"
        response["reason"] = "identity_extracted"
    else:
        response["result"] = "failed"
        response["reason"] = "identity_not_found"
    return response


def _extract_identity_heuristic(text: str) -> Tuple[Optional[str], Optional[str]]:
    # Preserve line boundaries for labeled values and also keep a compact copy for split labels.
    clean_lines = []
    for line in str(text or "").splitlines():
        compact = re.sub(r"\s+", " ", line).strip()
        if compact:
            clean_lines.append(compact)
    clean = "\n".join(clean_lines)
    compact_all = re.sub(r"\s+", " ", clean).strip()

    name_patterns = [
        r"(?:account\s*name|account\s*holder|depositor\s*name|customer\s*name)\s*[:\-]?\s*([A-Za-z0-9][A-Za-z0-9 .,'/&()-]{2,120})",
        r"(?:name)\s*[:\-]\s*([A-Za-z0-9][A-Za-z0-9 .,'/&()-]{2,120})",
    ]
    number_patterns = [
        r"(?:account\s*(?:no\.?|number|#)|acct\.?\s*(?:no\.?|#)|casa\s*(?:no\.?|#)|a\/c\s*(?:no\.?|#))\s*[:\-]?\s*([0-9Xx\*\- ]{6,50})",
    ]

    name = _extract_first_match(name_patterns, clean) or _extract_first_match(name_patterns, compact_all)
    if not name:
        name = _extract_name_from_header(clean_lines, compact_all)
    number = _extract_first_match(number_patterns, clean) or _extract_first_match(number_patterns, compact_all)

    name = _normalize_name_candidate(name)
    number = _normalize_number_candidate(number) or _find_account_number_in_lines(clean_lines)
    return name, number


def _extract_first_match(patterns: List[str], text: str) -> Optional[str]:
    for pattern in patterns:
        try:
            match = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
        except re.error:
            continue
        if not match:
            continue
        value = str(match.group(1) or "").strip()
        if value:
            return value
    return None


def _normalize_name_candidate(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = re.sub(r"\s+", " ", value).strip(" -:|")
    stop_words = [
        "account number",
        "acct no",
        "available balance",
        "statement period",
        "date from",
        "date to",
    ]
    low = cleaned.lower()
    for token in stop_words:
        idx = low.find(token)
        if idx > 0:
            cleaned = cleaned[:idx].strip(" -:|")
            low = cleaned.lower()
    if len(cleaned) < 3 or len(cleaned) > 120:
        return None
    if cleaned.lower() in {"none", "null", "n/a", "na"}:
        return None
    return cleaned


def _normalize_number_candidate(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = re.sub(r"\s+", "", value).replace("—", "-").strip(" -:|")
    cleaned = cleaned.replace("*", "X")
    if len(cleaned) < 4:
        return None
    # Allow masked account numbers (e.g. XXXXX1234 / XXXXXXXX) even without plain digits.
    if not re.search(r"[0-9Xx]", cleaned):
        return None
    return cleaned


def _find_account_number_in_lines(lines: List[str]) -> Optional[str]:
    for idx, line in enumerate(lines):
        low = line.lower()
        if not any(token in low for token in ("account", "acct", "a/c", "casa")):
            continue
        candidates = re.findall(r"[0-9Xx\*\-]{6,50}", line)
        if not candidates and idx + 1 < len(lines):
            candidates = re.findall(r"[0-9Xx\*\-]{6,50}", lines[idx + 1])
        for cand in candidates:
            normalized = _normalize_number_candidate(cand)
            if normalized:
                return normalized
    return None


def _extract_name_from_header(lines: List[str], compact_all: str) -> Optional[str]:
    header = ""
    if lines:
        header = lines[0]
    if not header:
        header = compact_all[:260]
    if not header:
        return None

    normalized = re.sub(r"\s+", " ", header).strip()
    # Trim obvious table/header markers.
    for token in [
        " TRANSACTION DESCRIPTION",
        " DATE CHECK NO.",
        " DATE ",
        " ACCOUNT NO",
        " ACCOUNT NUMBER",
    ]:
        idx = normalized.upper().find(token)
        if idx > 0:
            normalized = normalized[:idx].strip()

    # Trim common address starts that usually come right after company/person name.
    for marker in [
        " LOT ",
        " BLOCK ",
        " STREET ",
        " ST. ",
        " AVE ",
        " AVENUE ",
        " BARANGAY ",
        " BRGY ",
        " CITY ",
        " QUEZON CITY ",
        " MAKATI ",
        " MANILA ",
    ]:
        idx = normalized.upper().find(marker)
        if idx > 0:
            normalized = normalized[:idx].strip()
            break

    # Prefer legal entity endings if present.
    m = re.search(
        r"\b([A-Z][A-Z0-9 .,&'/-]{2,120}\b(?:CORPORATION|CORP\.?|INC\.?|CO\.?|COMPANY|LIMITED|LTD\.?))\b",
        normalized,
        flags=re.IGNORECASE,
    )
    if m:
        normalized = m.group(1).strip()

    return _normalize_name_candidate(normalized)


def analyze_unknown_bank_and_apply(
    layout_pages: List[Dict],
    sample_pages: int,
    min_rows: int,
    min_date_ratio: float,
    min_balance_ratio: float,
) -> Dict:
    provider = str(os.getenv("AI_ANALYZER_PROVIDER", "openai")).strip().lower() or "openai"
    model = str(os.getenv("AI_ANALYZER_MODEL", "")).strip() or _default_model(provider)
    response = {
        "triggered": True,
        "result": "failed",
        "reason": "analyzer_error",
        "provider": provider,
        "model": model,
        "profile_name": None,
    }

    snippets = _build_snippets(layout_pages, sample_pages)
    if not snippets:
        response["result"] = "skipped"
        response["reason"] = "no_text_snippets"
        return response

    proposal, proposal_reason = _generate_profile_with_llm(snippets, provider, model)
    if not proposal:
        response["result"] = "failed"
        response["reason"] = proposal_reason or "invalid_llm_output"
        return response

    candidate, rule, validation_reason = _validate_proposal(
        proposal=proposal,
        layout_pages=layout_pages,
        sample_pages=sample_pages,
        min_rows=min_rows,
        min_date_ratio=min_date_ratio,
        min_balance_ratio=min_balance_ratio,
    )
    if not candidate or not rule:
        response["result"] = "rejected"
        response["reason"] = (
            f"validation_failed_{validation_reason}" if validation_reason else "validation_failed"
        )
        return response

    response["profile_name"] = candidate.name

    applied, apply_reason = _apply_profile_update_atomic(candidate, rule)
    if not applied:
        response["result"] = "failed"
        response["reason"] = f"apply_failed_{apply_reason}" if apply_reason else "apply_failed"
        return response

    reload_profiles()
    response["result"] = "applied"
    response["reason"] = "profile_created"
    return response


def analyze_unknown_bank_and_apply_guided(
    layout_pages: List[Dict],
    guided_payload: Dict,
    sample_pages: int,
    min_rows: int,
    min_date_ratio: float,
    min_balance_ratio: float,
) -> Dict:
    provider = str(os.getenv("AI_ANALYZER_PROVIDER", "openai")).strip().lower() or "openai"
    model = str(os.getenv("AI_ANALYZER_MODEL", "")).strip() or _default_model(provider)
    response = {
        "triggered": True,
        "result": "failed",
        "reason": "analyzer_error",
        "provider": provider,
        "model": model,
        "profile_name": None,
    }

    snippets = _build_snippets(layout_pages, sample_pages)
    guided_rows = _build_guided_rows(guided_payload)
    if not snippets and not guided_rows:
        response["result"] = "skipped"
        response["reason"] = "no_text_snippets"
        return response

    proposal, proposal_reason = _generate_profile_with_llm_guided(
        snippets=snippets,
        guided_rows=guided_rows,
        provider=provider,
        model=model,
    )
    if not proposal:
        response["result"] = "failed"
        response["reason"] = proposal_reason or "invalid_llm_output"
        return response
    if not _is_bank_like_profile(
        str(proposal.get("profile_name") or ""),
        _normalize_items(proposal.get("detection_contains_any", [])),
        _normalize_items(proposal.get("detection_contains_all", [])),
    ):
        coerced_name = str(proposal.get("profile_name") or "").strip()
        if not coerced_name:
            coerced_name = "AUTO_GUIDED_BANK_LAYOUT"
        elif "bank" not in coerced_name.lower():
            coerced_name = f"{coerced_name}_BANK"
        proposal["profile_name"] = coerced_name

    candidate, rule, validation_reason = _validate_proposal(
        proposal=proposal,
        layout_pages=layout_pages,
        sample_pages=sample_pages,
        min_rows=min_rows,
        min_date_ratio=min_date_ratio,
        min_balance_ratio=min_balance_ratio,
    )
    if not candidate or not rule:
        response["result"] = "rejected"
        response["reason"] = (
            f"validation_failed_{validation_reason}" if validation_reason else "validation_failed"
        )
        return response

    response["profile_name"] = candidate.name
    applied, apply_reason = _apply_profile_update_atomic(candidate, rule)
    if not applied:
        response["result"] = "failed"
        response["reason"] = f"apply_failed_{apply_reason}" if apply_reason else "apply_failed"
        return response

    reload_profiles()
    response["result"] = "applied"
    response["reason"] = "profile_created"
    return response


def _build_snippets(layout_pages: List[Dict], sample_pages: int) -> List[Dict]:
    snippets: List[Dict] = []
    for idx, layout in enumerate(layout_pages, start=1):
        text = str((layout or {}).get("text") or "").strip()
        if not text:
            continue
        snippets.append(
            {
                "page": idx,
                "text": text[:7000],
            }
        )
        if len(snippets) >= max(1, sample_pages):
            break
    return snippets


def _default_model(provider: str) -> str:
    if provider == "openai":
        return "gpt-4o-mini"
    if provider == "gemini":
        return "gemini-2.5-flash"
    return "gpt-4o-mini"


def _generate_profile_with_llm(
    snippets: List[Dict],
    provider: str,
    model: str,
) -> Tuple[Optional[Dict], Optional[str]]:
    if provider == "openai":
        return _generate_profile_with_openai(snippets, model)
    if provider == "gemini":
        return _generate_profile_with_gemini(snippets, model)
    return None, "unsupported_provider"


def _generate_profile_with_llm_guided(
    snippets: List[Dict],
    guided_rows: List[Dict],
    provider: str,
    model: str,
) -> Tuple[Optional[Dict], Optional[str]]:
    if provider == "openai":
        return _generate_profile_with_openai_guided(snippets, guided_rows, model)
    if provider == "gemini":
        return _generate_profile_with_gemini_guided(snippets, guided_rows, model)
    return None, "unsupported_provider"


def _generate_profile_with_openai(
    snippets: List[Dict],
    model: str,
) -> Tuple[Optional[Dict], Optional[str]]:
    prompt = (
        "Generate a strict bank statement parsing profile from snippet text.\n"
        "Return one JSON object only, no markdown, no explanations.\n"
        "Required keys:\n"
        "profile_name, detection_contains_any, detection_contains_all, date_tokens, description_tokens, "
        "debit_tokens, credit_tokens, balance_tokens, date_order, noise_tokens, "
        "account_name_patterns, account_number_patterns.\n"
        "Rules:\n"
        "- date_order values must be from [mdy, dmy, ymd].\n"
        "- all array values must be strings.\n"
        "- detection_contains_any and detection_contains_all cannot both be empty.\n"
        "- profile_name should be short and bank-specific.\n"
        "Example shape:\n"
        "{\"profile_name\":\"AUTO_EXAMPLE\",\"detection_contains_any\":[\"example bank\"],\"detection_contains_all\":[],"
        "\"date_tokens\":[\"date\"],\"description_tokens\":[\"description\"],\"debit_tokens\":[\"debit\"],"
        "\"credit_tokens\":[\"credit\"],\"balance_tokens\":[\"balance\"],\"date_order\":[\"mdy\"],"
        "\"noise_tokens\":[],\"account_name_patterns\":[],\"account_number_patterns\":[]}\n"
        f"Statement snippets: {json.dumps(snippets, ensure_ascii=True)}\n"
        "Output JSON now."
    )
    return _call_openai_json_prompt(prompt, model)


def _generate_profile_with_openai_guided(
    snippets: List[Dict],
    guided_rows: List[Dict],
    model: str,
) -> Tuple[Optional[Dict], Optional[str]]:
    prompt = (
        "Generate a strict bank statement parsing profile using guided table OCR samples.\n"
        "Return one JSON object only, no markdown.\n"
        "Required keys:\n"
        "profile_name, detection_contains_any, detection_contains_all, date_tokens, description_tokens, "
        "debit_tokens, credit_tokens, balance_tokens, date_order, noise_tokens, "
        "account_name_patterns, account_number_patterns.\n"
        "Rules:\n"
        "- date_order values must be from [mdy, dmy, ymd].\n"
        "- all arrays contain strings only.\n"
        "- detection_contains_any and detection_contains_all cannot both be empty.\n"
        "- infer headers/tokens from guided OCR rows and snippets.\n"
        "- do not invent bank/account names as profile names; profile_name must represent a bank layout.\n"
        "Example shape:\n"
        "{\"profile_name\":\"AUTO_EXAMPLE_BANK\",\"detection_contains_any\":[\"example bank\"],"
        "\"detection_contains_all\":[],\"date_tokens\":[\"date\"],\"description_tokens\":[\"description\"],"
        "\"debit_tokens\":[\"debit\"],\"credit_tokens\":[\"credit\"],\"balance_tokens\":[\"balance\"],"
        "\"date_order\":[\"mdy\"],\"noise_tokens\":[],\"account_name_patterns\":[],\"account_number_patterns\":[]}\n"
        f"Guided rows: {json.dumps(guided_rows, ensure_ascii=True)}\n"
        f"Statement snippets: {json.dumps(snippets, ensure_ascii=True)}\n"
        "Output JSON now."
    )
    return _call_openai_json_prompt(prompt, model)


def _generate_profile_with_gemini(
    snippets: List[Dict],
    model: str,
) -> Tuple[Optional[Dict], Optional[str]]:
    prompt = (
        "Generate a strict bank statement parsing profile from snippet text.\n"
        "Return one JSON object only, no markdown, no explanations.\n"
        "Required keys:\n"
        "profile_name, detection_contains_any, detection_contains_all, date_tokens, description_tokens, "
        "debit_tokens, credit_tokens, balance_tokens, date_order, noise_tokens, "
        "account_name_patterns, account_number_patterns.\n"
        "Rules:\n"
        "- date_order values must be from [mdy, dmy, ymd].\n"
        "- all array values must be strings.\n"
        "- detection_contains_any and detection_contains_all cannot both be empty.\n"
        "- profile_name should be short and bank-specific.\n"
        "Example shape:\n"
        "{\"profile_name\":\"AUTO_EXAMPLE\",\"detection_contains_any\":[\"example bank\"],\"detection_contains_all\":[],"
        "\"date_tokens\":[\"date\"],\"description_tokens\":[\"description\"],\"debit_tokens\":[\"debit\"],"
        "\"credit_tokens\":[\"credit\"],\"balance_tokens\":[\"balance\"],\"date_order\":[\"mdy\"],"
        "\"noise_tokens\":[],\"account_name_patterns\":[],\"account_number_patterns\":[]}\n"
        f"Statement snippets: {json.dumps(snippets, ensure_ascii=True)}\n"
        "Output JSON now."
    )

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0,
            "responseMimeType": "application/json",
        },
    }
    return _call_gemini_json(payload, model)


def _generate_profile_with_gemini_guided(
    snippets: List[Dict],
    guided_rows: List[Dict],
    model: str,
) -> Tuple[Optional[Dict], Optional[str]]:
    prompt = (
        "Generate a strict bank statement parsing profile using guided table OCR samples.\n"
        "Return one JSON object only, no markdown.\n"
        "Required keys:\n"
        "profile_name, detection_contains_any, detection_contains_all, date_tokens, description_tokens, "
        "debit_tokens, credit_tokens, balance_tokens, date_order, noise_tokens, "
        "account_name_patterns, account_number_patterns.\n"
        "Rules:\n"
        "- date_order values must be from [mdy, dmy, ymd].\n"
        "- all arrays contain strings only.\n"
        "- detection_contains_any and detection_contains_all cannot both be empty.\n"
        "- infer headers/tokens from guided OCR rows and snippets.\n"
        "- do not invent bank/account names as profile names; profile_name must represent a bank layout.\n"
        "Example shape:\n"
        "{\"profile_name\":\"AUTO_EXAMPLE_BANK\",\"detection_contains_any\":[\"example bank\"],"
        "\"detection_contains_all\":[],\"date_tokens\":[\"date\"],\"description_tokens\":[\"description\"],"
        "\"debit_tokens\":[\"debit\"],\"credit_tokens\":[\"credit\"],\"balance_tokens\":[\"balance\"],"
        "\"date_order\":[\"mdy\"],\"noise_tokens\":[],\"account_name_patterns\":[],\"account_number_patterns\":[]}\n"
        f"Guided rows: {json.dumps(guided_rows, ensure_ascii=True)}\n"
        f"Statement snippets: {json.dumps(snippets, ensure_ascii=True)}\n"
        "Output JSON now."
    )
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0,
            "responseMimeType": "application/json",
        },
    }
    return _call_gemini_json(payload, model)


def _build_guided_rows(guided_payload: Dict) -> List[Dict]:
    rows = guided_payload.get("rows") if isinstance(guided_payload, dict) else None
    if not isinstance(rows, list):
        return []
    out: List[Dict] = []
    for row in rows[:60]:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "date": str(row.get("date") or "")[:64],
                "description": str(row.get("description") or "")[:180],
                "debit": str(row.get("debit") or "")[:64],
                "credit": str(row.get("credit") or "")[:64],
                "balance": str(row.get("balance") or "")[:64],
            }
        )
    return out


def _call_gemini_json(payload: Dict, model: str) -> Tuple[Optional[Dict], Optional[str]]:
    api_key = str(os.getenv("GEMINI_API_KEY", "")).strip()
    if not api_key:
        return None, "missing_api_key"
    timeout = int(os.getenv("AI_ANALYZER_TIMEOUT_SEC", "20"))

    max_retries = max(0, int(os.getenv("AI_ANALYZER_RETRIES", "2")))
    backoff_sec = max(0.1, float(os.getenv("AI_ANALYZER_RETRY_BACKOFF_SEC", "1.2")))

    raw = ""
    last_reason = None
    for attempt in range(max_retries + 1):
        body = json.dumps(payload).encode("utf-8")
        encoded_model = urllib.parse.quote(model, safe="")
        encoded_key = urllib.parse.quote(api_key, safe="")
        req = urllib.request.Request(
            f"https://generativelanguage.googleapis.com/v1beta/models/{encoded_model}:generateContent?key={encoded_key}",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as res:
                raw = res.read().decode("utf-8", errors="ignore")
            last_reason = None
            break
        except urllib.error.HTTPError as exc:
            last_reason = f"http_error_{exc.code}"
            if exc.code in {429, 500, 502, 503, 504} and attempt < max_retries:
                time.sleep(backoff_sec * (2 ** attempt))
                continue
            return None, last_reason
        except TimeoutError:
            last_reason = "timeout"
            if attempt < max_retries:
                time.sleep(backoff_sec * (2 ** attempt))
                continue
            return None, last_reason
        except (urllib.error.URLError, ValueError):
            last_reason = "http_error_network"
            if attempt < max_retries:
                time.sleep(backoff_sec * (2 ** attempt))
                continue
            return None, last_reason
    if last_reason and not raw:
        return None, last_reason

    try:
        parsed = json.loads(raw)
        candidates = parsed.get("candidates", [])
    except Exception:
        return None, "invalid_llm_output"

    content_parts: List[str] = []
    if isinstance(candidates, list):
        for candidate in candidates:
            parts = (((candidate or {}).get("content") or {}).get("parts") or [])
            if not isinstance(parts, list):
                continue
            for part in parts:
                text = str((part or {}).get("text") or "").strip()
                if text:
                    content_parts.append(text)

    content = "\n".join(content_parts).strip()
    if not content:
        return None, "invalid_llm_output"

    try:
        return json.loads(content), None
    except Exception:
        match = re.search(r"\{.*\}", content, flags=re.DOTALL)
        if not match:
            return None, "invalid_llm_output"


def _call_openai_json_prompt(prompt: str, model: str) -> Tuple[Optional[Dict], Optional[str]]:
    api_key = str(os.getenv("OPENAI_API_KEY", "")).strip()
    if not api_key:
        return None, "missing_api_key"

    timeout = int(os.getenv("AI_ANALYZER_TIMEOUT_SEC", "20"))
    max_retries = max(0, int(os.getenv("AI_ANALYZER_RETRIES", "2")))
    backoff_sec = max(0.1, float(os.getenv("AI_ANALYZER_RETRY_BACKOFF_SEC", "1.2")))

    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "You return only a strict JSON object with no markdown and no extra text.",
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }

    raw = ""
    last_reason = None
    for attempt in range(max_retries + 1):
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            "https://api.openai.com/v1/chat/completions",
            data=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as res:
                raw = res.read().decode("utf-8", errors="ignore")
            last_reason = None
            break
        except urllib.error.HTTPError as exc:
            last_reason = f"http_error_{exc.code}"
            if exc.code in {429, 500, 502, 503, 504} and attempt < max_retries:
                time.sleep(backoff_sec * (2 ** attempt))
                continue
            return None, last_reason
        except TimeoutError:
            last_reason = "timeout"
            if attempt < max_retries:
                time.sleep(backoff_sec * (2 ** attempt))
                continue
            return None, last_reason
        except (urllib.error.URLError, ValueError):
            last_reason = "http_error_network"
            if attempt < max_retries:
                time.sleep(backoff_sec * (2 ** attempt))
                continue
            return None, last_reason
    if last_reason and not raw:
        return None, last_reason

    try:
        parsed = json.loads(raw)
        content = (
            (((parsed.get("choices") or [{}])[0]).get("message") or {}).get("content")
            if isinstance(parsed, dict)
            else None
        )
    except Exception:
        return None, "invalid_llm_output"

    text = str(content or "").strip()
    if not text:
        return None, "invalid_llm_output"

    try:
        return json.loads(text), None
    except Exception:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            return None, "invalid_llm_output"
        try:
            return json.loads(match.group(0)), None
        except Exception:
            return None, "invalid_llm_output"
        try:
            return json.loads(match.group(0)), None
        except Exception:
            return None, "invalid_llm_output"


def _validate_proposal(
    proposal: Dict,
    layout_pages: List[Dict],
    sample_pages: int,
    min_rows: int,
    min_date_ratio: float,
    min_balance_ratio: float,
) -> Tuple[Optional[BankProfile], Optional[DetectionRule], Optional[str]]:
    generic = PROFILES.get("GENERIC")
    if not generic:
        return None, None, "missing_generic_profile"

    name = _sanitize_profile_name(str(proposal.get("profile_name") or ""))
    if not name:
        return None, None, "invalid_profile_name"
    if name in PROFILES:
        return None, None, "profile_already_exists"

    contains_any = _normalize_items(proposal.get("detection_contains_any", []))
    contains_all = _normalize_items(proposal.get("detection_contains_all", []))
    if not contains_any and not contains_all:
        return None, None, "empty_detection_rule"
    if not _is_bank_like_profile(str(proposal.get("profile_name") or ""), contains_any, contains_all):
        return None, None, "profile_not_bank_like"

    date_order = [v for v in _normalize_items(proposal.get("date_order", [])) if v in {"mdy", "dmy", "ymd"}]
    if not date_order:
        date_order = list(generic.date_order)

    candidate = BankProfile(
        name=name,
        date_tokens=_pick_tokens(proposal.get("date_tokens", []), generic.date_tokens),
        description_tokens=_pick_tokens(proposal.get("description_tokens", []), generic.description_tokens),
        debit_tokens=_pick_tokens(proposal.get("debit_tokens", []), generic.debit_tokens),
        credit_tokens=_pick_tokens(proposal.get("credit_tokens", []), generic.credit_tokens),
        balance_tokens=_pick_tokens(proposal.get("balance_tokens", []), generic.balance_tokens),
        date_order=date_order,
        noise_tokens=_normalize_items(proposal.get("noise_tokens", [])),
        ocr_backends=list(generic.ocr_backends),
        account_name_patterns=_pick_patterns(
            proposal.get("account_name_patterns", []),
            generic.account_name_patterns,
        ),
        account_number_patterns=_pick_patterns(
            proposal.get("account_number_patterns", []),
            generic.account_number_patterns,
        ),
    )

    for rule in DETECTION_RULES:
        if rule.contains_any == contains_any and rule.contains_all == contains_all:
            return None, None, "duplicate_detection_rule"
    detection_rule = DetectionRule(profile=name, contains_any=contains_any, contains_all=contains_all)

    ok, reason = _validate_parse_quality(
        candidate,
        layout_pages,
        sample_pages,
        min_rows,
        min_date_ratio,
        min_balance_ratio,
    )
    if not ok:
        return None, None, reason

    return candidate, detection_rule, None


def _validate_parse_quality(
    profile: BankProfile,
    layout_pages: List[Dict],
    sample_pages: int,
    min_rows: int,
    min_date_ratio: float,
    min_balance_ratio: float,
) -> Tuple[bool, str]:
    checked_pages = 0
    total_rows = 0
    valid_dates = 0
    valid_balances = 0

    for layout in layout_pages:
        words = layout.get("words", []) if isinstance(layout, dict) else []
        if not words:
            continue
        w = float(layout.get("width", 1) if isinstance(layout, dict) else 1)
        h = float(layout.get("height", 1) if isinstance(layout, dict) else 1)
        rows, _, _ = parse_words_page(words, w, h, profile)
        tx_rows = [row for row in rows if is_transaction_row(row, profile)]
        if not tx_rows:
            checked_pages += 1
            if checked_pages >= max(1, sample_pages):
                break
            continue
        total_rows += len(tx_rows)
        valid_dates += sum(1 for r in tx_rows if str(r.get("date") or "").strip())
        valid_balances += sum(1 for r in tx_rows if str(r.get("balance") or "").strip())
        checked_pages += 1
        if checked_pages >= max(1, sample_pages):
            break

    if total_rows < max(1, min_rows):
        return False, "quality_rows_below_threshold"

    date_ratio = valid_dates / total_rows if total_rows else 0.0
    if date_ratio < min_date_ratio:
        return False, "quality_date_ratio_below_threshold"

    balance_ratio = valid_balances / total_rows if total_rows else 0.0
    if balance_ratio < min_balance_ratio:
        return False, "quality_balance_ratio_below_threshold"

    return True, "quality_ok"


def _apply_profile_update_atomic(profile: BankProfile, rule: DetectionRule) -> Tuple[bool, str]:
    path = get_profiles_config_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        return False, "config_parent_unwritable"

    if not path.exists():
        return False, "config_missing"

    try:
        with open(path, "r+") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                raw = json.load(f)
            except Exception:
                return False, "config_invalid_json"

            profiles = raw.get("profiles", {})
            rules = raw.get("detection_rules", [])
            if profile.name in profiles:
                return False, "profile_already_exists"

            profiles[profile.name] = {
                "date_tokens": list(profile.date_tokens),
                "description_tokens": list(profile.description_tokens),
                "debit_tokens": list(profile.debit_tokens),
                "credit_tokens": list(profile.credit_tokens),
                "balance_tokens": list(profile.balance_tokens),
                "date_order": list(profile.date_order),
                "noise_tokens": list(profile.noise_tokens),
                "ocr_backends": list(profile.ocr_backends),
                "account_name_patterns": list(profile.account_name_patterns),
                "account_number_patterns": list(profile.account_number_patterns),
            }
            rules.append(
                {
                    "profile": rule.profile,
                    "contains_any": list(rule.contains_any),
                    "contains_all": list(rule.contains_all),
                }
            )
            raw["profiles"] = profiles
            raw["detection_rules"] = rules

            with tempfile.NamedTemporaryFile(
                "w",
                dir=str(path.parent),
                delete=False,
            ) as tmp:
                json.dump(raw, tmp, indent=2)
                tmp_path = tmp.name
            os.replace(tmp_path, path)
    except Exception:
        return False, "config_write_failed"

    return True, "applied"


def _sanitize_profile_name(raw: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", (raw or "").strip()).strip("_").upper()
    if not cleaned:
        return ""
    if not cleaned.startswith("AUTO_"):
        cleaned = f"AUTO_{cleaned}"
    return cleaned[:64]


def _is_bank_like_profile(raw_name: str, contains_any: List[str], contains_all: List[str]) -> bool:
    pool = " ".join([str(raw_name or ""), *contains_any, *contains_all]).lower()
    bank_markers = (
        "bank",
        "unibank",
        "aub",
        "bdo",
        "bpi",
        "rcbc",
        "eastwest",
        "ewb",
        "unionbank",
        "chinabank",
        "maybank",
        "security bank",
        "metrobank",
        "ps bank",
        "pbcom",
        "sterling",
    )
    return any(marker in pool for marker in bank_markers)


def _normalize_items(values) -> List[str]:
    if not isinstance(values, list):
        return []
    out = []
    for item in values:
        text = str(item).strip().lower()
        if text:
            out.append(text)
    return out


def _pick_tokens(values, fallback: List[str]) -> List[str]:
    picked = _normalize_items(values)
    return picked if picked else list(fallback)


def _pick_patterns(values, fallback: List[str]) -> List[str]:
    if not isinstance(values, list):
        return list(fallback)
    picked = []
    for item in values:
        text = str(item).strip()
        if not text:
            continue
        try:
            compiled = re.compile(text, flags=re.IGNORECASE | re.MULTILINE)
        except re.error:
            continue
        if compiled.groups < 1:
            text = f"({text})"
            try:
                re.compile(text, flags=re.IGNORECASE | re.MULTILINE)
            except re.error:
                continue
        picked.append(text)
    return picked if picked else list(fallback)
