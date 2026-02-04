import re

DATE_RE = re.compile(r"\b\d{2}/\d{2}/\d{4}\b")
AMOUNT_RE = re.compile(r"[-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})")

def parse_row(text: str):
    result = {
        "date": None,
        "description": None,
        "debit": None,
        "credit": None,
        "balance": None
    }

    # DATE
    date_match = DATE_RE.search(text)
    if date_match:
        result["date"] = date_match.group()

    # AMOUNTS (right-aligned heuristic)
    amounts = AMOUNT_RE.findall(text)
    amounts = [a.replace(",", "") for a in amounts]

    if len(amounts) >= 2:
        result["balance"] = amounts[-1]

        # Heuristic: debit usually comes before balance
        result["debit"] = amounts[-2]
    elif len(amounts) == 1:
        result["balance"] = amounts[0]

    # DESCRIPTION = text minus date & amounts
    desc = text
    if result["date"]:
        desc = desc.replace(result["date"], "")
    for a in amounts:
        desc = desc.replace(a, "")
    result["description"] = " ".join(desc.split()).strip()

    return result
