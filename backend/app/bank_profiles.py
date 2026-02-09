from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class BankProfile:
    name: str
    date_tokens: List[str]
    debit_tokens: List[str]
    credit_tokens: List[str]
    balance_tokens: List[str]
    date_order: List[str]
    noise_tokens: List[str]
    ocr_backends: List[str]


PROFILES: Dict[str, BankProfile] = {
    "GENERIC": BankProfile(
        name="GENERIC",
        date_tokens=["date", "book date", "posting date", "value date", "transaction date"],
        debit_tokens=["debit", "debits", "withdrawal", "withdrawals"],
        credit_tokens=["credit", "credits", "deposit", "deposits"],
        balance_tokens=["balance", "ending balance", "closing balance", "end balance", "running balance"],
        date_order=["mdy", "dmy", "ymd"],
        noise_tokens=["balance at period", "beginning balance", "brought forward"],
        ocr_backends=["paddleocr", "tesseract", "easyocr"],
    ),
    "AUB_BDO": BankProfile(
        name="AUB_BDO",
        date_tokens=["date", "book date"],
        debit_tokens=["debit"],
        credit_tokens=["credit"],
        balance_tokens=["balance", "closing balance"],
        date_order=["dmy", "mdy", "ymd"],
        noise_tokens=["balance carried forward", "balance at period"],
        ocr_backends=["paddleocr", "tesseract", "easyocr"],
    ),
    "EWB": BankProfile(
        name="EWB",
        date_tokens=["book date", "value date", "date"],
        debit_tokens=["debit", "withdrawal"],
        credit_tokens=["credit", "deposit"],
        balance_tokens=["closing balance", "balance"],
        date_order=["dmy", "mdy", "ymd"],
        noise_tokens=["balance at period"],
        ocr_backends=["paddleocr", "tesseract", "easyocr"],
    ),
    "MAYBANK": BankProfile(
        name="MAYBANK",
        date_tokens=["posting date", "date"],
        debit_tokens=["debit", "withdrawal"],
        credit_tokens=["credit", "deposit"],
        balance_tokens=["end balance", "ending balance", "balance"],
        date_order=["dmy", "mdy", "ymd"],
        noise_tokens=["total debit", "total credit", "begin balance", "end balance"],
        ocr_backends=["paddleocr", "tesseract", "easyocr"],
    ),
    "UNIONBANK": BankProfile(
        name="UNIONBANK",
        date_tokens=["date", "value date"],
        debit_tokens=["debits", "debit"],
        credit_tokens=["credits", "credit"],
        balance_tokens=["ending balance", "balance"],
        date_order=["ymd", "mdy", "dmy"],
        noise_tokens=["available balance", "current balance", "period covered"],
        ocr_backends=["paddleocr", "tesseract", "easyocr"],
    ),
    "BPI": BankProfile(
        name="BPI",
        date_tokens=["date", "transaction date", "posting date"],
        debit_tokens=["debit", "debits", "withdrawal", "withdrawals"],
        credit_tokens=["credit", "credits", "deposit", "deposits"],
        balance_tokens=["balance", "ending balance", "running balance"],
        date_order=["mdy", "dmy", "ymd"],
        noise_tokens=["beginning balance", "available balance", "total"],
        ocr_backends=["paddleocr", "tesseract", "easyocr"],
    ),
    "CHINABANK": BankProfile(
        name="CHINABANK",
        date_tokens=["date", "txn date", "transaction date", "value date"],
        debit_tokens=["debit", "debits", "withdrawal", "withdrawals"],
        credit_tokens=["credit", "credits", "deposit", "deposits"],
        balance_tokens=["balance", "ending balance", "running balance", "outstanding balance"],
        date_order=["mdy", "dmy", "ymd"],
        noise_tokens=["total debit", "total credit", "balance forward"],
        ocr_backends=["paddleocr", "tesseract", "easyocr"],
    ),
}


def detect_bank_profile(page_text: str) -> BankProfile:
    lower = (page_text or "").lower()

    # Ordered from most specific to generic.
    if "chinabank" in lower:
        return PROFILES["CHINABANK"]
    if "bank of the philippine islands" in lower or " bpi " in f" {lower} ":
        return PROFILES["BPI"]
    if "unionbank" in lower or "transaction id" in lower:
        return PROFILES["UNIONBANK"]
    if "maybank" in lower or "posting date" in lower:
        return PROFILES["MAYBANK"]
    if "book date" in lower and "closing balance" in lower:
        return PROFILES["EWB"]
    if "aub" in lower or "check no." in lower or "tc" in lower:
        return PROFILES["AUB_BDO"]
    if "bdo" in lower:
        return PROFILES["AUB_BDO"]

    return PROFILES["GENERIC"]
