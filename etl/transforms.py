"""
Pure transform functions — no pyspark/awsglue imports, so these are unit-testable without a
Spark session or a Glue environment (see tests/test_transforms.py). silver_transform.py wraps
these as Spark UDFs; this module is where the actual logic lives and gets validated.

Both canonicalization functions were validated against the real 6,609-row IBEDC dataset before
being written here — see README.md's "How the cleaning was actually validated, not just
written" for the exact methodology and results (99.95% / 98.0% match rates).
"""

import re

CANONICAL_FEEDERS = [
    "OLUYOLE", "INTERCHANGE", "EXPRESS", "LIBERTY", "INDUSTRIAL", "ROM", "APETE",
    "AGODI 1", "AGODI 2", "MINISTER", "SAMONDA", "FAN MILK", "ERUWA TOWN",
    "ERUWA/LANLATE", "OMI ADIO", "APATA", "IYAGANKU", "ELEYELE",
]

# Real typos observed in the source data (validated against all 6,609 rows — see README.md).
# Fixed explicitly, not via a generic fuzzy-matcher, so every correction is auditable.
KNOWN_FEEDER_TYPOS = {
    "INDUSRIAL": "INDUSTRIAL", "INDUTRIAL": "INDUSTRIAL",
    "INTERCHANE": "INTERCHANGE", "INTERCHNAGE": "INTERCHANGE", "INTRCHANGE": "INTERCHANGE",
    "LBERTY": "LIBERTY", "LIBERTRY": "LIBERTY",
    "M1NISTER": "MINISTER", "MINISTETR": "MINISTER", "MINSTER": "MINISTER",
    "OLUYOLY": "OLUYOLE", "OLYOLE": "OLUYOLE",
    "SAMANDA": "SAMONDA", "SAMODA": "SAMONDA",
    "ERUW": "ERUWA",
    "OMI-ADIO": "OMI ADIO",
}

CANONICAL_OUTAGE_TYPES = {
    "EDCFO": "EDC F/O", "EDCEF": "EDC E/F", "EDCLS": "EDC L/S", "EDCPO": "EDC P/O",
    "EDCOC": "EDC O/C", "TCNLS": "TCN L/S", "TCNFO": "TCN F/O", "TCNPO": "TCN P/O",
    "GENSC": "GEN S/C",
}

# Transformer/substation-level events (T1/T2/T-2A/T-2B designators, MVA capacity ratings,
# "JERICHO COMPLEX") — a genuinely different asset category from the 18 named feeders, checked
# *before* substation-name matching below. Without this check first, a string like
# "T1 15MVA @ AGODI" (a transformer at the Agodi substation) was incorrectly matching the
# "AGODI" + digit "1" pattern and getting mapped to the "AGODI 1" *feeder* — a real bug this
# module's unit tests caught (see tests/test_transforms.py), not a hypothetical one.
TRANSFORMER_INDICATORS = re.compile(r"\bMVA\b|JERICHO|COMPLE?X|\bT[\s\-]?\d[A-Z]?\b|\bTI\b")


def canonicalize_feeder(raw: str) -> str:
    """Maps a raw feeder-name string to one of the 18 canonical feeders, or "UNMAPPED" for
    transformer/substation-level events that are genuinely not one of the 18 (not a typo of
    one — validated by hand against every unique unmapped value, see README.md)."""
    if raw is None:
        return "UNMAPPED"
    s = raw.upper().strip()
    if TRANSFORMER_INDICATORS.search(s):
        return "UNMAPPED"
    for typo, fix in KNOWN_FEEDER_TYPOS.items():
        s = s.replace(typo, fix)
    s = re.sub(r"[,.\-]", " ", s)
    s = re.sub(r"\b(FDR|FEEDER|LINE|RESTORED|33KLV|33IV|33BKV|\d+KV|\d+MVA|@)\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    n_nospace = s.replace(" ", "")

    if "AGODI" in n_nospace:
        if "1" in n_nospace:
            return "AGODI 1"
        if "2" in n_nospace:
            return "AGODI 2"
        return "UNMAPPED"
    if "ERUWA" in n_nospace:
        if "LANLATE" in n_nospace or "LANALTE" in n_nospace:
            return "ERUWA/LANLATE"
        if "TOWN" in n_nospace:
            return "ERUWA TOWN"
        return "UNMAPPED"
    if "APETE" in n_nospace:
        return "APETE"
    if "APATA" in n_nospace:
        return "APATA"
    for canon in CANONICAL_FEEDERS:
        canon_key = canon.replace(" ", "").replace("/", "")
        if canon_key in n_nospace or n_nospace in canon_key:
            return canon
    return "UNMAPPED"


def canonicalize_outage_type(raw: str) -> str:
    """Maps a raw outage-type string to one of 9 canonical types, or "OTHER" for values that
    don't confidently match any of them (validated: exactly 3 of 6,609 rows, "EDC /S" — left
    as OTHER rather than guessed, see README.md)."""
    if raw is None:
        return "OTHER"
    s = raw.upper().strip()
    s = re.sub(r"[\s/.\-]", "", s)
    s = s.replace("0", "O")
    return CANONICAL_OUTAGE_TYPES.get(s, "OTHER")
