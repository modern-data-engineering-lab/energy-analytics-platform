"""
Tests the pure canonicalization logic directly — no Spark, no Glue, no AWS. Every case here is
drawn from real values observed in the actual IBEDC dataset (see README.md's "How the cleaning
was actually validated" for the full methodology this was extracted from), not invented edge
cases.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "etl"))

from transforms import canonicalize_feeder, canonicalize_outage_type  # noqa: E402


class TestCanonicalizeFeeder:
    def test_exact_canonical_name_unchanged(self):
        assert canonicalize_feeder("OLUYOLE") == "OLUYOLE"

    def test_whitespace_and_suffix_variants_collapse(self):
        for raw in ["INDUSTRIAL 33KV", "INDUSTRIAL 33KV ", "INDUSTRIAL", "INDUTRIAL 33KV"]:
            assert canonicalize_feeder(raw) == "INDUSTRIAL"

    def test_known_typos_are_corrected(self):
        assert canonicalize_feeder("M1NISTER 33KV") == "MINISTER"
        assert canonicalize_feeder("SAMANDA 33KV") == "SAMONDA"
        assert canonicalize_feeder("OLUYOLY 33KV") == "OLUYOLE"

    def test_agodi_1_and_2_are_distinguished(self):
        assert canonicalize_feeder("AGODI 1 33KV") == "AGODI 1"
        assert canonicalize_feeder("AGODI LINE 2 33KV") == "AGODI 2"
        assert canonicalize_feeder("AGODI 1, 33KV") == "AGODI 1"

    def test_eruwa_town_vs_eruwa_lanlate_are_distinguished(self):
        assert canonicalize_feeder("ERUWA TOWN 33KV FDR") == "ERUWA TOWN"
        assert canonicalize_feeder("ERUWA/LANLATE 33KV FDR") == "ERUWA/LANLATE"
        assert canonicalize_feeder("ERUW/LANLATE 33KV FDR") == "ERUWA/LANLATE"

    def test_transformer_events_are_unmapped_not_forced(self):
        # These are genuinely a different asset category, not a typo of a named feeder —
        # confirmed by hand against every unique unmapped value in the real dataset.
        for raw in ["T-2B @ JERICHO", "JERICHO COMPLEX", "T1 15MVA @ AGODI", "40MVA @ JERICHO"]:
            assert canonicalize_feeder(raw) == "UNMAPPED"

    def test_none_is_unmapped(self):
        assert canonicalize_feeder(None) == "UNMAPPED"

    def test_apete_and_apata_are_distinguished(self):
        # Two different real feeders with similar names — must not collapse into one.
        assert canonicalize_feeder("APETE 33KV") == "APETE"
        assert canonicalize_feeder("APATA 33KV FDR") == "APATA"


class TestCanonicalizeOutageType:
    def test_exact_canonical_type_unchanged(self):
        assert canonicalize_outage_type("EDC F/O") == "EDC F/O"

    def test_punctuation_and_spacing_variants_collapse(self):
        for raw in ["EDC F/O", "EDC/F/O", "EDCF/O", "EDC /F/O", "EDC/FO"]:
            assert canonicalize_outage_type(raw) == "EDC F/O"

    def test_zero_digit_typo_is_treated_as_letter_o(self):
        assert canonicalize_outage_type("EDC/P/0") == "EDC P/O"

    def test_all_nine_canonical_types_are_reachable(self):
        cases = {
            "EDC F/O": "EDC F/O", "EDC/E/F": "EDC E/F", "TCN/L/S": "TCN L/S",
            "EDC L/S": "EDC L/S", "EDC P/O": "EDC P/O", "EDC/O/C": "EDC O/C",
            "TCN P/O": "TCN P/O", "TCN F/O": "TCN F/O", "GEN S/C": "GEN S/C",
        }
        for raw, expected in cases.items():
            assert canonicalize_outage_type(raw) == expected

    def test_genuinely_ambiguous_value_is_other_not_guessed(self):
        # "EDC /S" is missing a token and could plausibly be several different real types —
        # left as OTHER rather than force-mapped, same discipline as the transformer-event case.
        assert canonicalize_outage_type("EDC /S") == "OTHER"

    def test_none_is_other(self):
        assert canonicalize_outage_type(None) == "OTHER"
