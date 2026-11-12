"""Tests for normalization functions."""

import pytest
from datetime import datetime

from solution.ingestion.normalization import (
    normalize_header,
    normalize_phone,
    parse_date,
    format_date,
    normalize_decimal,
    normalize_enum,
    normalize_timestamp,
    to_ist,
    format_timestamp_ist,
    format_timestamp_iso,
)


class TestHeaderNormalization:
    """Tests for header normalization."""

    def test_lowercase_and_trim(self):
        assert normalize_header("  CUSTOMER_NAME  ") == "customer name"
        assert normalize_header("PHONE") == "phone"

    def test_underscore_collapse(self):
        assert normalize_header("customer_name") == "customer name"
        assert normalize_header("preferred__language") == "preferred language"
        assert normalize_header("policy__expiry__date") == "policy expiry date"

    def test_whitespace_collapse(self):
        assert normalize_header("customer  name") == "customer name"
        assert normalize_header("phone\tnumber") == "phone number"

    def test_mixed_underscore_and_whitespace(self):
        assert normalize_header("CUSTOMER__NAME") == "customer name"
        assert normalize_header("preferred_language ") == "preferred language"
        assert normalize_header("  phone_number  ") == "phone number"

    def test_empty_string(self):
        assert normalize_header("") == ""
        assert normalize_header("   ") == ""

    def test_already_normalized(self):
        assert normalize_header("customer name") == "customer name"


class TestPhoneNormalization:
    """Tests for phone normalization."""

    def test_valid_10_digit(self):
        assert normalize_phone("9876543210") == "+919876543210"
        assert normalize_phone("+919876543210") == "+919876543210"

    def test_12_digit_with_91_prefix(self):
        assert normalize_phone("919876543210") == "+919876543210"
        assert normalize_phone("+919876543210") == "+919876543210"

    def test_11_digit_with_leading_zero(self):
        assert normalize_phone("09876543210") == "+919876543210"

    def test_with_spaces(self):
        assert normalize_phone("98765 43210") == "+919876543210"
        assert normalize_phone("98765-43210") == "+919876543210"

    def test_with_parentheses(self):
        assert normalize_phone("(98765) 43210") == "+919876543210"

    def test_invalid_length_too_short(self):
        assert normalize_phone("123456789") is None  # 9 digits

    def test_invalid_length_too_long(self):
        assert normalize_phone("987654321012") is None  # 12 digits without 91 prefix

    def test_invalid_first_digit(self):
        assert normalize_phone("5876543210") is None  # starts with 5
        assert normalize_phone("4876543210") is None  # starts with 4
        assert normalize_phone("0876543210") is None  # starts with 0 after stripping

    def test_empty_or_none(self):
        assert normalize_phone("") is None
        assert normalize_phone(None) is None

    def test_mixed_formats(self):
        assert normalize_phone("098765 43210") == "+919876543210"
        assert normalize_phone("91-98765-43210") == "+919876543210"
        assert normalize_phone("+91 (98765) 43210") == "+919876543210"


class TestDateParsing:
    """Tests for date parsing."""

    def test_iso_format(self):
        dt = parse_date("2026-10-15")
        assert dt == datetime(2026, 10, 15)

    def test_dmy_slash_format(self):
        dt = parse_date("15/10/2026")
        assert dt == datetime(2026, 10, 15)

    def test_dmy_dash_format(self):
        dt = parse_date("15-10-2026")
        assert dt == datetime(2026, 10, 15)

    def test_dmon_format_upper(self):
        dt = parse_date("15-OCT-2026")
        assert dt == datetime(2026, 10, 15)

    def test_dmon_format_lower(self):
        dt = parse_date("15-oct-2026")
        assert dt == datetime(2026, 10, 15)

    def test_dmon_format_mixed(self):
        dt = parse_date("15-Nov-2026")
        assert dt == datetime(2026, 11, 15)

    def test_dmon_single_digit_day(self):
        dt = parse_date("5-Jan-2026")
        assert dt == datetime(2026, 1, 5)

    def test_impossible_date(self):
        assert parse_date("31-02-2026") is None
        assert parse_date("29-02-2025") is None  # not a leap year
        assert parse_date("31-04-2026") is None  # April has 30 days

    def test_empty_or_none(self):
        assert parse_date("") is None
        assert parse_date(None) is None

    def test_invalid_format(self):
        assert parse_date("2026/10/15") is None
        assert parse_date("10-15-2026") is None  # month-first not accepted


class TestDecimalNormalization:
    """Tests for decimal normalization."""

    def test_plain_number(self):
        from decimal import Decimal
        assert normalize_decimal("123.45") == Decimal("123.45")

    def test_integer(self):
        from decimal import Decimal
        assert normalize_decimal("100") == Decimal("100.00")

    def test_rupee_prefix(self):
        from decimal import Decimal
        assert normalize_decimal("Rs 123.45") == Decimal("123.45")
        assert normalize_decimal("Rs. 123.45") == Decimal("123.45")

    def test_inr_prefix(self):
        from decimal import Decimal
        assert normalize_decimal("INR 123.45") == Decimal("123.45")
        assert normalize_decimal("inr 123.45") == Decimal("123.45")

    def test_rupee_symbol(self):
        from decimal import Decimal
        assert normalize_decimal("₹123.45") == Decimal("123.45")

    def test_with_commas(self):
        from decimal import Decimal
        assert normalize_decimal("1,234.56") == Decimal("1234.56")
        assert normalize_decimal("10,000.00") == Decimal("10000.00")
        assert normalize_decimal("1,23,456.78") == Decimal("123456.78")

    def test_negative_rejected(self):
        assert normalize_decimal("-100.00") is None
        assert normalize_decimal("-0.01") is None

    def test_rounding_half_up(self):
        from decimal import Decimal
        # ROUND_HALF_UP: 2.345 -> 2.35, 2.335 -> 2.34
        assert normalize_decimal("2.345") == Decimal("2.35")
        assert normalize_decimal("2.335") == Decimal("2.34")

    def test_empty_or_none(self):
        assert normalize_decimal("") is None
        assert normalize_decimal(None) is None

    def test_invalid_format(self):
        assert normalize_decimal("abc") is None
        assert normalize_decimal("12.34.56") is None

    def test_zero(self):
        assert normalize_decimal("0") == 0.00
        assert normalize_decimal("0.00") == 0.00


class TestEnumNormalization:
    """Tests for enum normalization."""

    def test_valid_lowercase(self):
        values = ["en", "hi", "ta"]
        assert normalize_enum("en", values) == "en"

    def test_valid_uppercase(self):
        values = ["en", "hi", "ta"]
        assert normalize_enum("EN", values) == "en"

    def test_valid_mixed_case(self):
        values = ["en", "hi", "ta"]
        assert normalize_enum("En", values) == "en"
        assert normalize_enum("Hi", values) == "hi"

    def test_invalid_value(self):
        values = ["en", "hi", "ta"]
        assert normalize_enum("fr", values) is None

    def test_empty_or_none(self):
        values = ["en", "hi", "ta"]
        assert normalize_enum("", values) is None
        assert normalize_enum(None, values) is None

    def test_with_whitespace(self):
        values = ["en", "hi"]
        assert normalize_enum("  en  ", values) == "en"


class TestTimestampNormalization:
    """Tests for timestamp normalization."""

    def test_utc_z_suffix(self):
        dt = normalize_timestamp("2026-11-27T02:30:56Z")
        assert dt is not None
        # Should be in UTC
        assert dt.tzinfo is not None

    def test_offset_aware(self):
        dt = normalize_timestamp("2026-12-11T21:43:55+05:30")
        assert dt is not None

    def test_naive_ist(self):
        dt = normalize_timestamp("2026-11-20 21:51:20")
        assert dt is not None
        # Should be converted to UTC
        assert dt.tzinfo is not None

    def test_naive_iso_format(self):
        dt = normalize_timestamp("2026-11-20T21:51:20")
        assert dt is not None

    def test_empty_or_none(self):
        assert normalize_timestamp("") is None
        assert normalize_timestamp(None) is None

    def test_ist_to_utc_conversion(self):
        # 12:00 IST = 06:30 UTC
        dt = normalize_timestamp("2026-11-20 12:00:00")
        assert dt is not None
        # Check UTC conversion is correct
        from datetime import timezone
        expected = datetime(2026, 11, 20, 6, 30, 0, tzinfo=timezone.utc)
        assert dt.year == expected.year
        assert dt.month == expected.month
        assert dt.day == expected.day
        assert dt.hour == expected.hour
        assert dt.minute == expected.minute


class TestISTConversion:
    """Tests for IST conversion helpers."""

    def test_to_ist(self):
        from datetime import timezone
        utc_dt = datetime(2026, 11, 20, 6, 30, 0, tzinfo=timezone.utc)
        ist_dt = to_ist(utc_dt)
        assert ist_dt.hour == 12
        assert ist_dt.minute == 0
        assert ist_dt.tzinfo is not None

    def test_format_timestamp_ist(self):
        from datetime import timezone
        utc_dt = datetime(2026, 11, 20, 6, 30, 0, tzinfo=timezone.utc)
        result = format_timestamp_ist(utc_dt)
        # IST is UTC+5:30, so 06:30 UTC = 12:00 IST
        assert "+05:30" in result
        assert "2026-11-20T12:00:00" in result

    def test_format_timestamp_iso(self):
        from datetime import timezone
        utc_dt = datetime(2026, 11, 20, 6, 30, 0, tzinfo=timezone.utc)
        result = format_timestamp_iso(utc_dt)
        assert result == "2026-11-20T06:30:00Z"

    def test_format_empty(self):
        assert format_timestamp_ist(None) == ""
        assert format_timestamp_iso(None) == ""
