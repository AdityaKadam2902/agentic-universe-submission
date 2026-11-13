"""Pure normalization and validation functions.

All functions are pure (no side effects) and operate on strings/values.
Easy to unit test in isolation.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List
from zoneinfo import ZoneInfo

# Constants
IST = ZoneInfo("Asia/Kolkata")
UTC = timezone.utc

# Phone validation
PHONE_DIGITS_ONLY = re.compile(r"\D")
VALID_PHONE_PREFIXES = frozenset("6789")

# Date formats
DATE_FORMAT_ISO = "%Y-%m-%d"
DATE_FORMAT_DMY_SLASH = "%d/%m/%Y"
DATE_FORMAT_DMY_DASH = "%d-%m-%Y"
# Month abbreviations for DD-Mon-YYYY
MONTH_ABBR_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# Decimal prefixes (case-insensitive)
DECIMAL_PREFIX_PATTERN = re.compile(
    r"^\s*(?:[\u20b9]|Rs\.?|INR)\s*",
    re.IGNORECASE
)

# Valid comma grouping for decimals: Western (1,234,567) and Indian (1,23,456)
DECIMAL_COMMA_WESTERN_PATTERN = re.compile(r"\d{1,3}(?:,\d{3})*(?:\.\d+)?")
DECIMAL_COMMA_INDIAN_PATTERN = re.compile(r"\d{1,2}(?:,\d{2})*,\d{3}(?:\.\d+)?")


def normalize_header(raw: str) -> str:
    """
    Normalize a CSV header: lowercase, trim, collapse runs of underscores/whitespace to single space.

    Examples:
        "CUSTOMER_NAME" -> "customer name"
        "  phone_number  " -> "phone number"
        "preferred__language" -> "preferred language"
    """
    if not raw:
        return ""
    # Lowercase and strip
    result = raw.lower().strip()
    # Replace runs of underscores or whitespace with single space
    result = re.sub(r"[\s_]+", " ", result)
    return result.strip()


def normalize_header_match(normalized_header: str, schema_names: List[str],
                          aliases: List[str]) -> bool:
    """
    Check if a normalized header matches a schema field name or any of its aliases.

    All names/aliases are expected to be pre-normalized.
    """
    if normalized_header in schema_names:
        return True
    if normalized_header in aliases:
        return True
    return False


def normalize_phone(raw: str) -> Optional[str]:
    """
    Normalize a phone number to canonical form +91XXXXXXXXXX.

    Steps:
    1. Strip all non-digits
    2. If 12 digits starting with 91, remove 91
    3. If 11 digits starting with 0, remove 0
    4. Require exactly 10 digits starting with 6/7/8/9
    5. Return +91XXXXXXXXXX or None if invalid

    Returns None for invalid phone numbers (don't raise).
    """
    if not raw:
        return None

    # Strip non-digits
    digits = PHONE_DIGITS_ONLY.sub("", raw)

    if not digits:
        return None

    # Handle prefixes
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]

    # Validate: exactly 10 digits starting with 6/7/8/9
    if len(digits) != 10:
        return None

    if digits[0] not in VALID_PHONE_PREFIXES:
        return None

    return f"+91{digits}"


def parse_date(raw: str) -> Optional[datetime]:
    """
    Parse a date string in one of the accepted formats.

    Accepted formats (day-first):
    - YYYY-MM-DD
    - DD/MM/YYYY
    - DD-MM-YYYY
    - DD-Mon-YYYY (month case-insensitive, e.g., 01-Jan-2026)

    Returns a naive datetime (date only, time=00:00:00) or None if invalid.
    Raises ValueError for impossible dates (e.g., 31-Feb).
    """
    if not raw:
        return None

    raw = raw.strip()
    if not raw:
        return None

    # Try YYYY-MM-DD
    try:
        dt = datetime.strptime(raw, DATE_FORMAT_ISO)
        return dt
    except ValueError:
        pass

    # Try DD/MM/YYYY
    try:
        dt = datetime.strptime(raw, DATE_FORMAT_DMY_SLASH)
        return dt
    except ValueError:
        pass

    # Try DD-MM-YYYY
    try:
        dt = datetime.strptime(raw, DATE_FORMAT_DMY_DASH)
        return dt
    except ValueError:
        pass

    # Try DD-Mon-YYYY (case-insensitive)
    # Pattern: 1-2 digits, dash, 3-letter month, dash, 4-digit year
    match = re.match(r"^(\d{1,2})-([a-zA-Z]{3})-(\d{4})$", raw)
    if match:
        day_str, month_str, year_str = match.groups()
        day = int(day_str)
        year = int(year_str)
        month_abbr = month_str.lower()[:3]

        if month_abbr in MONTH_ABBR_MAP:
            month = MONTH_ABBR_MAP[month_abbr]
            try:
                dt = datetime(year, month, day)
                return dt
            except ValueError:
                # Invalid date (e.g., 31-Feb)
                return None

    return None


def format_date(dt: datetime) -> str:
    """Format a datetime as YYYY-MM-DD."""
    if dt is None:
        return ""
    return dt.strftime(DATE_FORMAT_ISO)


def normalize_decimal(raw: str) -> Optional[Decimal]:
    """
    Parse a decimal value from a string.

    Accepted formats:
    - Optional prefix: ₹, Rs, Rs., or INR (case-insensitive, optional whitespace)
    - Commas as thousands separators: Western (1,234,567) or Indian (1,23,456)
      grouping; malformed grouping (e.g. 1,2,3, 12,34, 1,234,56) is rejected
    - Value must be >= 0
    - Round to 2 decimal places using ROUND_HALF_UP

    Returns Decimal or None if invalid.
    """
    if not raw:
        return None

    raw = raw.strip()
    if not raw:
        return None

    # Remove currency prefix
    cleaned = DECIMAL_PREFIX_PATTERN.sub("", raw)

    # Commas must form valid thousands-separator groups; otherwise reject
    # rather than silently collapsing malformed grouping into a valid value
    if "," in cleaned and not (
        DECIMAL_COMMA_WESTERN_PATTERN.fullmatch(cleaned)
        or DECIMAL_COMMA_INDIAN_PATTERN.fullmatch(cleaned)
    ):
        return None

    # Remove commas (thousands separators)
    cleaned = cleaned.replace(",", "")

    # Try to parse as Decimal
    try:
        value = Decimal(cleaned)
    except InvalidOperation:
        return None

    # Check non-negative
    if value < 0:
        return None

    # Round to 2 decimal places
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def normalize_enum(raw: str, valid_values: List[str]) -> Optional[str]:
    """
    Normalize an enum value: case-insensitive match against valid values, return lowercase.

    Returns lowercase matched value or None if invalid.
    """
    if not raw:
        return None

    raw = raw.strip()
    if not raw:
        return None

    raw_lower = raw.lower()

    for valid in valid_values:
        if valid.lower() == raw_lower:
            return valid.lower()

    return None


def normalize_timestamp(raw: str) -> Optional[datetime]:
    """
    Parse an event timestamp and normalize to UTC.

    Accepted formats:
    - ISO 8601 with Z suffix (UTC): "2026-11-27T02:30:56Z"
    - ISO 8601 with offset: "2026-12-11T21:43:55+05:30"
    - Naive datetime (interpreted as Asia/Kolkata/IST): "2026-11-20 21:51:20"

    Returns timezone-aware datetime in UTC, or None if invalid.
    """
    if not raw:
        return None

    raw = raw.strip()
    if not raw:
        return None

    # Try parsing as ISO format with timezone
    try:
        # Python 3.7+ fromisoformat handles Z and offsets
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        # Convert to UTC
        return dt.astimezone(UTC)
    except (ValueError, AttributeError):
        pass

    # Try naive datetime format (interpreted as IST)
    naive_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ]

    for fmt in naive_formats:
        try:
            dt_naive = datetime.strptime(raw, fmt)
            # Interpret as IST
            dt_ist = dt_naive.replace(tzinfo=IST)
            # Convert to UTC
            return dt_ist.astimezone(UTC)
        except ValueError:
            continue

    return None


def to_ist(dt_utc: datetime) -> datetime:
    """Convert a UTC datetime to Asia/Kolkata (IST)."""
    if dt_utc is None:
        return None
    if dt_utc.tzinfo is None:
        # Assume UTC if naive
        dt_utc = dt_utc.replace(tzinfo=UTC)
    return dt_utc.astimezone(IST)


def format_timestamp_ist(dt_utc: datetime) -> str:
    """
    Format a UTC datetime as ISO-8601 in IST.

    Example: 2026-10-14T17:55:24+05:30
    """
    if dt_utc is None:
        return ""
    dt_ist = to_ist(dt_utc)
    # Format with offset
    offset = dt_ist.tzinfo.utcoffset(dt_ist)
    total_seconds = int(offset.total_seconds())
    hours, remainder = divmod(abs(total_seconds), 3600)
    minutes = remainder // 60
    sign = "+" if total_seconds >= 0 else "-"
    return f"{dt_ist.strftime('%Y-%m-%dT%H:%M:%S')}{sign}{hours:02d}:{minutes:02d}"


def format_timestamp_iso(dt_utc: datetime) -> str:
    """Format a UTC datetime as ISO-8601 with Z suffix."""
    if dt_utc is None:
        return ""
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=UTC)
    utc_dt = dt_utc.astimezone(UTC)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
