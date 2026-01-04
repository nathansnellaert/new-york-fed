"""Connector-specific utility functions for New York Fed data."""

from datetime import datetime


def parse_date(date_str, formats=None):
    """Parse a date string to a date object.

    Args:
        date_str: Date string to parse
        formats: List of date formats to try. Defaults to ["%Y-%m-%d", "%m/%d/%Y"]

    Returns:
        date object or None if parsing fails
    """
    if not date_str:
        return None

    if formats is None:
        formats = ["%Y-%m-%d", "%m/%d/%Y"]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None


def parse_number(value):
    """Parse a numeric value, handling None, empty strings, 'NA', and comma-formatted numbers.

    Args:
        value: Value to parse (string, int, float, or None)

    Returns:
        float or None if parsing fails
    """
    if value is None or value == "" or value == "NA":
        return None
    try:
        if isinstance(value, str):
            value = value.replace(",", "")
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_integer(value):
    """Parse an integer value.

    Args:
        value: Value to parse

    Returns:
        int or None if parsing fails
    """
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def parse_timestamp(timestamp_str):
    """Parse a timestamp string to a datetime object.

    Args:
        timestamp_str: Timestamp string to parse

    Returns:
        datetime object or None if parsing fails
    """
    if not timestamp_str:
        return None
    try:
        return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M")
        except ValueError:
            return None


def parse_bool(value):
    """Parse a boolean value from various representations.

    Args:
        value: Value to parse

    Returns:
        bool or None if value is None/empty
    """
    if value is None or value == "":
        return None
    return str(value).lower() in ['true', '1', 'yes']
