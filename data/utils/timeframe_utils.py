"""
Utility functions for timeframe calculations and conversions.
"""

from datetime import datetime, timedelta, timezone, time
import re
from typing import Tuple, Dict, Any, Optional, List

# Timeframe regex patterns for parsing
TF_REGEX = {
    'minutes': re.compile(r'^(\d+)m$'),
    'hours': re.compile(r'^(\d+)h$'),
    'days': re.compile(r'^(\d+)d$'),
    'weeks': re.compile(r'^(\d+)w$'),
    'months': re.compile(r'^(\d+)M$')
}

# Conversion factors to milliseconds
MS_CONVERSION = {
    'minutes': 60 * 1000,
    'hours': 60 * 60 * 1000,
    'days': 24 * 60 * 60 * 1000,
    'weeks': 7 * 24 * 60 * 60 * 1000,
    'months': 30 * 24 * 60 * 60 * 1000  # Approximate
}


def parse_timeframe(timeframe: str) -> Tuple[int, str]:
    """
    Parse a timeframe string into a value and unit.
    
    Args:
        timeframe: Timeframe string (e.g., "1m", "4h", "1d")
        
    Returns:
        Tuple of (value, unit)
        
    Raises:
        ValueError: If the timeframe format is invalid
    """
    for unit, pattern in TF_REGEX.items():
        match = pattern.match(timeframe)
        if match:
            return int(match.group(1)), unit
    
    raise ValueError(f"Invalid timeframe format: {timeframe}")


def timeframe_to_ms(timeframe: str) -> int:
    """
    Convert a timeframe string to milliseconds.
    
    Args:
        timeframe: Timeframe string (e.g., "1m", "4h", "1d")
        
    Returns:
        Milliseconds equivalent
    """
    value, unit = parse_timeframe(timeframe)
    return value * MS_CONVERSION[unit]


def is_custom_timeframe(timeframe: str, standard_timeframes: List[str]) -> bool:
    """
    Check if a timeframe is a custom (non-standard) timeframe.
    
    Args:
        timeframe: Timeframe to check
        standard_timeframes: List of standard timeframes supported by the exchange
        
    Returns:
        True if custom timeframe, False if standard
    """
    return timeframe not in standard_timeframes

def get_reference_timestamp(reference: str, close_timestamp: datetime, timeframe: str = None) -> datetime:
    """
    Get a reference timestamp based on the reference type, adjusted for close timestamp.
    
    Args:
        reference: Reference type ('epoch', 'midnight', 'week_start', 'month_start')
        close_timestamp: Closing timestamp of the candle
        timeframe: Timeframe of the candle (to adjust reference date if needed)
        
    Returns:
        Reference datetime
    """
    # For close timestamps, we may need to adjust the date
    # Check if this is a potential edge case (close to midnight)
    is_close_to_midnight = close_timestamp.hour == 23 and close_timestamp.minute >= 55
    
    # For timeframes that end at midnight, use the next day as reference
    if is_close_to_midnight and timeframe and any(tf in timeframe for tf in ['h', 'd', 'w']):
        # Use the next day as the reference date
        reference_date = (close_timestamp + timedelta(days=1)).date()
    else:
        reference_date = close_timestamp.date()
        
    if reference == 'epoch':
        # Unix epoch: January 1, 1970, at 00:00:00 UTC
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    
    elif reference == 'midnight':
        # Midnight UTC of the reference day
        return datetime.combine(reference_date, time(0, 0), tzinfo=timezone.utc)
    
    elif reference == 'week_start':
        # Start of the week (Monday 00:00 UTC)
        date_obj = datetime.combine(reference_date, time(0, 0), tzinfo=timezone.utc)
        days_since_monday = date_obj.weekday()
        return date_obj - timedelta(days=days_since_monday)
    
    elif reference == 'month_start':
        # Start of the month (1st day 00:00 UTC)
        return datetime(reference_date.year, reference_date.month, 1, tzinfo=timezone.utc)
    
    else:
        raise ValueError(f"Unknown reference type: {reference}")


def calculate_candle_boundaries(close_timestamp: datetime, 
                               timeframe_config: Dict[str, Any]) -> Tuple[datetime, datetime]:
    """
    Calculate the start and end boundaries of a custom timeframe candle 
    based on a candle's close timestamp.
    
    Args:
        close_timestamp: Closing timestamp of the candle
        timeframe_config: Configuration for the timeframe alignment
        
    Returns:
        Tuple of (start_time, end_time)
    """
    # Get alignment configuration
    alignment = timeframe_config['alignment']
    reference_type = alignment['reference']
    base_timeframe = timeframe_config.get('base_timeframe', '1m')
    
    # Get reference timestamp with adjusted logic for close times
    reference_timestamp = get_reference_timestamp(
        reference_type, close_timestamp, base_timeframe
    )
    
    # Determine interval in milliseconds
    interval_ms = 0
    if 'interval_minutes' in alignment:
        interval_ms = alignment['interval_minutes'] * 60 * 1000
    elif 'interval_hours' in alignment:
        interval_ms = alignment['interval_hours'] * 60 * 60 * 1000
    elif 'interval_days' in alignment:
        interval_ms = alignment['interval_days'] * 24 * 60 * 60 * 1000
    elif 'interval_weeks' in alignment:
        interval_ms = alignment['interval_weeks'] * 7 * 24 * 60 * 60 * 1000
    else:
        raise ValueError("No interval found in alignment configuration")
    
    # Calculate elapsed milliseconds since reference
    close_ts_ms = int(close_timestamp.timestamp() * 1000)
    reference_ms = int(reference_timestamp.timestamp() * 1000)
    elapsed_ms = close_ts_ms - reference_ms
    
    # For a close timestamp, we need to find which interval it belongs to
    intervals = elapsed_ms // interval_ms
    
    # Calculate start and end timestamps for this interval
    start_ms = reference_ms + (intervals * interval_ms)
    end_ms = start_ms + interval_ms - 1  # End is inclusive
    
    # Convert back to datetime
    start_time = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    end_time = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
    
    # Check if the close time is exactly at the interval boundary
    if close_ts_ms == end_ms + 1:
        # This candle closes exactly at the end of the interval
        # Make sure we assign it to the correct interval
        pass  # The calculation already handles this correctly
    
    return start_time, end_time

def get_base_timeframe_for_custom(custom_timeframe: str, config: Dict[str, Any]) -> Optional[str]:
    """
    Get the base timeframe for a custom timeframe from config.
    
    Args:
        custom_timeframe: Custom timeframe string (e.g., "2h")
        config: Configuration dictionary with custom timeframe mappings
        
    Returns:
        Base timeframe string or None if not found
    """
    try:
        return config['data']['custom_timeframes']['mappings'][custom_timeframe]['base_timeframe']
    except (KeyError, TypeError):
        return None


def get_all_custom_timeframes(config: Dict[str, Any]) -> List[str]:
    """
    Get all configured custom timeframes.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        List of custom timeframe strings
    """
    try:
        return list(config['data']['custom_timeframes']['mappings'].keys())
    except (KeyError, TypeError):
        return []


def get_custom_timeframes_for_base(base_timeframe: str, config: Dict[str, Any]) -> List[str]:
    """
    Get all custom timeframes that use a specific base timeframe.
    
    Args:
        base_timeframe: Base timeframe string
        config: Configuration dictionary
        
    Returns:
        List of custom timeframe strings
    """
    try:
        mappings = config['data']['custom_timeframes']['mappings']
        return [tf for tf, cfg in mappings.items() if cfg['base_timeframe'] == base_timeframe]
    except (KeyError, TypeError):
        return []


def is_timeframe_enabled(config: Dict[str, Any]) -> bool:
    """
    Check if custom timeframe processing is enabled in config.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        True if enabled, False otherwise
    """
    try:
        return config['data']['custom_timeframes']['enabled']
    except (KeyError, TypeError):
        return False