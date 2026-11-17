def retry_delay_ms(attempt: int, base_delay_sec: float) -> int:
    """Exponential backoff delay in milliseconds. attempt is 1-based."""
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    return int(base_delay_sec * (2 ** (attempt - 1)) * 1000)
