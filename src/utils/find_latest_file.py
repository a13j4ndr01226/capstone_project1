from __future__ import annotations
import re
from pathlib import Path

def find_latest_raw_nested(
    root: Path,
    expected_template: str = "spotify_rising_with_trends_{date}.csv",
    logger=None,
):
    """
    Find the latest subfolder named YYYY_MM_DD under `root` and, inside it,
    the file named by `expected_template` with {date} substituted.

    Returns:
        (Path, str): (path_to_file, 'YYYY_MM_DD') if found, else (None, None).

    Example:
        >>> from pathlib import Path
        >>> root = Path("data/raw")
        >>> file_path, batch_date = find_latest_raw_nested(
        ...     root,
        ...     expected_template="spotify_rising_cleaned_{date}.csv"
        ... )
        >>> print(file_path)
        data/raw/2025_08_31/spotify_rising_cleaned_2025_08_31.csv
        >>> print(batch_date)
        2025_08_31
    """
    date_pat = re.compile(r"^\d{4}_\d{2}_\d{2}$")
    if logger:
        logger.info(f"Searching for latest batch under: {root.resolve()}")
    if not root.exists():
        if logger:
            logger.warning(f"Root not found: {root.resolve()}")
        return None, None

    subdirs = [p for p in root.iterdir() if p.is_dir() and date_pat.match(p.name)]
    if not subdirs:
        if logger:
            logger.warning(f"No dated subfolders under: {root.resolve()}")
        return None, None

    subdirs.sort(key=lambda d: d.name)  # YYYY_MM_DD sorts lexicographically
    latest_dir = subdirs[-1]
    batch_date = latest_dir.name

    expected_name = expected_template.format(date=batch_date)
    file_pat = re.compile(rf"^{re.escape(expected_name)}$", re.I)  # case-insensitive
    candidate = next((p for p in latest_dir.iterdir() if p.is_file() and file_pat.match(p.name)), None)
    if not candidate:
        if logger:
            logger.warning(f"Expected file not found in latest batch: {latest_dir.resolve()}/{expected_name}")
        return None, None

    if logger:
        logger.info(f"Latest input: {candidate.resolve()} (batch_date={batch_date})")
    return candidate, batch_date
