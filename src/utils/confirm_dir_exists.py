from pathlib import Path

def ensure_dir(p: Path, logger) -> None:
    """Ensure directory `p` exists; create recursively if missing."""
    if p.exists():
        logger.info(f"Folder exists: {p.resolve()}")
    else:
        p.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created folder: {p.resolve()}")