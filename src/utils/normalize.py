import re

def normalize_text(text):
    """
    Normalizes a string by converting it to lowercase and removing non-alphanumeric characters,
    while preserving spaces. Useful for consistent text comparison (e.g., playlist titles).

    Parameters
    text : str
        The input string to normalize.

    Returns
    str
        A normalized version of the input string with lowercase letters and no punctuation or emojis.
    """
    text = text.strip().lower()
    return re.sub(r'[^\w\s]', '', text)
