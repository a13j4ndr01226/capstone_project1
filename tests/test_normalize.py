from src.utils.normalize import normalize_text


def test_normalize_text_strips_whitespace_and_lowercases():
    result = normalize_text("  Hello World  ")
    assert result == "hello world"


def test_normalize_text_removes_punctuation():
    result = normalize_text("Pop, Rock & Roll!")
    assert result == "pop rock  roll"


def test_normalize_text_preserves_spaces():
    result = normalize_text("New York City")
    assert result == "new york city"


def test_normalize_text_removes_symbols_and_emojis():
    result = normalize_text("EDM🔥 #1!")
    assert result == "edm 1"