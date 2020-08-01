

from keyword import iskeyword
import os
import yaml

from thales.config.paths import DIR_PACKAGE_DATA


def get_bots() -> list:
    """Load list of unique registered bot names."""
    fp = os.path.join(DIR_PACKAGE_DATA, "bots.yaml")
    with open(fp) as stream:
        bots = yaml.safe_load(stream)
    return list() if not bots else sorted(bots)


def register_bot(bot: str):
    """Register a new unique bot name."""
    def is_valid_variable_name(name: str):
        return name.isidentifier() and not iskeyword(name)
    assert is_valid_variable_name(bot), f"Name must be a valid Python variable."
    existing = get_bots()
    assert bot.lower() not in [s.lower() for s in existing], f"Bot name already registered: {bot}"
    updated_list = sorted(existing + [bot])
    fp = os.path.join(DIR_PACKAGE_DATA, "bots.yaml")
    with open(fp, "w") as stream:
        yaml.safe_dump(updated_list, stream)


def validate_bot_name(bot: str) -> str:
    """Check if a bot name is a valid registered name, and return the properly
    formatted string name."""
    lower_to_real = {b.lower(): b for b in get_bots()}
    assert bot.lower() in lower_to_real, f"Not a valid registered bot name: {bot}"
    return lower_to_real[bot.lower()]
