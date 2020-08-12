

from keyword import iskeyword
import yaml

from thales.config.paths import io_path, package_path


def list_bots() -> list:
    """Get a list of all registered bot names."""
    fp = io_path(filename="bots.yaml")
    with open(fp) as stream:
        bots = yaml.safe_load(stream)
    return list() if not bots else sorted(bots)


def register_bot(bot: str):
    """Register a new unique bot name and create template directories."""
    def is_valid_variable_name(name: str):
        return name.isidentifier() and not iskeyword(name)
    assert is_valid_variable_name(bot), f"Name must be a valid Python variable."
    existing = list_bots()
    assert bot.lower() not in [s.lower() for s in existing], f"Bot name already registered: {bot}"
    io_path("bot_data", bot)
    package_path("bots", bot, "test", filename="__init__.py")
    package_path("bots", bot, "production", filename="__init__.py")
    updated_list = sorted(existing + [bot])
    fp = io_path(filename="bots.yaml")
    with open(fp, "w") as stream:
        yaml.safe_dump(updated_list, stream)


def validate_bot_name(bot: str) -> str:
    """Check if a bot name is a valid registered name, and return the properly
    formatted string name."""
    lower_to_real = {b.lower(): b for b in list_bots()}
    assert bot.lower() in lower_to_real, f"Not a valid registered bot name: {bot}"
    return lower_to_real[bot.lower()]
