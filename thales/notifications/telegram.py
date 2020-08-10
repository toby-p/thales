
import requests
from requests.utils import requote_uri

from thales.config.notifications import get_credentials


def send_telegram_message(chat_id: int, msg: str,
                          bot_username: str = "toby_trading_bot"):
    token = get_credentials("telegram", bot_username)["token"]
    url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={requote_uri(msg)}"
    _ = requests.get(url, timeout=10)
