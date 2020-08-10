

from thales.notifications.gmail import send_plain_text_gmail
from thales.notifications.telegram import send_telegram_message


def wupfh(mail_to: list, subject: str, body: str, telegram_chat_id: int):
    send_plain_text_gmail(mail_to=mail_to, subject=subject, body=body)
    msg = f"{subject}\n{body}"
    send_telegram_message(chat_id=telegram_chat_id, msg=msg)
