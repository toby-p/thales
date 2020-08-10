

import smtplib

from thales.config.notifications import get_credentials


def send_plain_text_gmail(mail_to: list, subject: str, body: str,
                          mail_from: str = "thales.trading.bot@gmail.com"):
    mail_to = [mail_to] if isinstance(mail_to, str) else mail_to
    s = smtplib.SMTP("smtp.gmail.com", 587)
    s.starttls()
    s.login(mail_from, get_credentials("gmail", mail_from)["password"])
    msg = "Subject: {}\n\n{}".format(subject, body)
    for email in mail_to:
        s.sendmail(mail_from, email, msg)
    s.quit()
