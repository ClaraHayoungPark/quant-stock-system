import os
import smtplib
from email.mime.text import MIMEText

import pandas as pd
from dotenv import load_dotenv

from paths import TOP_STOCKS


def build_message():
    df = pd.read_csv(TOP_STOCKS, dtype={"ticker": str})
    df["ticker"] = df["ticker"].str.zfill(6)
    df["close"] = df["close"].map("{:,.0f}".format)
    latest_date = pd.to_datetime(df["date"].iloc[0]).strftime("%Y-%m-%d")

    body = f"기준일: {latest_date}\n\n{df[['ticker', 'name', 'close']].to_string(index=False)}\n"
    msg = MIMEText(body)
    msg["Subject"] = "Today's Stock Picks by Hayoung's Quant System"
    return msg


def send_email():
    load_dotenv()
    email_user = os.getenv("EMAIL_USER")
    email_app_password = os.getenv("EMAIL_APP_PASSWORD")
    recipients = [r for r in [os.getenv("EMAIL_TO_1")] if r]

    msg = build_message()
    msg["From"] = email_user
    msg["To"] = ", ".join(recipients)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(email_user, email_app_password)
        server.sendmail(email_user, recipients, msg.as_string())


def main():
    send_email()
    print("email sent")


if __name__ == "__main__":
    main()
