import os
import smtplib
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

TOP_STOCKS_PATH = Path(__file__).resolve().parents[1] / "data" / "raw" / "top_stocks.csv"


def load_recipients():
    # Drop unset recipient env vars.
    return [recipient for recipient in [os.getenv("EMAIL_TO_1")] if recipient]


def build_message():
    df = pd.read_csv(TOP_STOCKS_PATH, dtype={"ticker": str})
    df["ticker"] = df["ticker"].str.zfill(6)
    df["close"] = df["close"].map("{:,.0f}".format)
    latest_date = pd.to_datetime(df["date"].iloc[0]).strftime("%Y-%m-%d")
    body = f"""기준일: {latest_date}

{df[["ticker", "name", "close"]].to_string(index=False)}
"""

    msg = MIMEText(body)
    msg["Subject"] = "Today's Stock Picks by Hayoung's Quant System"
    return msg


def send_email():
    load_dotenv()
    email_user = os.getenv("EMAIL_USER")
    email_app_password = os.getenv("EMAIL_APP_PASSWORD")
    recipients = load_recipients()
    # Build the message separately so formatting stays testable.
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
