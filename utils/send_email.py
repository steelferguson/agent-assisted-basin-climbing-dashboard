import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


def send_insight_email(
    subject: str,
    content: str,
    to_email: str,
    from_email: str = "info@basinclimbing.com",
):
    """
    Sends the insight summary email using SendGrid.
    """
    message = Mail(
        from_email=from_email,
        to_emails=to_email,
        subject=subject,
        html_content=content,
    )

    try:
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        response = sg.send(message)
        print(f"✅ Email sent! Status: {response.status_code}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")


if __name__ == "__main__":
    send_insight_email(
        subject="Test Email",
        content="This is a test email",
        to_email="steel@basinclimbing.com",
    )
