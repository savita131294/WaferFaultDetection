import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


class EmailSender:
    __sender_email_id = "yadav.tara.avnish@gmail.com"
    __receiver_email_id = "yadav.tara.avnish@gmail.com"
    __password = "OtAAp$629"

    def __init__(self):
        pass

    def sendEmail(self, mail_text, subject):
        """
        message: Message string in html format
        subject: subject of email
        """
        try:
            message = MIMEMultipart("alternative")
            message["Subject"] = subject
            message["From"] = EmailSender.__sender_email_id
            message["To"] = EmailSender.__receiver_email_id
            text = mail_text

            part1 = MIMEText(text, "plain")

            # Add HTML/plain-text parts to MIMEMultipart message
            # The email client will try to render the last part first
            message.attach(part1)

            # Create secure connection with server and send email
            context = ssl.create_default_context()
            print(message.as_string())
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                server.login(EmailSender.__sender_email_id, EmailSender.__password)
                server.sendmail(
                    EmailSender.__sender_email_id, EmailSender.__receiver_email_id, message.as_string()
                )

        except Exception as e:
            raise Exception("Error occured in class: EmailSender method: sendEmail" + str(e))
