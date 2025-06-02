import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

MAILGUN_DOMAIN = os.getenv('MAILGUN_DOMAIN')
MAILGUN_API_KEY = os.getenv('MAILGUN_API_KEY')
MAILGUN_SENDER = os.getenv('MAILGUN_SENDER')  # Example: "Automation Bot <bot@yourdomain.com>"

class EmailSender:
    def __init__(self):
        """Initialize the email sender."""
        if not all([MAILGUN_DOMAIN, MAILGUN_API_KEY, MAILGUN_SENDER]):
            raise ValueError("Missing Mailgun API credentials. Check your .env file.")

    def create_email_body(self, first_name, last_name):
        """Creates the email body using a template."""
        return (
            f"Hello {first_name},\n\n"
            "Your Penelope Case Audit is attached.\n\n"
            "Best regards,\n"
            "Your Team"
        )

    def send_email_with_attachment(self, recipient_info):
        """Send an email with an attachment to a recipient using Mailgun."""
        subject = f'Penelope Case Audit for {recipient_info["first name"]} {recipient_info["last name"]}'
        body = self.create_email_body(recipient_info['first name'], recipient_info['last name'])
        recipient_email = recipient_info['email']

        filename = recipient_info['filename']
        attachment = None

        if os.path.isfile(filename):
            attachment = open(filename, 'rb')
        else:
            print(f"Attachment file not found: {filename}")

        # Send email via Mailgun API
        response = requests.post(
            f"https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages",
            auth=("api", MAILGUN_API_KEY),
            files=[("attachment", (os.path.basename(filename), attachment))] if attachment else None,
            data={
                "from": MAILGUN_SENDER,
                "to": recipient_email,
                "subject": subject,
                "text": body,
            }
        )

        # Close file if it was opened
        if attachment:
            attachment.close()

        if response.status_code == 200:
            print(f"Email sent successfully to {recipient_email}")
        else:
            print(f"Failed to send email to {recipient_email}: {response.status_code}, {response.text}")

    def send_email_to_admin(self, recipient_email, successful):
        """Send an email to the admin notifying script run success or failure."""
        subject = (
            "Penelope Audits Successfully Updated"
            if successful
            else "Issue Running Penelope Audit Script on Server"
        )

        body = (
            "Script ran on server with no issues."
            if successful
            else "Check the script or server process, something is up!"
        )

        # Send email via Mailgun API
        response = requests.post(
            f"https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages",
            auth=("api", MAILGUN_API_KEY),
            data={
                "from": MAILGUN_SENDER,
                "to": recipient_email,
                "subject": subject,
                "text": body,
            }
        )

        if response.status_code == 200:
            print("Admin email sent successfully.")
        else:
            print(f"Failed to send admin email: {response.status_code}, {response.text}")

    def send_worker_emails(self, worker_list):
        """Send emails to workers with case audit attachments."""
        for worker in worker_list:
            self.send_email_with_attachment(worker)

    def send_supervisor_emails(self, sup_list):
        """Send emails to supervisors with case audit attachments."""
        for sup in sup_list:
            self.send_email_with_attachment(sup)

    def send_test_emails(self, worker_list):
        """Send emails only to 'Test Worker'."""
        for worker in worker_list:
            if worker['first name'] == 'Test' and worker['last name'] == 'Worker':
                print(f"Sending test email to worker: {worker['first name']} {worker['last name']}")
                self.send_email_with_attachment(worker)































# import win32com.client as win32
# import os
#
# class EmailSender:
#     def __init__(self):
#         # Create an instance of the Outlook application
#         self.outlook = win32.Dispatch('outlook.application')
#
#     def create_email_body(self, first_name, last_name):
#         """Creates the email body using a template."""
#         body_template = (
#             f"Hello {first_name},\n\n"
#             "Your Penelope Case Audit is attached.\n\n"
#             "Best regards,\n"
#             "Your Team"
#         )
#         return body_template
#
#     def send_email_with_attachment(self, recipient_info):
#         """Send an email with an attachment to a recipient."""
#         mail = self.outlook.CreateItem(0)
#
#         # Set email parameters
#         mail.To = recipient_info['email']
#         mail.Subject = f'Penelope Case Audit for {recipient_info["first name"]} {recipient_info["last name"]}'
#         mail.Body = self.create_email_body(recipient_info['first name'], recipient_info['last name'])
#
#         filename = recipient_info['filename']
#         # Check if the file exists
#         if os.path.isfile(filename):
#             mail.Attachments.Add(filename)
#         else:
#             print(f"Attachment file not found: {filename}")
#
#         # Send the email
#         mail.Send()
#
#     def send_email_to_admin(self, recipient_email, successful):
#         """ sends email to admin, letting know run was successful or not, primarily with for use with server run automation"""
#         mail = self.outlook.CreateItem(0)
#
#         if successful:
#             # Set email parameters
#             mail.To = recipient_email
#             mail.Subject = 'Penelope Audits Successfully Updated'
#             mail.Body = 'Script ran on server with no issues.'
#
#         else:
#             # Set email parameters
#             mail.To = recipient_email
#             mail.Subject = 'Issue Running Penelope Audit Script on Server'
#             mail.Body = 'Check the script or server process, something is up!'
#
#         # Send the email
#         mail.Send()
#
#     def send_worker_emails(self, worker_list):
#         """Send emails to workers."""
#         for worker in worker_list:
#             self.send_email_with_attachment(worker)
#
#     def send_supervisor_emails(self, sup_list):
#         """Send emails to supervisors."""
#         for sup in sup_list:
#             self.send_email_with_attachment(sup)
#
#     def send_test_emails(self, worker_list):
#         """Send emails only to 'Test Worker'."""
#         # Look for 'Test Worker' in the combined list
#         for worker in worker_list.to_dict('records'):
#             if worker['first name'] == 'Test' and worker['last name'] == 'Worker':
#                 print(f"Sending test email to worker: {worker['first name']} {worker['last name']}")
#                 self.send_email_with_attachment(worker)
#
#
