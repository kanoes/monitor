"""
Email service for sending reports via SendGrid.
"""
import os
import logging
from typing import List
from pathlib import Path
import base64
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition

class EmailService:
    """Service for sending emails with attachments."""
    
    def __init__(self):
        self.logger = logging.getLogger("CoreAnalytics")
        self.sendgrid_api_key = os.environ.get("SENDGRID_API_KEY")
        self.from_email = os.environ.get("FROM_EMAIL", "noreply@company.com")
        self.to_emails = os.environ.get("TO_EMAILS", "").split(",")
        
        if not self.sendgrid_api_key:
            raise ValueError("SENDGRID_API_KEY environment variable is required")
        
        self.client = SendGridAPIClient(api_key=self.sendgrid_api_key)
    
    def send_daily_monitor_report(self, file_paths: List[str], date_str: str) -> bool:
        """Send daily monitor report via email."""
        try:
            subject = f"市場GAI使用状況レポート - {date_str}"
            
            html_content = f"""
            <html>
            <body>
                <h2>市場GAI使用状況レポート</h2>
                <p>日付: {date_str}</p>
                <p>添付ファイルをご確認ください。</p>
                <br>
                <p>このメールは自動送信されています。</p>
            </body>
            </html>
            """
            
            message = Mail(
                from_email=self.from_email,
                to_emails=self.to_emails,
                subject=subject,
                html_content=html_content
            )
            
            # Add attachments
            for file_path in file_paths:
                if Path(file_path).exists():
                    self._add_attachment(message, file_path)
            
            response = self.client.send(message)
            
            if response.status_code == 202:
                self.logger.info(f"Daily monitor report email sent successfully to {self.to_emails}")
                return True
            else:
                self.logger.error(f"Failed to send email. Status code: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error sending daily monitor report email: {e}")
            return False
    
    def _add_attachment(self, message: Mail, file_path: str) -> None:
        """Add file attachment to email message."""
        try:
            with open(file_path, 'rb') as f:
                data = f.read()
            
            file_name = Path(file_path).name
            encoded_file = base64.b64encode(data).decode()
            
            attachment = Attachment(
                FileContent(encoded_file),
                FileName(file_name),
                FileType('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
                Disposition('attachment')
            )

            # Ensure multiple attachments are kept
            try:
                message.add_attachment(attachment)
            except AttributeError:
                # Fallback in case add_attachment is unavailable
                if hasattr(message, 'attachments') and isinstance(message.attachments, list):
                    message.attachments.append(attachment)
                else:
                    message.attachments = [attachment]
            
        except Exception as e:
            self.logger.error(f"Error adding attachment {file_path}: {e}")