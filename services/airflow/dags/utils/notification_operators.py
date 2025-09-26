from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
import requests
import json
import logging
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)


class WebSocketNotifyOperator(BaseOperator):
    """
    Operator for sending WebSocket notifications via backend API
    """

    def __init__(self, job_config: Dict[str, Any], message_type: str, custom_message: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_config = job_config
        self.message_type = message_type
        self.custom_message = custom_message

    def execute(self, context: Context):
        """Send notification via backend API"""

        job_id = self.job_config['job_id']
        task_instance = context.get('task_instance')

        # Create notification message
        message = self._create_notification_message(context)

        logger.info(f"Sending {self.message_type} notification for job {job_id}")

        try:
            # Send notification to backend
            response = self._send_to_backend(job_id, message)

            if response and response.status_code == 200:
                logger.info(f"Notification sent successfully for job {job_id}")
            else:
                logger.warning(f"Failed to send notification: HTTP {response.status_code if response else 'No response'}")

            return message

        except Exception as e:
            # Don't fail the task if notification fails, just log the error
            logger.error(f"Failed to send notification for job {job_id}: {str(e)}")
            return None

    def _create_notification_message(self, context: Context) -> Dict[str, Any]:
        """Create notification message based on context"""

        task_instance = context.get('task_instance')
        dag_run = context.get('dag_run')

        base_message = {
            'job_id': self.job_config['job_id'],
            'type': self.message_type,
            'timestamp': datetime.utcnow().isoformat(),
            'dag_id': dag_run.dag_id if dag_run else None,
            'task_id': task_instance.task_id if task_instance else None,
            'execution_date': context.get('ds'),
        }

        # Add message type specific data
        if self.message_type == 'task_start':
            base_message.update({
                'status': 'processing',
                'stage': task_instance.task_id,
                'message': self.custom_message or f'Starting task: {task_instance.task_id}'
            })

        elif self.message_type == 'task_complete':
            base_message.update({
                'status': 'processing',
                'stage': f'{task_instance.task_id}_complete',
                'message': self.custom_message or f'Completed task: {task_instance.task_id}'
            })

        elif self.message_type == 'processing_complete':
            base_message.update({
                'status': 'processing',
                'stage': 'processing_complete',
                'message': self.custom_message or 'Data processing completed'
            })

        elif self.message_type == 'job_complete':
            base_message.update({
                'status': 'completed',
                'stage': 'finished',
                'message': self.custom_message or 'Job completed successfully'
            })

        elif self.message_type == 'progress_update':
            # Try to get progress information from task instance
            progress_data = self._extract_progress_data(context)
            base_message.update({
                'status': 'processing',
                'stage': 'progress_update',
                'message': self.custom_message or 'Processing in progress',
                **progress_data
            })

        else:
            # Generic notification
            base_message.update({
                'status': 'processing',
                'message': self.custom_message or f'Notification: {self.message_type}'
            })

        return base_message

    def _extract_progress_data(self, context: Context) -> Dict[str, Any]:
        """Extract progress data from context"""

        progress_data = {}

        try:
            # Try to get data from XCom
            task_instance = context.get('task_instance')
            if task_instance:
                # Look for progress data from previous tasks
                xcom_data = task_instance.xcom_pull(task_ids=None, key='progress_data')
                if xcom_data:
                    progress_data.update(xcom_data)

        except Exception as e:
            logger.debug(f"Could not extract progress data: {str(e)}")

        return progress_data

    def _send_to_backend(self, job_id: str, message: Dict[str, Any]) -> requests.Response:
        """Send notification to backend API"""

        backend_url = 'http://backend:8000'
        notification_url = f"{backend_url}/api/v1/notify/{job_id}"

        try:
            response = requests.post(
                notification_url,
                json=message,
                timeout=10,
                headers={'Content-Type': 'application/json'}
            )

            return response

        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP request failed: {str(e)}")
            raise


class SlackNotifyOperator(BaseOperator):
    """
    Operator for sending Slack notifications (optional)
    """

    def __init__(self, job_config: Dict[str, Any], webhook_url: str, message_type: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_config = job_config
        self.webhook_url = webhook_url
        self.message_type = message_type

    def execute(self, context: Context):
        """Send Slack notification"""

        if not self.webhook_url:
            logger.info("No Slack webhook URL provided, skipping notification")
            return

        message = self._create_slack_message(context)

        try:
            response = requests.post(
                self.webhook_url,
                json=message,
                timeout=10
            )

            if response.status_code == 200:
                logger.info("Slack notification sent successfully")
            else:
                logger.warning(f"Failed to send Slack notification: HTTP {response.status_code}")

            return response.status_code == 200

        except Exception as e:
            logger.error(f"Failed to send Slack notification: {str(e)}")
            return False

    def _create_slack_message(self, context: Context) -> Dict[str, Any]:
        """Create Slack message"""

        job_id = self.job_config['job_id']
        filename = self.job_config.get('filename', 'unknown')
        task_instance = context.get('task_instance')

        if self.message_type == 'job_complete':
            color = 'good'
            title = '✅ ETL Job Completed'
            message = f"Successfully processed file: {filename}"
        elif self.message_type == 'job_failed':
            color = 'danger'
            title = '❌ ETL Job Failed'
            message = f"Failed to process file: {filename}"
        else:
            color = 'warning'
            title = '📊 ETL Job Update'
            message = f"Job update for file: {filename}"

        slack_message = {
            "attachments": [
                {
                    "color": color,
                    "title": title,
                    "fields": [
                        {
                            "title": "Job ID",
                            "value": job_id,
                            "short": True
                        },
                        {
                            "title": "File",
                            "value": filename,
                            "short": True
                        },
                        {
                            "title": "Message",
                            "value": message,
                            "short": False
                        }
                    ],
                    "footer": "BigData Processing System",
                    "ts": int(datetime.now().timestamp())
                }
            ]
        }

        return slack_message


class EmailNotifyOperator(BaseOperator):
    """
    Operator for sending email notifications (optional)
    """

    def __init__(self, job_config: Dict[str, Any], email_to: str, subject: str, message_type: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_config = job_config
        self.email_to = email_to
        self.subject = subject
        self.message_type = message_type

    def execute(self, context: Context):
        """Send email notification"""

        if not self.email_to:
            logger.info("No email address provided, skipping notification")
            return

        try:
            from airflow.providers.smtp.operators.smtp import EmailOperator

            email_content = self._create_email_content(context)

            email_op = EmailOperator(
                task_id='send_email',
                to=self.email_to,
                subject=self.subject,
                html_content=email_content,
                dag=context['dag']
            )

            email_op.execute(context)
            logger.info(f"Email notification sent to {self.email_to}")

        except Exception as e:
            logger.error(f"Failed to send email notification: {str(e)}")

    def _create_email_content(self, context: Context) -> str:
        """Create HTML email content"""

        job_id = self.job_config['job_id']
        filename = self.job_config.get('filename', 'unknown')
        file_format = self.job_config.get('file_format', 'unknown')
        destination = self.job_config.get('destination_type', 'unknown')

        html_content = f"""
        <html>
        <body>
            <h2>BigData Processing System - Job Notification</h2>

            <table border="1" cellpadding="5" cellspacing="0">
                <tr><th>Property</th><th>Value</th></tr>
                <tr><td>Job ID</td><td>{job_id}</td></tr>
                <tr><td>File</td><td>{filename}</td></tr>
                <tr><td>Format</td><td>{file_format}</td></tr>
                <tr><td>Destination</td><td>{destination}</td></tr>
                <tr><td>Status</td><td>{self.message_type}</td></tr>
                <tr><td>Timestamp</td><td>{datetime.now().isoformat()}</td></tr>
            </table>

            <p>This is an automated notification from the BigData Processing System.</p>
        </body>
        </html>
        """

        return html_content