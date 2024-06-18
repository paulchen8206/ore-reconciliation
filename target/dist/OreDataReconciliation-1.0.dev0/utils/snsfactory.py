import json
import logging
import time
import boto3
from botocore.exceptions import ClientError


class SnsFactory:

    @staticmethod
    def publish_message(topic, subject, default_message, sms_message, email_message):
        try:
            message = {
                "default": default_message,
                "sms": sms_message,
                "email": email_message,
            }
            response = topic.publish(
                Message=json.dumps(message), Subject=subject, MessageStructure="json"
            )
            message_id = response["MessageId"]

        except ClientError:
            raise
        else:
            return message_id
