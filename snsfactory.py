import json


class SnsFactory:

    @staticmethod
    def publish_message(topic, subject, email_message):
        message = {
            'default': "default_message",
            "email": email_message,
        }
        response = topic.publish(
            Message=json.dumps(message), Subject=subject, MessageStructure="json"
        )
        HTTPStatusCode = response["ResponseMetadata"]["HTTPStatusCode"]

        return HTTPStatusCode
