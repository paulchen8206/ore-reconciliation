import json


class SnsFactory:

    @staticmethod
    def publish_message(topic, subject, email_message):
        response = topic.publish(
            Message=json.dumps(email_message), Subject=subject
        )
        HTTPStatusCode = response["ResponseMetadata"]["HTTPStatusCode"]

        return HTTPStatusCode
