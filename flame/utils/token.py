import os


def get_tokens() -> dict[str, str]:
    return {'RESULT_SERVICE_TOKEN': os.getenv("RESULT_SERVICE_TOKEN"),
            'DATA_SOURCE_TOKEN': os.getenv("DATA_SOURCE_TOKEN"),
            'MESSAGE_BROKER_TOKEN': os.getenv("MESSAGE_BROKER_TOKEN")}
