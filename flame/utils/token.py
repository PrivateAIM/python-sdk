import os


def get_tokens() -> dict[str, str]:
    return {'KEYCLOAK_TOKEN': os.getenv('KEYCLOAK_TOKEN'),
            'DATA_SOURCE_TOKEN': os.getenv('DATA_SOURCE_TOKEN')}
