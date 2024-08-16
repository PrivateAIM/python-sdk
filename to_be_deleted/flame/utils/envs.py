import os


def get_envs() -> dict[str, str]:
    return {'KEYCLOAK_TOKEN': os.getenv('KEYCLOAK_TOKEN'),
            'DATA_SOURCE_TOKEN': os.getenv('DATA_SOURCE_TOKEN'),
            'NGINX_NAME': f'nginx-{os.getenv("DEPLOYMENT_NAME")}'}

