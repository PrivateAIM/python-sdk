from minio import Minio


class MinIOClient:
    def __init__(self, endpoint, access_key, secret_key):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.client = self.get_client()

    def get_client(self):
        return Minio(self.endpoint, access_key=self.access_key, secret_key=self.secret_key)
