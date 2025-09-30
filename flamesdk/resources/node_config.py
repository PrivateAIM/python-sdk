import os


class NodeConfig:
    def __init__(self) -> None:
        # init analysis status
        self.finished = False

        # environment variables
        self.analysis_id = os.getenv('ANALYSIS_ID')
        self.project_id = os.getenv('PROJECT_ID')
        self.keycloak_token = os.getenv('KEYCLOAK_TOKEN')
        self.data_source_token = os.getenv('DATA_SOURCE_TOKEN')
        self.nginx_name = f'nginx-{os.getenv("DEPLOYMENT_NAME")}'

        # tbd by MessageBroker
        self.node_role = None
        self.node_id = None

    def set_role(self, role) -> None:
        self.node_role = role

    def set_node_id(self, node_id) -> None:
        self.node_id = node_id

    def finish_analysis(self) -> None:
        self.finished = True
