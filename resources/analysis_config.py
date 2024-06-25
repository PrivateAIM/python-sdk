# TODO keep under resources or move to flame folder ?
import os


class AnalysisConfig:

    def __init__(self, ):
        # analysis id and project id
        self.analysis_id = None
        self.project_id = None
        # analysis status
        self.finished = False
        # environment variables
        self.keycloak_token = os.getenv('KEYCLOAK_TOKEN')
        self.data_source_token = os.getenv('DATA_SOURCE_TOKEN')
        self.nginx_name = f'service-nginx-{os.getenv("DEPLOYMENT_NAME")}'
        #
        self.role = None
        self.node_id = None

    def set_analysis_id(self, analysis_id):
        self.analysis_id = analysis_id

    def set_project_id(self, project_id):
        self.project_id = project_id

    def set_role(self, role):
        self.role = role

    def set_node_id(self, node_id):
        self.node_id = node_id

    def finish_analysis(self):
        self.finished = True
