"""Per-node configuration read from the container environment."""

import os


class NodeConfig:
    """Identity and credentials of the node this analysis container runs on.

    Everything except the node's role and id comes from environment variables
    injected by the platform. ``node_role``, ``node_type`` and ``node_id`` stay
    ``None`` until the MessageBroker handshake fills them in via
    :meth:`set_role` and :meth:`set_node_id`.
    """

    def __init__(self) -> None:
        """Read the analysis environment variables into the config."""
        # init analysis status
        self.finished = False

        # environment variables
        self.analysis_id = os.getenv("ANALYSIS_ID")
        self.project_id = os.getenv("PROJECT_ID")
        self.keycloak_token = os.getenv("KEYCLOAK_TOKEN")
        self.data_source_token = os.getenv("DATA_SOURCE_TOKEN")
        self.nginx_name = f'nginx-{os.getenv("DEPLOYMENT_NAME")}'

        # tbd by MessageBroker
        self.node_role = None
        self.node_type = None
        self.node_id = None

    def set_role(self, role) -> None:
        """Record the role the MessageBroker assigned to this node.

        The first role seen also becomes the node's ``node_type``, which -
        unlike ``node_role`` - is not changed again afterwards.

        :param role: the node role, e.g. ``'aggregator'`` or ``'default'``
        """
        self.node_role = role
        if self.node_type is None:
            self.node_type = role

    def set_node_id(self, node_id) -> None:
        """Record the node id the MessageBroker assigned to this node.

        :param node_id: the id identifying this node within the analysis
        """
        self.node_id = node_id

    def finish_analysis(self) -> None:
        """Mark the analysis as finished, so the node stops accepting work."""
        self.finished = True
