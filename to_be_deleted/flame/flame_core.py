from httpx import AsyncClient

from resources.clients.message_broker_client import MessageBrokerClient
from flame.federated.node_base_client import NodeConfig
from resources.clients.result_client import ResultClient

from resources.rest_api import FlameAPI

from typing import List, Literal, IO

from threading import Thread
from flame.utils.envs import get_envs
from flame.utils.nginx import wait_until_nginx_online
class FlameCoreSDK:

    def __init__(self):
        print("Starting Flame core SDK")
        # Setup the connection to all the services needed

        print("Getting environment variables")
        # get environment variables
        envs = get_envs()
        self.finished = False

        # wait until nginx is online
        wait_until_nginx_online(envs)

        print("Connecting to message broker")
        # connect to message broker
        self._message_broker = MessageBrokerClient(envs)

        print("Extracting node config")
        # extract node config
        self._node_config = NodeConfig(self.message_broker)

        print("Connecting to result service")
        # connection to result service
        self._result_service_client = ResultClient(envs)

        print("Starting flame api thread")
        # start the flame api thread , this is uesed for incoming messages from the message broker and health checks
        self._flame_api_thread = Thread(target=self._start_flame_api,
                                       args=('analyzer', self.message_broker))
        self._flame_api_thread.start()

        print("Flame core SDK started")
########################################Internal###############################################
    def _start_flame_api(self, node_mode: str, message_broker: MessageBrokerClient) -> None:
        """
        Start the flame api, this is used for incoming messages from the message broker and health checks
        :param node_mode:
        :param message_broker:
        :return:
        """
        self.flame_api = FlameAPI(node_mode, message_broker, self._converged)

    def _converged(self) -> bool:
        """
        Check if the node has finished processing used by the flame api to check if the node has finished processing
        :return:
        """
        return self.finished

########################################Message Broker Client####################################
    def send_message(self, receivers: List[str], message_category: str, message: dict, timeout: int = None) -> str:
        """
        Sends a message to all specified nodes.
        :param receivers:  list of node ids to send the message to
        :param message_category: a string that specifies the message category,
        :param message:  the message to send
        :param timeout: time in seconds to wait for the message acknowledgement, if None waits indefinetly
        :return: the message id
        """
        pass
        #TODO Implement this

        # Send the message
        # Wait for the response

    def await_responses(self, node_ids: List[str], message_id: str, message_category: str, timeout: int = None) -> List[dict]:
        """
        Wait for responses from the specified nodes
        :param node_ids: list of node ids to wait for
        :param message_id: the message id to wait for
        :param message_category: the message category to wait for
        :param timeout: time in seconds to wait for the message, if None waits indefinetly
        :return:
        """
        pass
        #TODO Implement this
        while True:
            pass
            # Check if the message has been received6
            # If received return the message
            # If not received wait for the message


    def get_messages(self, status: Literal["read", "unread", "all"] = "unread") -> List[dict]:
        """
        Get all messages that have been sent to the node
        :param status: the status of the messages to get
        :return:
        """
        pass
        #TODO Implement this

    def delete_messages(self, message_ids: List[str]) -> int:
        """
        Delete messages from the node
        :param message_ids: list of message ids to delete
        :return: the number of messages deleted
        """
        pass
        #TODO Implement this

    def clear_messages(self, status: Literal["read", "unread", "all"] = "read", time_limit: int = None) -> int:
        """
        Deletes all messages by status (default: read messages) and if they are older than the specified time_limit. It returns the number of deleted messages.
        :param status: the status of the messages to clear
        :param time_limit: is set, only the messages with the specified status that are older than the limit in seconds are deleted
        :return: the number of messages cleared
        """
        pass
        #TODO Implement this

    def send_message_and_wait_for_responses(self, receivers: List[str], message_category: str, message: dict, timeout: int = None) -> dict:
        """
        Sends a message to all specified nodes and waits for responses,( combines send_message and await_responses)
        :param receivers:  list of node ids to send the message to
        :param message_category: a string that specifies the message category,
        :param message:  the message to send
        :param timeout: time in seconds to wait for the message acknowledgement, if None waits indefinetly
        :return: the responses
        """
        pass
        #TODO Implement this

########################################Storage Client###########################################
    def submit_final_result(self, result: IO) -> str:
        """
        sends the final result to the hub. Making it available for analysts to download.
        This method is only available for nodes for which the method `get_role(self)` returns "aggregator”.
        :param result: the final result
        :return: the request status code
        """
        pass
        #TODO Implement this

    def save_intermediate_data(self,location: Literal["local","global"] ,data: IO) -> str:
        """
        saves intermediate results/data either on the hub (location="global"), or locally
        :param location: the location to save the result, local saves in the node, global saves in central instance of MinIO
        :param data: the result to save
        :return: the request status code
        """
        pass
        #TODO Implement this

    def list_intermediate_data(self, location: Literal["local","global"]) -> List[str]:
        """
        returns a list of all locally/globally saved intermediate data available
        :param location: the location to list the result, local lists in the node, global lists in central instance of MinIO
        :return: the list of results
        """
        pass
        #TODO Implement this

    def get_intermediate_data(self, location: Literal["local","global"], id: str) -> IO:
        """
        returns the intermediate data with the specified id
        :param location: the location to get the result, local gets in the node, global gets in central instance of MinIO
        :param id: the id of the result to get
        :return: the result
        """
        pass
        #TODO Implement this


########################################Data Source Client#######################################
    def get_data_client(self, data_id: str) -> AsyncClient:
        """
        Returns the data client for a specific fhir or S3 store used for this project.
        :param data_id: the id of the data source
        :return: the data client
        """
        pass
        #TODO Implement this
    def get_data_sources(self) -> List[str]:
        """
        Returns a list of all data sources available for this project.
        :return: the list of data sources
        """
        pass
        #TODO Implement this

    def get_fhir_data(self, data_id: str, queries: List[str]) -> List[dict]:
        """
        Returns the data from the FHIR store for each of the specified queries.
        :param data_id: the id of the data source
        :param query: the query to get the data
        :return: the data
        """
        pass
        #TODO Implement this
    def get_s3_data(self, key: str, local_path: str) -> IO:
        """
        Returns the data from the S3 store associated with the given key.
        :param key:
        :param local_path:
        :return:
        """

########################################General##################################################
    def get_participants(self) -> List[str]:
        """
        Returns a list of all participants in the analysis
        :return: the list of participants
        """
        pass
        #TODO Implement this

    def get_node_status(self,timeout: int = None) -> dict[str, Literal["online", "offline", "not_connected"]]:
        """
        Returns the status of all nodes.
        :param timeout:  time in seconds to wait for the response, if None waits indefinetly
        :return:
        """
        pass
        #TODO Implement this

    def get_analysis_id(self) -> str:
        """
        Returns the analysis id
        :return: the analysis id
        """
        pass
        #TODO Implement this

    def get_project_id(self) -> str:
        """
        Returns the project id
        :return: the project id
        """
        pass
        #TODO Implement this


    def get_id(self) -> str:
        """
        Returns the node id
        :return: the node id
        """
        pass
        #TODO Implement this

    def get_role(self) -> Literal["aggregator", "analyzer"]:
        """
        get the role of the node. "aggregator" means that the node can submit final results using "submit_final_result", else "default".
        (this my change with futer permission settings for more function,)
        :return: the role of the node
        """
        pass
        #TODO Implement this
    def send_intermediate_result(self, receivers: List[str], result: IO) -> str:
        """
        SSends an intermediate result using Result Service and Message Broker.
        :param receivers: list of node ids to send the result to
        :param result: the result to send
        :return: the request status code
        """
        pass
        #TODO Implement this

    def node_finished(self) -> bool:
        """
        Set the node to finished, this will stop signal to stop the container from running.
        Need to be called when all processing is done
        :return:
        """
        self.finished = True
        return self.finished

    def analysis_finished(self) -> bool:
        """
        Sends a signal to all nodes to set their node_finished to True, then calles node_finished
        :return:
        """
        # TODO: Implement this
        pass
