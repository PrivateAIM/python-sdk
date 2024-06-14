
from flame.clients.message_broker_client import MessageBrokerClient
from flame.federated.node_base_client import Node, NodeConfig
from flame.clients.result_client import ResultClient

from flame.api import FlameAPI

from typing import List, Literal

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
    def submit_final_result(self, result):
        """
        Submit the final result to the result service
        :param result: the final result
        :return:
        """
        pass
        #TODO Implement this
########################################Data Source Client#######################################

########################################General##################################################

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
