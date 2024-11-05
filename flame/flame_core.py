import asyncio
from io import BytesIO

from typing import Any, Literal, Optional
from threading import Thread
from httpx import AsyncClient

from flame.resources.client_apis.data_api import DataAPI
from flame.resources.client_apis.message_broker_api import MessageBrokerAPI, Message
from flame.resources.client_apis.storage_api import StorageAPI
from flame.resources.node_config import NodeConfig
from flame.resources.rest_api import FlameAPI
from flame.resources.utils import wait_until_nginx_online


class FlameCoreSDK:

    def __init__(self):
        print("Starting FlameCoreSDK")

        print("\tExtracting node config")
        # Extract node config
        self.config = NodeConfig()

        # Wait until nginx is online
        wait_until_nginx_online(self.config.nginx_name)

        # Set up the connection to all the services needed
        ## Connect to message broker
        print("\tConnecting to MessageBroker...", end='')
        self._message_broker_api = MessageBrokerAPI(self.config)
        print("success")
        ### Update config with self_config from Messagebroker
        self.config = self._message_broker_api.config

        ## Connect to result service
        print("\tConnecting to ResultService...", end='')
        self._storage_api = StorageAPI(self.config)
        print("success")

        ## Connection to data service
        print("\tConnecting to DataApi...", end='')
        self._data_api = DataAPI(self.config)
        print("success")

        # Start the flame api thread used for incoming messages and health checks
        print("\tStarting FlameApi thread...", end='')
        self._flame_api_thread = Thread(target=self._start_flame_api)
        self._flame_api_thread.start()
        print("success")

        print("FlameCoreSDK ready")

    ########################################General##################################################
    def get_aggregator_id(self) -> Optional[str]:
        """
        Returns node_id of node dedicated as aggregator
        :return:
        """
        for participant in self.get_participants():
            if participant['nodeType'] == 'aggregator':
                return participant['nodeId']
        return None

    def get_participants(self) -> list[dict[str, str]]:
        """
        Returns a list of all participants in the analysis
        :return: the list of participants
        """
        return self._message_broker_api.participants

    def get_participant_ids(self) -> list[str]:
        """
        Returns a list of all participant ids in the analysis
        :return: the list of participants
        """
        return [participant['nodeId'] for participant in self._message_broker_api.participants]

    def get_node_status(self, timeout: int = None) -> dict[str, Literal["online", "offline", "not_connected"]]:
        """
        Returns the status of all nodes.
        :param timeout:  time in seconds to wait for the response, if None waits indefinitely
        :return:
        """

    def get_analysis_id(self) -> str:
        """
        Returns the analysis id
        :return: the analysis id
        """
        return self.config.analysis_id

    def get_project_id(self) -> str:
        """
        Returns the project id
        :return: the project id
        """
        return self.config.project_id

    def get_id(self) -> str:
        """
        Returns the node id
        :return: the node id
        """
        return self.config.node_id

    def get_role(self) -> str:
        """
        get the role of the node. "aggregator" means that the node can submit final results using "submit_final_result",
         else "default" (this may change with further permission settings).
        :return: the role of the node
        """
        return self.config.node_role

    def send_intermediate_result(self, receivers: list[str], result: BytesIO) -> str:  # TODO: tba (when messagebroker submissions are outdated)
        """
        Sends an intermediate result using Result Service and Message Broker.
        :param receivers: list of node ids to send the result to
        :param result: the result to send
        :return: the request status code
        """
        pass

    def analysis_finished(self) -> bool:
        """
        Sends a signal to all nodes to set their node_finished to True, then sets the node to finished
        :return:
        """
        self.send_message(self.get_participant_ids(), "analysis_finished", {}, timeout=None)

        return self._node_finished()

    ########################################Message Broker Client####################################
    def send_message(self, receivers: list[str], message_category: str, message: dict, timeout: int = None) -> \
            tuple[list[str], list[str]]:
        """
        Send a message to the specified nodes
        :param receivers: list of node ids to send the message to
        :param message_category: a string that specifies the message category,
        :param message: the message to send
        :param timeout: time in seconds to wait for the message acknowledgement, if None waits indefinitely
        :return: a tuple of nodes ids that acknowledged and not acknowledged the message
        """
        return asyncio.run(self._message_broker_api.send_message(receivers, message_category, message, timeout))

    def await_and_return_responses(self, node_ids: list[str], message_category: str, message_id: Optional[str] = None,
                                   timeout: int = None) -> dict[str, Optional[list[Message]]]:
        """
        Wait for responses from the specified nodes
        :param node_ids: list of node ids to wait for
        :param message_category: the message category to wait for
        :param message_id: optional message id to wait for
        :param timeout: time in seconds to wait for the message, if None waits indefinitely
        :return:
        """
        return asyncio.run(
            self._message_broker_api.await_and_return_responses(node_ids, message_category, message_id, timeout))

    def get_messages(self) -> list[Message]:
        """
        Get all messages that have been sent to the node
        :param status: the status of the messages to get
        :return:
        """
        return self._message_broker_api.get_messages()

    def delete_messages(self, message_ids: list[str]) -> int:
        """
        Delete messages from the node
        :param message_ids: list of message ids to delete
        :return: the number of messages deleted
        """
        return self._message_broker_api.delete_messages_by_id(message_ids)

    def clear_messages(self, status: Literal["read", "unread", "all"] = "read", time_limit: int = None) -> int:
        """
        Deletes all messages by status (default: read messages) and if they are older than the specified time_limit. It returns the number of deleted messages.
        :param status: the status of the messages to clear
        :param time_limit: is set, only the messages with the specified status that are older than the limit in seconds are deleted
        :return: the number of messages cleared
        """
        return self._message_broker_api.clear_messages(status, time_limit)

    def send_message_and_wait_for_responses(self, receivers: list[str], message_category: str, message: dict,
                                            timeout: int = None) -> dict:
        """
        Sends a message to all specified nodes and waits for responses, (combines send_message and await_responses)
        :param receivers:  list of node ids to send the message to
        :param message_category: a string that specifies the message category,
        :param message:  the message to send
        :param timeout: time in seconds to wait for the message acknowledgement, if None waits indefinitely
        :return: the responses
        """
        return self._message_broker_api.send_message_and_wait_for_responses(receivers,
                                                                            message_category,
                                                                            message,
                                                                            timeout)

    ########################################Storage Client###########################################
    def submit_final_result(self, result: Any, output_type: Literal['str', 'bytes', 'pickle'] = 'str') -> dict[str, str]:
        """
        sends the final result to the hub. Making it available for analysts to download.
        This method is only available for nodes for which the method `get_role(self)` returns "aggregator”.
        :param result: the final result
        :param output_type: output type of final results (default: string)
        :return: the request status code
        """
        return self._storage_api.submit_final_result(result, output_type)

    def save_intermediate_data(self, location: Literal["local", "global"], data: Any) -> dict[str, str]:
        """
        saves intermediate results/data either on the hub (location="global"), or locally
        :param location: the location to save the result, local saves in the node, global saves in central instance of MinIO
        :param data: the result to save
        :return: the request status code
        """
        return self._storage_api.save_intermediate_data(location, data)

    # def list_intermediate_data(self, location: Literal["local", "global"]) -> list[str]:
    #     """
    #     returns a list of all locally/globally saved intermediate data available
    #     :param location: the location to list the result, local lists in the node, global lists in central instance of MinIO
    #     :return: the list of results
    #     """
    #     return self._storage_api.list_intermediate_data(location)

    def get_intermediate_data(self, location: Literal["local", "global"], id: str) -> Any:
        """
        returns the intermediate data with the specified id
        :param location: the location to get the result, local gets in the node, global gets in central instance of MinIO
        :param id: the id of the result to get
        :return: the result
        """
        return self._storage_api.get_intermediate_data(location, id)

    ########################################Data Client#######################################
    def get_data_client(self, data_id: str) -> AsyncClient:
        """
        Returns the data client for a specific fhir or S3 store used for this project.
        :param data_id: the id of the data source
        :return: the data client
        """
        return self._data_api.get_data_client(data_id)

    def get_data_sources(self) -> list[str]:
        """
        Returns a list of all data sources available for this project.
        :return: the list of data sources
        """
        return self._data_api.get_data_sources()

    def get_fhir_data(self, fhir_queries: Optional[list[str]] = None) -> list[dict[str, dict]]:
        """
        Returns the data from the FHIR store for each of the specified queries.
        :param fhir_queries: list of queries to get the data
        :return:
        """
        return self._data_api.get_fhir_data(fhir_queries)

    def get_s3_data(self, s3_keys: Optional[list[str]] = None) -> list[dict[str, str]]:
        """
        Returns the data from the S3 store associated with the given key.
        :param s3_keys:
        :return:
        """
        return self._data_api.get_s3_data(s3_keys)

    ########################################Internal###############################################
    def _start_flame_api(self) -> None:
        """
        Start the flame api, this is used for incoming messages from the message broker and health checks
        :return:
        """
        self.flame_api = FlameAPI(self._message_broker_api.message_broker_client,
                                  finished_check=self._has_finished,
                                  finishing_call=self._node_finished)

    def _node_finished(self) -> bool:
        """
        Set the node to finished, this will send a signal to stop the container from running.
        Needs to be called when all processing is done.
        :return:
        """
        self.config.finish_analysis()
        return self.config.finished

    def _has_finished(self) -> bool:
        """
        Check if the node itself has finished processing (used by the flame api)
        :return:
        """
        return self.config.finished
