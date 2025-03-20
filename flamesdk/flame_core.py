import asyncio
import time
from datetime import datetime

from typing import Any, Literal, Optional, Union
from threading import Thread
from httpx import AsyncClient

from flamesdk.resources.client_apis.data_api import DataAPI
from flamesdk.resources.client_apis.message_broker_api import MessageBrokerAPI, Message
from flamesdk.resources.client_apis.storage_api import StorageAPI
from flamesdk.resources.node_config import NodeConfig
from flamesdk.resources.rest_api import FlameAPI
from flamesdk.resources.utils import wait_until_nginx_online


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

    def get_node_status(self,
                        timeout: Optional[int] = None) -> dict[str, Literal["online", "offline", "not_connected"]]:
        """
        Returns the status of all nodes.
        :param timeout: time in seconds to wait for the response, if None waits indefinitely
        :return:
        """
        #TODO: Remove this? (-> ready check)

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
        Returns the role of the node. "aggregator" means that the node can submit final results using
        "submit_final_result", else "default" (this may change with further permission settings).
        :return: the role of the node
        """
        return self.config.node_role

    def analysis_finished(self) -> bool:
        """
        Sends a signal to all nodes to set their node_finished to True, then sets the node to finished
        :return:
        """
        if self.get_participant_ids():
            self.send_message(self.get_participant_ids(),
                              "analysis_finished",
                              {},
                              max_attempts=5,
                              attempt_timeout=30)

        return self._node_finished()

    def ready_check(self,
                    nodes: list[str] = 'all',
                    attempt_interval: int = 30,
                    timeout: Optional[int] = None) -> dict[str, bool]:
        """
        Waits until specified partner nodes in a federated system are ready.

        This function sends repeated "ready check" messages to the provided nodes via the `FlameCoreSDK`
        and waits for acknowledgments indicating readiness. The function continues to retry at the
        specified interval until all nodes respond or the timeout is reached.

        Parameters:
            flame (FlameCoreSDK): The SDK instance used to communicate with the nodes.
            nodes (list[str]): A list of node identifiers to check for readiness.
            attempt_interval (int, optional): The interval (in seconds) between successive attempts.
                                              Defaults to 30 seconds.
            timeout (int, optional): The maximum time (in seconds) to wait for all nodes to respond.
                                     If `None`, the function waits indefinitely.

        Returns:
            dict[str, bool]: A dictionary mapping each node identifier to a boolean indicating whether
                             the node responded before the timeout.

        Raises:
            ValueError: If the `nodes` list is empty.
            FlameCoreSDKError: If there are issues communicating with the nodes.

        Example:
            ```python
            flame = FlameCoreSDK()
            nodes = get_participant_ids() # ["node1", "node2", "node3"]
            result = wait_until_partners_ready(flame, nodes, attempt_interval=15, timeout=120)
            print(result)  # {"node1": True, "node2": True, "node3": False}
            ```
        """
        if nodes == 'all':
            nodes = self.get_participant_ids()

        received = {node: False for node in nodes}

        start_time = datetime.now()

        time_passed = (datetime.now() - start_time).seconds
        while (not all(received.values())) and ((timeout is None) or (time_passed < timeout)):
            acknowledged_list, _ = self.send_message(receivers=nodes,
                                                     message_category='ready_check',
                                                     message={},
                                                     timeout=attempt_interval)
            for node in acknowledged_list:
                received[node] = True
                nodes.remove(node)

            time.sleep(1)
            time_passed = (datetime.now() - start_time).seconds

        return received

    ########################################Message Broker Client####################################
    def send_message(self,
                     receivers: list[str],
                     message_category: str,
                     message: dict,
                     max_attempts: int = 1,
                     timeout: Optional[int] = None,
                     attempt_timeout: int = 10) -> tuple[list[str], list[str]]:
        """
        Send a message to the specified nodes
        :param receivers: list of node ids to send the message to
        :param message_category: a string that specifies the message category
        :param message: the message to send
        :param max_attempts: the maximum number of attempts to send the message
        :param timeout: time in seconds to wait for the message acknowledgement, if None waits indefinitely
        :param attempt_timeout: timeout of each attempt, if timeout is None (the last attempt will be indefinite though)
        :return: a tuple of nodes ids that acknowledged and not acknowledged the message
        """
        return asyncio.run(self._message_broker_api.send_message(receivers,
                                                                 message_category,
                                                                 message,
                                                                 max_attempts,
                                                                 timeout,
                                                                 attempt_timeout))

    def await_messages(self,
                       senders: list[str],
                       message_category: str,
                       message_id: Optional[str] = None,
                       timeout: Optional[int] = None) -> dict[str, Optional[list[Message]]]:
        """
        Wait for responses from the specified nodes
        :param senders: list of node ids to wait for
        :param message_category: the message category to wait for
        :param message_id: optional message id to wait for
        :param timeout: time in seconds to wait for the message, if None waits indefinitely
        :return:
        """
        return asyncio.run(self._message_broker_api.await_messages(senders, message_category, message_id, timeout))

    def get_messages(self, status: Literal['unread', 'read'] = 'unread') -> list[Message]:
        """
        Get all messages that have been sent to the node
        :param status: the status of the messages to get
        :return:
        """
        return self._message_broker_api.get_messages(status)

    def delete_messages(self, message_ids: list[str]) -> int:
        """
        Delete messages from the node
        :param message_ids: list of message ids to delete
        :return: the number of messages deleted
        """
        return self._message_broker_api.delete_messages_by_id(message_ids)

    def clear_messages(self, status: Literal["read", "unread", "all"] = "read",
                       min_age: Optional[int] = None) -> int:
        """
        Deletes all messages by status (default: read messages) and if they are older than the specified min_age. It
        returns the number of deleted messages.
        :param status: the status of the messages to clear
        :param min_age: is set, only the messages with the specified status that are older than the limit in seconds
        are deleted
        :return: the number of messages cleared
        """
        return self._message_broker_api.clear_messages(status, min_age)

    def send_message_and_wait_for_responses(self,
                                            receivers: list[str],
                                            message_category: str,
                                            message: dict,
                                            max_attempts: int = 1,
                                            timeout: Optional[int] = None,
                                            attempt_timeout: int = 10) -> dict[str, Optional[list[Message]]]:
        """
        Sends a message to all specified nodes and waits for responses, (combines send_message and await_responses)
        :param receivers: list of node ids to send the message to
        :param message_category: a string that specifies the message category,
        :param message: the message to send
        :param max_attempts: the maximum number of attempts to send the message
        :param timeout: time in seconds to wait for the message acknowledgement, if None waits indefinitely
        :param attempt_timeout: timeout of each attempt, if timeout is None (the last attempt will be indefinite though)
        :return: the responses
        """
        return self._message_broker_api.send_message_and_wait_for_responses(receivers,
                                                                            message_category,
                                                                            message,
                                                                            max_attempts,
                                                                            timeout,
                                                                            attempt_timeout)

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

    def save_intermediate_data(self,
                               data: Any,
                               location: Literal["local", "global"],
                               remote_node_ids: Optional[list[str]] = None,
                               tag: Optional[str] = None) -> Union[dict[str, dict[str, str]], dict[str, str]]:
        """
        saves intermediate results/data either on the hub (location="global"), or locally
        :param data: the result to save
        :param location: the location to save the result, local saves in the node, global saves in central instance of MinIO
        :param remote_node_ids: optional remote node ids (used for accessing remote node's public key for encryption)
        :param tag: optional storage tag
        :return: the request status code{"status": ,"url":, "id": }, or dict of said dicts if encrypted mode is used, i.e. remote_node_ids are set
        """
        return self._storage_api.save_intermediate_data(data, location=location, remote_node_ids=remote_node_ids, tag=tag)

    def get_intermediate_data(self,
                              location: Literal["local", "global"],
                              id: Optional[str] = None,
                              tag: Optional[str] = None,
                              tag_option: Optional[Literal["all", "last","first"]]= "all") -> Any:
        """
        returns the intermediate data with the specified id
        :param location: the location to get the result, local gets in the node, global gets in central instance of MinIO
        :param id: the id of the result to get
        :param tag: optional storage tag of targeted local result
        :param tag_option: return mode if multiple tagged data are found
        :return: the result
        """
        return self._storage_api.get_intermediate_data(location, id, tag, tag_option=tag_option)

    def send_intermediate_data(self,
                               receivers: list[str],
                               data: Any,
                               message_category: str = "intermediate_data",
                               max_attempts: int = 1,
                               timeout: Optional[int] = None,
                               attempt_timeout: int = 10,
                               encrypted: bool = True) -> tuple[list[str], list[str]]:
        """
        Sends intermediate data to specified receivers using the Result Service and Message Broker.

        This function saves the provided data using the Result Service and then sends a message to
        the specified receivers via the Message Broker, notifying them of the availability of the
        intermediate data. The message includes the `result_id` of the saved data.

        Parameters:
            receivers (list[str]): A list of node identifiers to which the intermediate data will be sent.
            data (Any): The intermediate data to be sent. This can be of any serializable type.
            message_category (str, optional): A string specifying the category of the message.
                                              Defaults to "intermediate_data".
            max_attempts (int): the maximum number of attempts to send the message
            timeout (int, optional): time in seconds to wait for the message acknowledgement, if None waits indefinitely
            attempt_timeout (int): timeout of each attempt, if timeout is None (the last attempt will be indefinite though)
            encrypted (bool): bool whether data should be encrypted or not

        Returns:
            tuple[list[str], list[str]]:
                - A list of node IDs that successfully received the message.
                - A list of node IDs that failed to receive the message.

       Example:
            ```python
            receivers = ["node1", "node2", "node3"]
            data = {"key": "value"}

            successful, failed = send_intermediate_data(receivers, data)

            print("Successful nodes:", successful)  # e.g., ["node1", "node2"]
            print("Failed nodes:", failed)  # e.g., ["node3"]
            ```
        """
        if encrypted:
            result_id_body = {k: v['id']
                              for k, v in self.save_intermediate_data(data,
                                                                      "global",
                                                                      remote_node_ids=receivers).items()}
        else:
            result_id_body = self.save_intermediate_data(data, "global")['id']
        return self.send_message(receivers,
                                 message_category,
                                 {"result_id": result_id_body},
                                 max_attempts,
                                 timeout,
                                 attempt_timeout)

    def await_intermediate_data(self,
                                senders: list[str],
                                message_category: str = "intermediate_data",
                                timeout: Optional[int] = None) -> dict[str, Any]:
        """
           Waits for messages containing intermediate data ids from specified senders and retrieves the data.

           This function listens for messages of a specified category from the provided senders. Once a
           message is received, it retrieves the intermediate data associated with the `result_id`
           included in the message. The function returns a dictionary mapping each sender to their
           corresponding data or `None` if no data was received within the timeout period.

           Parameters:
               senders (list[str]): A list of sender node identifiers to listen for.
               message_category (str, optional): The category of the message to wait for.
                                                 Defaults to "intermediate_data".
               timeout (int, optional): The maximum time (in seconds) to wait for messages.
                                        If `None`, waits indefinitely.

           Returns:
               dict[str, Any]: A dictionary where the keys are sender node IDs and the values are the
                               intermediate data retrieved for each sender. If no data is received from
                               a sender within the timeout period, its value will be `None`.


           Example:
               ```python
               senders = ["node1", "node2"]

               # Wait for intermediate data from the senders
               data = await_intermediate_data(senders, timeout=60)

               # Output the retrieved data
               print(data)
               # Example: {"node1": {"key": "value"}, "node2": None}
               ```
           """
        data_dict = {sender: None for sender in senders}
        message_dict = self.await_messages(senders, message_category, timeout=timeout)
        for sender, message_list in message_dict.items():
            if message_list:
                result_id_body = message_list[-1].body['result_id']
                result_sent_is_encrypted = type(result_id_body) == dict
                result_id_sent = result_id_body[self.config.node_id] if result_sent_is_encrypted else result_id_body
                data_dict[sender] = self.get_intermediate_data("global", result_id_sent)
        return data_dict

    def get_local_tags(self, filter: Optional[str] = None) -> list[str]:
        """
        returns the list of tags used to save local results
        :param filter: filter tags by a substring
        :return: the list of tags
        """
        return self._storage_api.get_local_tags(filter)

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

    def get_fhir_data(self, fhir_queries: Optional[list[str]] = None) -> list[Union[dict[str, dict], dict]]:
        """
        Returns the data from the FHIR store for each of the specified queries.
        :param fhir_queries: list of queries to get the data
        :return:
        """
        return self._data_api.get_fhir_data(fhir_queries)

    def get_s3_data(self, s3_keys: Optional[list[str]] = None) -> list[Union[dict[str, str], str]]:
        """
        Returns the data from the S3 store associated with the given key.
        :param s3_keys:f
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
                                  self._data_api.data_client,
                                  self._storage_api.result_client,
                                  self.config.keycloak_token,
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
