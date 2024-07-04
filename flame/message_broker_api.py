import asyncio
from datetime import datetime
from typing import Literal, Dict, Any

from typing import List, Literal, IO

from resources.node_config import NodeConfig
from resources.clients.message_broker_client import MessageBrokerClient, Message


class MessageBrokerAPI:
    def __init__(self, config: NodeConfig):
        self.message_broker_client = MessageBrokerClient(config.nginx_name, config.keycloak_token)
        self._config = config

    async def send_message(self, receivers: list[str], message_category: str, message: dict,
                           timeout: int = None) -> tuple[list[str], list[str]]:
        """
        Sends a message to all specified nodes.
        :param receivers:  list of node ids to send the message to
        :param message_category: a string that specifies the message category,
        :param message:  the message to send
        :param timeout: time in seconds to wait for the message acknowledgement, if None waits indefinetly
        :return: the message id
        """
        # Create a message object
        message = Message(recipients=receivers,
                          message=message,
                          category=message_category,
                          config=self._config,
                          message_number=self.message_broker_client.message_number,
                          outgoing=True)

        # Send the message
        await self.message_broker_client.send_message(message)

        # await the message acknowledgement
        await_list = []

        # Create a list of tasks to await the message acknowledgement
        for receiver in receivers:
            await_list.append(
                asyncio.create_task(
                    self.message_broker_client.await_message_acknowledgement(message, receiver)
                )
            )

        # Run the tasks and wait for the message acknowledgement until the timeout or all messages are acknowledged
        done, pending = await asyncio.wait(await_list, timeout=timeout, return_when=asyncio.ALL_COMPLETED)

        # Check if the message was acknowledged
        acknowledged = []
        for task in done:
            if not task.result():
                acknowledged.append(task.result())

        # If the message was not acknowledged raise an error
        # not_acknowledged = receivers - acknowledged
        not_acknowledged = [receiver for receiver in receivers if receiver not in acknowledged]

        return acknowledged, not_acknowledged

    async def await_and_return_responses(self, node_ids: list[str], message_category: str, timeout: int = None) \
            -> dict[str, list[Message] | None]:
        """
        Wait for responses from the specified nodes
        :param node_ids: list of node ids to wait for
        :param message_category: the message category to wait for
        :param timeout: time in seconds to wait for the message, if None waits indefinetly
        :return:
        """
        await_list = []
        for node_id in node_ids:
            await_list.append(
                asyncio.create_task(
                    self.message_broker_client.await_message(node_id, message_category)
                )
            )
        done, pending = await asyncio.wait(await_list, timeout=timeout, return_when=asyncio.ALL_COMPLETED)

        responses = dict()
        for node_id in node_ids:
            for task in done:
                id, response_list = task.result()
                if id == node_id:
                    responses[node_id] = response_list
                    for response in response_list:
                        response.set_read()
                    break
            if node_id not in responses.keys():
                responses[node_id] = None

        return responses

    def get_messages(self) -> list[Message]:
        """
        Get all messages that have been sent to the node
        :param status: the status of the messages to get
        :return:
        """
        return [msg for msg in self.message_broker_client.list_of_incoming_messages
                if msg.body["meta"]["status"] == "read"]

    def delete_messages_by_id(self, message_ids: list[str]) -> int:
        """
        Delete messages from the node
        :param message_ids: list of message ids to delete
        :return: the number of messages deleted
        """
        number_of_deleted_messages = 0
        for message_id in message_ids:
            number_of_deleted_messages += self.message_broker_client.delete_incoming_message(message_id)
            number_of_deleted_messages += self.message_broker_client.delete_outgoing_message(message_id)
        return number_of_deleted_messages

    def clear_messages(self, status: Literal["read", "unread", "all"] = "read", time_limit: int = None) -> int:
        """
        Deletes all messages by status (default: read messages) and if they are older than the specified time_limit. It
        returns the number of deleted messages.
        :param status: the status of the messages to clear
        :param time_limit: is set, only the messages with the specified status that are older than the limit in seconds are
        deleted
        :return: the number of messages cleared
        """
        number_of_deleted_messages = 0
        number_of_deleted_messages += self.message_broker_client.clear_messages(status, time_limit, type="incoming")
        number_of_deleted_messages += self.message_broker_client.clear_messages(status, time_limit, type="outgoing")
        return number_of_deleted_messages

    def send_message_and_wait_for_responses(self, receivers: list[str], message_category: str, message: dict,
                                            timeout: int = None) -> dict:
        """
        Sends a message to all specified nodes and waits for responses,( combines send_message and await_responses)
        :param receivers:  list of node ids to send the message to
        :param message_category: a string that specifies the message category,
        :param message:  the message to send
        :param timeout: time in seconds to wait for the message acknowledgement, if None waits indefinetly
        :return: the responses
        """
        # TODO
        time_start = datetime.now()
        asyncio.run(self.send_message(receivers, message_category, message, timeout))  # send the message
        timeout = timeout - (datetime.now() - time_start).seconds
        if timeout < 0:
            timeout = 1
        responses = asyncio.run(
            self.await_and_return_responses(receivers, message_category, timeout))  # wait for the responses
        return responses

