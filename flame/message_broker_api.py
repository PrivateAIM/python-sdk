import asyncio
from typing import Literal

from typing import List, Literal, IO

from resources.analysis_config import AnalysisConfig
from resources.clients.message_broker_client import MessageBrokerClient, Message, MessageWaiter


class MessageBrokerAPI:
    def __init__(self, config: AnalysisConfig):
        self._message_broker_client = MessageBrokerClient(config.nginx_name, config.keycloak_token)
        self._config = config
        self.message_counter = 0
        self._send_messages = []

    async def send_message(self, receivers: list[str], message_category: str, message: dict,
                           timeout: int = None) -> str:
        """
        Sends a message to all specified nodes.
        :param receivers:  list of node ids to send the message to
        :param message_category: a string that specifies the message category,
        :param message:  the message to send
        :param timeout: time in seconds to wait for the message acknowledgement, if None waits indefinetly
        :return: the message id
        """
        self.message_counter += 1
        # Create a message object
        message = Message(receivers, message, message_category, self._config, self.message_counter)
        # Send the message
        await self._message_broker_client.send_message(message)
        # Add the message to the list of sent messages
        self._send_messages.append(message)
        # await the message acknowledgement
        await_list = []
        for receiver in message.recipients:
            await_list.append(MessageWaiter(
                self._message_broker_client, message, receiver).await_message_acknowledgement())
        done, pending = await asyncio.wait(await_list, timeout=timeout, return_when=asyncio.ALL_COMPLETED)
        # Check if the message was acknowledged
        not_acknowledged = []
        for task in done:
            if not task.result():
                not_acknowledged.append(task)

    def await_responses(self, node_ids: list[str], message_id: str, message_category: str, timeout: int = None) \
            -> list[dict]:
        """
        Wait for responses from the specified nodes
        :param node_ids: list of node ids to wait for
        :param message_id: the message id to wait for
        :param message_category: the message category to wait for
        :param timeout: time in seconds to wait for the message, if None waits indefinetly
        :return:
        """
        pass
        # TODO Implement this
        while True:
            pass
            # Check if the message has been received6
            # If received return the message
            # If not received wait for the message

    def get_messages(self, status: Literal["read", "unread", "all"] = "unread") -> list[dict]:
        """
        Get all messages that have been sent to the node
        :param status: the status of the messages to get
        :return:
        """
        pass
        # TODO Implement this

    def delete_messages(self, message_ids: list[str]) -> int:
        """
        Delete messages from the node
        :param message_ids: list of message ids to delete
        :return: the number of messages deleted
        """
        pass
        # TODO Implement this

    def clear_messages(self, status: Literal["read", "unread", "all"] = "read", time_limit: int = None) -> int:
        """
        Deletes all messages by status (default: read messages) and if they are older than the specified time_limit. It
        returns the number of deleted messages.
        :param status: the status of the messages to clear
        :param time_limit: is set, only the messages with the specified status that are older than the limit in seconds are
        deleted
        :return: the number of messages cleared
        """
        pass
        # TODO Implement this

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
        pass
        # TODO Implement this
