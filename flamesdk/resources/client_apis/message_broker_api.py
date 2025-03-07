import asyncio
from datetime import datetime
from typing import Literal, Optional

from flamesdk.resources.node_config import NodeConfig
from flamesdk.resources.client_apis.clients.message_broker_client import MessageBrokerClient, Message


class MessageBrokerAPI:
    def __init__(self, config: NodeConfig):
        self.message_broker_client = MessageBrokerClient(config)
        self.config = self.message_broker_client.nodeConfig
        self.participants = asyncio.run(self.message_broker_client.get_partner_nodes(self.config.node_id,
                                                                                     self.config.analysis_id))

    async def send_message(self,
                           receivers: list[str],
                           message_category: str,
                           message: dict,
                           max_attempts: int = 1,
                           timeout: Optional[int] = None,
                           attempt_timeout: int = 10) -> tuple[list[str], list[str]]:
        """
        Sends a message to specified nodes with support for multiple attempts and timeout handling.

        This asynchronous method dispatches a message to a list of receiver node IDs. It attempts to send the message
        up to `max_attempts` times if acknowledgments are not received within the specified `timeout`. The method
        returns two lists: one containing the IDs of nodes that successfully acknowledged the message and another
        with the IDs of nodes that did not acknowledge.

        :param receivers: list of node ids to send the message to
        :param message_category: a string that specifies the message category,
        :param message: the message to send
        :param max_attempts: the maximum number of attempts to send the message
        :param timeout: time in seconds to wait for the message acknowledgement, if None waits indefinitely
        :param attempt_timeout: timeout of each attempt, if timeout is None (the last attempt will be indefinite though)
        :return: a tuple of nodes ids that acknowledged and not acknowledged the message
        """
        # Create a message object
        message = Message(recipients=receivers,
                          message=message,
                          category=message_category,
                          config=self.config,
                          message_number=self.message_broker_client.message_number,
                          outgoing=True)
        start_time = datetime.now()
        acknowledged = []
        not_acknowledged = receivers

        for attempt in range(max_attempts):
            if timeout is None:
                attempt_timeout = attempt_timeout if attempt < (max_attempts - 1) else None
            else:
                attempt_timeout = timeout / max_attempts

            message.recipients = not_acknowledged

            # Send the message
            await self.message_broker_client.send_message(message)

            # await the message acknowledgement
            await_list = []

            # Create a list of tasks to await the message acknowledgement
            for receiver in not_acknowledged:
                await_list.append(
                    asyncio.create_task(
                        self.message_broker_client.await_message_acknowledgement(message, receiver)
                    )
                )

            # Run the tasks and wait for the message acknowledgement until the timeout or all messages are acknowledged
            done, pending = await asyncio.wait(await_list, timeout=attempt_timeout, return_when=asyncio.ALL_COMPLETED)

            # Check if the message was acknowledged
            for task in done:
                if task.result():
                    acknowledged.append(task.result())

            # If the message was not acknowledged raise an error
            # not_acknowledged = receivers - acknowledged
            not_acknowledged = [receiver for receiver in receivers if receiver not in acknowledged]

            time_passed = (datetime.now() - start_time).seconds
            if (len(acknowledged) == len(receivers)) or ((timeout is not None) and (time_passed > timeout)):
                break

        return acknowledged, not_acknowledged

    async def await_messages(self,
                             node_ids: list[str],
                             message_category: str,
                             message_id: Optional[str] = None,
                             timeout: Optional[int] = None) -> dict[str, Optional[list[Message]]]:
        """
        Wait for responses from the specified nodes
        :param node_ids: list of node ids to wait for
        :param message_category: the message category to wait for
        :param message_id: optional message id to wait for
        :param timeout: time in seconds to wait for the message, if None waits indefinitely
        :return:
        """
        await_list = []
        for node_id in node_ids:
            await_list.append(
                asyncio.create_task(
                    self.message_broker_client.await_message(node_id, message_category, message_id)
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

    def get_messages(self, status: Literal['unread', 'read'] = 'unread') -> list[Message]:
        """
        Get all messages that have been sent to the node and have the specified un-/read status
        :return:
        """
        return [msg for msg in self.message_broker_client.list_of_incoming_messages
                if msg.body["meta"]["status"] == status]

    def delete_messages_by_id(self, message_ids: list[str]) -> int:
        """
        Delete messages from the node
        :param message_ids: list of message ids to delete
        :return: the number of messages deleted
        """
        number_of_deleted_messages = 0
        for message_id in message_ids:
            number_of_deleted_messages += self.message_broker_client.delete_message_by_id(message_id, type="incoming")
            number_of_deleted_messages += self.message_broker_client.delete_message_by_id(message_id, type="outgoing")
        return number_of_deleted_messages

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
        number_of_deleted_messages = 0
        number_of_deleted_messages += self.message_broker_client.clear_messages("incoming", status, min_age)
        number_of_deleted_messages += self.message_broker_client.clear_messages("outgoing", status, min_age)
        return number_of_deleted_messages

    def send_message_and_wait_for_responses(self, receivers: list[str],
                                            message_category: str,
                                            message: dict,
                                            max_attempts: int = 1,
                                            timeout: Optional[int] = None,
                                            attempt_timeout: int = 10) -> dict[str, Optional[list[Message]]]:
        """
        Sends a message to all specified nodes and waits for responses, (combines send_message and await_responses)
        :param receivers:  list of node ids to send the message to
        :param message_category: a string that specifies the message category,
        :param message:  the message to send
        :param max_attempts: the maximum number of attempts to send the message
        :param timeout: time in seconds to wait for the message acknowledgement, if None waits indefinitely
        :param attempt_timeout: timeout of each attempt, if timeout is None (the last attempt will be indefinite though)
        :return: the responses
        """
        time_start = datetime.now()
        # Send the message
        asyncio.run(self.send_message(receivers, message_category, message, max_attempts, timeout, attempt_timeout))
        timeout = timeout - (datetime.now() - time_start).seconds
        if timeout < 0:
            timeout = 1

        # Wait for the responses
        responses = asyncio.run(self.await_messages(receivers, message_category, timeout=timeout))
        return responses

