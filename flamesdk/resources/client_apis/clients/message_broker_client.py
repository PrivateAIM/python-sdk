import os
import uuid
import asyncio
import datetime
from typing import Optional, Literal
from httpx import AsyncClient, HTTPError

from flamesdk.resources.node_config import NodeConfig
from flamesdk.resources.utils import flame_log


class Message:
    def __init__(self,
                 message: dict,
                 config: NodeConfig,
                 outgoing: bool,
                 message_number: Optional[int] = None,
                 category: Optional[str] = None,
                 recipients: Optional[list[str]] = None) -> None:
        """
        Message object to be sent or received by the message broker.
        :param message: dict body of the message to be sent or received, must not contain the field 'meta'
        :param config: the node configuration
        :param outgoing: boolean value specifying if the message is outgoing or incoming
        :param message_number: the message number
        :param category: the message category
        :param recipients: the list of recipients
        """
        if outgoing:
            if "meta" in message.keys():
                raise ValueError("Cannot use field 'meta' in message body. "
                                 "This field is reserved for meta data used by the message broker.")
            elif type(message_number) != int:
                raise ValueError(f"Specified outgoing message, but did not specify integer value for message_number "
                                 f"(received: {type(message_number)}).")
            elif type(category) != str:
                raise ValueError("Specified outgoing message, but did not specify string value for category "
                                 f"(received: {type(category)}).")

            elif (type(recipients) != list) or (any([type(recipient) != str for recipient in recipients])):
                if hasattr(recipients, '__iter__'):
                    raise ValueError(f"Specified outgoing message, but did not specify list of strings value for "
                                     f"recipients (received: {type(recipients)} containing "
                                     f"{set([type(recipient) for recipient in recipients])}).")
                else:
                    raise ValueError(f"Specified outgoing message, but did not specify list of strings value for "
                                     f"recipients (received: {type(recipients)}).")
            self.recipients = recipients

        self.body = message
        self._update_meta_data(outgoing, config, category, message_number)

        if not outgoing:
            self.recipients = [self.body["meta"]["sender"]]

    def set_read(self) -> None:
        """
        Marks the message as read.
        :return:
        """
        self.body["meta"]["status"] = "read"

    def _update_meta_data(self,
                          outgoing: bool,
                          config: NodeConfig,
                          category: Optional[str] = None,
                          message_number: Optional[int] = None) -> None:
        """
        Adds meta data to the outgoing message or update it for incoming.
        :param outgoing:
        :param config:
        :param category:
        :param message_number:
        :return:
        """
        if outgoing:
            meta_data = {"type": "outgoing",
                         "category": category,
                         "id": f"{config.node_id}-{message_number}-{uuid.uuid4()}",
                         "akn_id": None,
                         "status": "unread",
                         "sender": config.node_id,
                         "created_at": str(datetime.datetime.now()),
                         "arrived_at": None,
                         "number": message_number}
            self.body["meta"] = meta_data
        else:
            self.body["meta"]["type"] = "incoming"
            if self.body["meta"]["akn_id"] is None:
                self.body["meta"]["akn_id"] = config.node_id
                self.body["meta"]["arrived_at"] = str(datetime.datetime.now())


class MessageBrokerClient:
    def __init__(self, config: NodeConfig, silent: bool = False) -> None:
        self.nodeConfig = config
        self._message_broker = AsyncClient(
            base_url=f"http://{self.nodeConfig.nginx_name}/message-broker",
            headers={"Authorization": f"Bearer {config.keycloak_token}", "Accept": "application/json"},
            follow_redirects=True
        )
        asyncio.run(self._connect(silent=silent))
        self.list_of_incoming_messages: list[Message] = []
        self.list_of_outgoing_messages: list[Message] = []
        self.message_number = 0
        message_node_info = asyncio.run(self.get_self_config(config.analysis_id))
        self.nodeConfig.set_role(message_node_info["nodeType"])
        self.nodeConfig.set_node_id(message_node_info["nodeId"])

    def refresh_token(self, keycloak_token: str):
        self._message_broker = AsyncClient(
            base_url=f"http://{self.nodeConfig.nginx_name}/message-broker",
            headers={"Authorization": f"Bearer {keycloak_token}", "Accept": "application/json"},
            follow_redirects=True
        )

    async def get_self_config(self, analysis_id: str) -> dict[str, str]:
        response = await self._message_broker.get(f'/analyses/{analysis_id}/participants/self',
                                                  headers=[('Connection', 'close')])
        response.raise_for_status()
        return response.json()

    async def get_partner_nodes(self, self_node_id: str, analysis_id: str) -> list[dict[str, str]]:
        response = await self._message_broker.get(f'/analyses/{analysis_id}/participants',
                                                  headers=[('Connection', 'close')])

        response.raise_for_status()

        response = [node_conf for node_conf in response.json() if node_conf['nodeId'] != self_node_id]
        return response

    async def test_connection(self) -> bool:
        response = await self._message_broker.get("/healthz",
                                                  headers=[('Connection', 'close')])
        try:
            response.raise_for_status()
            return True
        except HTTPError:
            return False

    async def _connect(self, silent: bool = False) -> None:
        response = await self._message_broker.post(
            f'/analyses/{os.getenv("ANALYSIS_ID")}/messages/subscriptions',
            json={'webhookUrl': f'http://{self.nodeConfig.nginx_name}/analysis/webhook'}
        )
        try:
            response.raise_for_status()
        except HTTPError as e:
            flame_log("Failed to subscribe to message broker", silent)
            flame_log(repr(e), silent)
        response = await self._message_broker.get(f'/analyses/{os.getenv("ANALYSIS_ID")}/participants/self',
                                                  headers=[('Connection', 'close')])
        try:
            response.raise_for_status()
        except HTTPError as e:
            flame_log("Successfully subscribed to message broker, but failed to retrieve participants", silent)
            flame_log(repr(e), silent)

    async def send_message(self, message: Message, silent: bool = False) -> None:
        self.message_number += 1
        body = {
            "recipients": message.recipients,
            "message": message.body
        }

        _ = await self._message_broker.post(f'/analyses/{os.getenv("ANALYSIS_ID")}/messages',
                                            json=body,
                                            headers=[('Connection', 'close'),
                                                     ("Content-Type", "application/json")])
        if message.body["meta"]["sender"] == self.nodeConfig.node_id:
            flame_log(f"send message: {body}", silent)

        self.list_of_outgoing_messages.append(message)

    def receive_message(self, body: dict, silent: bool = False) -> None:
        needs_acknowledgment = body["meta"]["akn_id"] is None
        message = Message(message=body, config=self.nodeConfig, outgoing=False)
        self.list_of_incoming_messages.append(message)

        if needs_acknowledgment:
            flame_log("acknowledging ready check" if body["meta"]["category"] == "ready_check" else "incoming message",
                      silent)
            asyncio.run(self.acknowledge_message(message))

    def delete_message_by_id(self, message_id: str, type: Literal["outgoing", "incoming"]) -> int:
        """
        Delete a message from the outgoing messages list.
        :param type:
        :param message_id:
        :return:
        """
        number_of_deleted_messages = 0
        if type == "outgoing":
            for message in self.list_of_outgoing_messages:
                if message.body["meta"]["id"] == message_id:
                    self.list_of_outgoing_messages.remove(message)
                    number_of_deleted_messages += 1
            if number_of_deleted_messages == 0:
                raise ValueError(f"Could not find message with id={message_id} in outgoing messages.")
        if type == "incoming":
            for message in self.list_of_outgoing_messages:
                if message.body["meta"]["id"] == message_id:
                    self.list_of_outgoing_messages.remove(message)
                    number_of_deleted_messages += 1
            if number_of_deleted_messages == 0:
                raise ValueError(f"Could not find message with id={message_id} in outgoing messages.")
        return number_of_deleted_messages

    async def await_message(self, node_id: str,
                            message_category: str,
                            message_id: Optional[str] = None) -> tuple[str, list[Message]]:
        possible_responses = []
        for msg in self.list_of_incoming_messages:
            if ((node_id == msg.body["meta"]["sender"]) and
                    (message_category == msg.body["meta"]["category"]) and
                    ("unread" == msg.body["meta"]["status"])):
                if message_id is not None:
                    if message_id == msg.body["meta"]["id"]:
                        possible_responses.append(msg)
                else:
                    possible_responses.append(msg)

        if len(possible_responses) == 0:
            number_of_incoming_messages = len(self.list_of_incoming_messages)
            while True:
                await asyncio.sleep(1)
                if len(self.list_of_incoming_messages) > number_of_incoming_messages:
                    for msg in self.list_of_incoming_messages:
                        if ((node_id == msg.body["meta"]["sender"]) and
                                (message_category == msg.body["meta"]["category"]) and
                                ("unread" == msg.body["meta"]["status"])):
                            if message_id is not None:
                                if message_id == msg.body["meta"]["id"]:
                                    possible_responses.append(msg)
                            else:
                                possible_responses.append(msg)
                            return node_id, possible_responses
        else:
            return node_id, possible_responses

    async def acknowledge_message(self, message: Message) -> None:
        await self.send_message(message)

    async def await_message_acknowledgement(self, message: Message, receiver: str) -> str:
        number_of_incoming_messages = len(self.list_of_incoming_messages)
        for incoming_message in self.list_of_incoming_messages:
            if (incoming_message.body["meta"]["id"] == message.body["meta"]["id"]) and \
                    (incoming_message.body["meta"]["akn_id"] == receiver):
                return receiver
        while True:
            if len(self.list_of_incoming_messages) > number_of_incoming_messages:
                for incoming_message in self.list_of_incoming_messages:
                    if (incoming_message.body["meta"]["id"] == message.body["meta"]["id"]) and \
                            (incoming_message.body["meta"]["akn_id"] == receiver):
                        return receiver
            await asyncio.sleep(1)

    def clear_messages(self,
                       type: Literal["outgoing", "incoming"],
                       status: Literal["read", "unread", "all"] = "read",
                       min_age: int = None) -> int:
        """
        Clear the incoming messages list.
        :param type:
        :param status: the status of the messages to clear
        :param min_age:
        :return:
        """
        number_of_deleted_messages = 0
        message_list = self.list_of_outgoing_messages if type == 'outgoing' else self.list_of_incoming_messages
        for message in message_list:
            if message.body["meta"]["status"] == status:
                if min_age is not None:
                    created_at = datetime.datetime.strptime(message.body["meta"]["created_at"],
                                                            "%Y-%m-%d %H:%M:%S.%f")
                    if (datetime.datetime.now() - created_at).seconds > min_age:
                        message_list.remove(message)
                        number_of_deleted_messages += 1
                else:
                    message_list.remove(message)
                    number_of_deleted_messages += 1

        if type == 'outgoing':
            self.list_of_outgoing_messages = message_list
        else:
            self.list_of_incoming_messages = message_list

        return number_of_deleted_messages
