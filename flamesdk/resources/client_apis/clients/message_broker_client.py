"""HTTP transport for the message broker sidecar, plus the local message log."""

import os
import uuid
import asyncio
import datetime
from typing import Optional, Literal
from httpx import AsyncClient, HTTPStatusError, ConnectError, TimeoutException

from flamesdk.resources.node_config import NodeConfig
from flamesdk.resources.utils.logging import FlameLogger
from flamesdk.resources.utils.constants import LogTypeLiteral


class Message:
    """A single message exchanged between the nodes of an analysis.

    The message body carries a ``meta`` block - sender, category, number,
    timestamps and an acknowledgement id - which the broker uses for routing
    and which is therefore reserved: an outgoing body may not define it itself.

    Outgoing messages are validated on construction; incoming ones are parsed
    from a body the broker delivered and get their recipient set to the sender,
    so that replying is just sending the message back.
    """

    def __init__(
        self,
        message: dict,
        config: NodeConfig,
        outgoing: bool,
        flame_logger: FlameLogger,
        message_number: Optional[int] = None,
        category: Optional[str] = None,
        recipients: Optional[list[str]] = None,
    ) -> None:
        """
        Message object to be sent or received by the message broker.
        :param message: dict body of the message to be sent or received, must not contain the field 'meta'
        :param config: the node configuration
        :param outgoing: boolean value specifying if the message is outgoing or incoming
        :param message_number: the message number
        :param category: the message category
        :param recipients: the list of recipients
        """
        self.flame_logger = flame_logger
        if outgoing:
            if "meta" in message.keys():
                self.flame_logger.raise_error(
                    "Cannot use field 'meta' in message body. "
                    "This field is reserved for meta data used by the message broker."
                )
            elif not isinstance(message_number, int) or isinstance(
                message_number, bool
            ):
                self.flame_logger.raise_error(
                    f"Specified outgoing message, but did not specify integer value for "
                    f"message_number (received: {type(message_number)})."
                )
            elif not isinstance(category, str):
                self.flame_logger.raise_error(
                    f"Specified outgoing message, but did not specify string value for "
                    f"category (received: {type(category)})."
                )
            elif (not isinstance(recipients, list)) or any(
                not isinstance(recipient, str) for recipient in recipients
            ):
                if hasattr(recipients, "__iter__"):
                    self.flame_logger.raise_error(
                        f"Specified outgoing message, but did not specify list of strings "
                        f"value for recipients (received: {type(recipients)} containing "
                        f"{set([type(recipient) for recipient in recipients])})."
                    )
                else:
                    self.flame_logger.raise_error(
                        f"Specified outgoing message, but did not specify list of strings "
                        f"value for recipients (received: {type(recipients)})."
                    )
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

    def _update_meta_data(
        self,
        outgoing: bool,
        config: NodeConfig,
        category: Optional[str] = None,
        message_number: Optional[int] = None,
    ) -> None:
        """
        Adds meta data to the outgoing message or update it for incoming.
        :param outgoing:
        :param config:
        :param category:
        :param message_number:
        :return:
        """
        if outgoing:
            meta_data = {
                "type": "outgoing",
                "category": category,
                "id": f"{config.node_id[:4]}-{message_number}-{str(uuid.uuid4())[-4:]}",
                "akn_id": None,
                "status": "unread",
                "sender": config.node_id,
                "created_at": str(datetime.datetime.now()),
                "arrived_at": None,
                "number": message_number,
            }
            self.body["meta"] = meta_data
        else:
            self.body["meta"]["type"] = "incoming"
            if self.body["meta"]["akn_id"] is None:
                self.body["meta"]["akn_id"] = config.node_id
                self.body["meta"]["arrived_at"] = str(datetime.datetime.now())


class MessageBrokerClient:
    """Thin HTTP transport for the message broker, plus the local message log.

    Besides talking to the broker this keeps the node's own view of the
    conversation: the messages sent, the messages received, and the ids already
    seen, which is what lets duplicates be dropped and replies be matched to
    their original message.
    """

    def __init__(self, config: NodeConfig, flame_logger: FlameLogger) -> None:
        """Subscribe to the broker and learn this node's own role and id.

        Constructing this performs the handshake that fills ``node_role`` and
        ``node_id`` in on the shared :class:`NodeConfig`.

        :param config: node configuration; its role and id are filled in here
        :param flame_logger: logger used to report connection problems
        """
        self.nodeConfig = config
        self.flame_logger = flame_logger
        self._message_broker = AsyncClient(
            base_url=f"http://{self.nodeConfig.nginx_name}/message-broker",
            headers={
                "Authorization": f"Bearer {config.keycloak_token}",
                "Accept": "application/json",
            },
            follow_redirects=True,
        )
        asyncio.run(self._connect())
        self.list_of_known_message_ids: set[str] = set()
        self.list_of_incoming_messages: list[Message] = []
        self.list_of_outgoing_messages: list[Message] = []
        self.message_number = 0
        message_node_info = asyncio.run(self.get_self_config(config.analysis_id))
        self.nodeConfig.set_role(message_node_info["nodeType"])
        self.nodeConfig.set_node_id(message_node_info["nodeId"])

    def refresh_token(self, keycloak_token: str):
        """Replace the broker client with one using a freshly issued token.

        :param keycloak_token: the renewed bearer token
        """
        self._message_broker = AsyncClient(
            base_url=f"http://{self.nodeConfig.nginx_name}/message-broker",
            headers={
                "Authorization": f"Bearer {keycloak_token}",
                "Accept": "application/json",
            },
            follow_redirects=True,
        )

    async def get_self_config(self, analysis_id: str) -> dict[str, str]:
        """Ask the broker who this node is within the analysis.

        :param analysis_id: id of the analysis this node takes part in
        :return: this node's participant record, carrying ``nodeType`` and ``nodeId``
        :raises ValueError: if the broker cannot be reached
        """
        try:
            response = await self._message_broker.get(
                f"/analyses/{analysis_id}/participants/self",
                headers=[("Connection", "close")],
            )
            response.raise_for_status()
        except (HTTPStatusError, ConnectError, TimeoutException) as e:
            self.flame_logger.new_log(
                f"Failed to retrieve self configuration for analysis {analysis_id}",
                log_type=LogTypeLiteral.CRITICAL.value,
            )
            raise ValueError(
                f"Failed to retrieve self configuration for analysis {analysis_id}: {repr(e)}"
            )
        return response.json()

    async def get_partner_nodes(
        self, self_node_id: str, analysis_id: str
    ) -> list[dict[str, str]]:
        """List the other nodes taking part in the analysis.

        :param self_node_id: this node's id, excluded from the result
        :param analysis_id: id of the analysis this node takes part in
        :return: one participant record per partner node
        """
        try:
            response = await self._message_broker.get(
                f"/analyses/{analysis_id}/participants",
                headers=[("Connection", "close")],
            )
            response.raise_for_status()
        except (HTTPStatusError, ConnectError, TimeoutException) as e:
            self.flame_logger.raise_error(
                f"Failed to retrieve partner nodes for analysis {analysis_id} : ",
                hidden_error_msg=repr(e),
            )
        response = [
            node_conf
            for node_conf in response.json()
            if node_conf["nodeId"] != self_node_id
        ]
        return response

    async def test_connection(self) -> bool:
        """Check whether the message broker answers its health check.

        :return: ``True`` if the broker is reachable; a failure is reported as
            an unrecoverable error rather than returning ``False``
        """
        try:
            response = await self._message_broker.get(
                "/healthz", headers=[("Connection", "close")]
            )
            response.raise_for_status()
            return True
        except (HTTPStatusError, ConnectError, TimeoutException) as e:
            self.flame_logger.raise_error(
                "Failed to connect to message broker:", hidden_error_msg=repr(e)
            )
            return False

    async def _connect(self) -> None:
        """Subscribe this node's webhook so the broker can deliver messages."""
        try:
            response = await self._message_broker.post(
                f'/analyses/{os.getenv("ANALYSIS_ID")}/messages/subscriptions',
                json={
                    "webhookUrl": f"http://{self.nodeConfig.nginx_name}/analysis/webhook"
                },
            )
            response.raise_for_status()
        except (HTTPStatusError, ConnectError, TimeoutException) as e:
            self.flame_logger.new_log(
                "Failed to subscribe to message broker",
                log_type=LogTypeLiteral.CRITICAL.value,
            )
            raise ValueError(f"Failed to subscribe to message broker: {repr(e)}")
        try:
            response = await self._message_broker.get(
                f'/analyses/{os.getenv("ANALYSIS_ID")}/participants/self',
                headers=[("Connection", "close")],
            )
            response.raise_for_status()
        except (HTTPStatusError, ConnectError, TimeoutException) as e:
            self.flame_logger.new_log(
                "Successfully subscribed to message broker, but failed to retrieve "
                "participants",
                log_type=LogTypeLiteral.CRITICAL.value,
            )
            raise ValueError(
                f"Successfully subscribed to message broker, but failed to retrieve "
                f"participants: {repr(e)}"
            )

    async def send_message(self, message: Message) -> None:
        """Send a message to its recipients, retrying transient failures.

        Retries up to ten times before reporting the failure as unrecoverable.
        A message that goes out is appended to the outgoing log, which is what
        :meth:`await_message_acknowledgement` later matches against.

        :param message: the message to deliver
        """
        self.message_number += 1
        body = {"recipients": message.recipients, "message": message.body}
        attempt_count = 0
        while True:
            attempt_count += 1
            try:
                response = await self._message_broker.post(
                    f'/analyses/{os.getenv("ANALYSIS_ID")}/messages',
                    json=body,
                    headers=[
                        ("Connection", "close"),
                        ("Content-Type", "application/json"),
                    ],
                )
                response.raise_for_status()
                self.list_of_outgoing_messages.append(message)
                break
            except Exception as e:
                if attempt_count < 10:
                    self.flame_logger.new_log(
                        f"Attempt failed to send message to message broker (attempt={attempt_count})",
                        log_type=LogTypeLiteral.WARNING.value,
                    )
                else:
                    self.flame_logger.raise_error(
                        "Failed to send message to message broker after repeated "
                        "attempts: ",
                        hidden_error_msg=repr(e),
                    )

    def receive_message(self, body: dict) -> None:
        """Take in a message the broker delivered to this node's webhook.

        Messages already seen are dropped, so a redelivery cannot be processed
        twice. A message that asks to be acknowledged is answered here, and a
        role call is answered with this node's role.

        :param body: the raw message body as delivered by the broker
        """
        needs_acknowledgment = body["meta"]["akn_id"] is None
        message = Message(
            message=body,
            config=self.nodeConfig,
            flame_logger=self.flame_logger,
            outgoing=False,
        )
        is_new_message = (
            message.body["meta"]["id"] not in self.list_of_known_message_ids
        )
        sender = message.body["meta"]["sender"]
        category = message.body["meta"]["category"]
        if is_new_message:
            if sender != self.nodeConfig.node_id:
                self.flame_logger.new_log(
                    f"received message from {sender}",
                    log_type=LogTypeLiteral.INFO.value,
                )
                self.flame_logger.new_log(
                    f"message body: {message.body}", log_type=LogTypeLiteral.DEBUG.value
                )
                self.list_of_known_message_ids.add(message.body["meta"]["id"])
            self.list_of_incoming_messages.append(message)

        if needs_acknowledgment:
            if is_new_message:
                self.flame_logger.new_log(
                    (
                        f"acknowledging ready check by sender={sender}"
                        if category == "ready_check"
                        else f"received role call by sender={sender}"
                    )
                    if category in ["ready_check", "role_call"]
                    else f"incoming message with category={category} from sender={sender}",
                    log_type=LogTypeLiteral.DEBUG.value,
                )
            asyncio.run(self.acknowledge_message(message))

            if category == "role_call":
                self._answer_role_call(sender)

    def delete_message_by_id(
        self, message_id: str, type: Literal["outgoing", "incoming"]
    ) -> int:
        """
        Delete a message from the outgoing messages list.
        :param type:
        :param message_id:
        :return:
        """
        number_of_deleted_messages = 0
        if type in ["outgoing", "incoming"]:
            message_list = (
                self.list_of_outgoing_messages.copy()
                if type == "outgoing"
                else self.list_of_incoming_messages.copy()
            )
            for message in message_list:
                if message.body["meta"]["id"] == message_id:
                    self.list_of_outgoing_messages.remove(
                        message
                    ) if type == "outgoing" else self.list_of_incoming_messages.remove(
                        message
                    )
                    number_of_deleted_messages += 1
        if number_of_deleted_messages == 0:
            self.flame_logger.new_log(
                f"Could not find message with id={message_id} in {type} messages.",
                log_type=LogTypeLiteral.WARNING.value,
            )
            return 0
        return number_of_deleted_messages

    async def await_message(
        self, node_id: str, message_category: str, message_id: Optional[str] = None
    ) -> tuple[str, list[Message]]:
        """Wait for unread messages from a node in a given category.

        Returns straight away if matching messages already arrived; otherwise
        polls the incoming log once a second until one does. There is no
        timeout - the caller is expected to impose one.

        :param node_id: the sender to wait for
        :param message_category: the category to wait for
        :param message_id: when given, only match this specific message
        :return: the sender's id together with the matching messages
        """
        possible_responses = []
        for msg in self.list_of_incoming_messages:
            if (
                (node_id == msg.body["meta"]["sender"])
                and (message_category == msg.body["meta"]["category"])
                and ("unread" == msg.body["meta"]["status"])
            ):
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
                        if (
                            (node_id == msg.body["meta"]["sender"])
                            and (message_category == msg.body["meta"]["category"])
                            and ("unread" == msg.body["meta"]["status"])
                        ):
                            if message_id is not None:
                                if message_id == msg.body["meta"]["id"]:
                                    possible_responses.append(msg)
                            else:
                                possible_responses.append(msg)
                            return node_id, possible_responses
        else:
            return node_id, possible_responses

    async def acknowledge_message(self, message: Message) -> None:
        """Send an acknowledgement back to a message's sender.

        :param message: the acknowledgement message to deliver
        """
        await self.send_message(message)

    async def await_message_acknowledgement(
        self, message: Message, receiver: str
    ) -> str:
        """Wait until a given receiver acknowledges a message.

        The acknowledgement is consumed - it is removed from the incoming log,
        since it is bookkeeping rather than something the analysis should read.
        There is no timeout; the caller is expected to impose one.

        :param message: the message whose acknowledgement is awaited
        :param receiver: the node expected to acknowledge it
        :return: the receiver's id, once it has acknowledged
        """
        number_of_incoming_messages = len(self.list_of_incoming_messages)
        for incoming_message in self.list_of_incoming_messages:
            if (incoming_message.body["meta"]["id"] == message.body["meta"]["id"]) and (
                incoming_message.body["meta"]["akn_id"] == receiver
            ):
                self.list_of_incoming_messages.remove(incoming_message)
                return receiver
        while True:
            if len(self.list_of_incoming_messages) > number_of_incoming_messages:
                for incoming_message in self.list_of_incoming_messages:
                    if (
                        incoming_message.body["meta"]["id"]
                        == message.body["meta"]["id"]
                    ) and (incoming_message.body["meta"]["akn_id"] == receiver):
                        self.list_of_incoming_messages.remove(incoming_message)
                        return receiver
            await asyncio.sleep(1)

    def clear_messages(
        self,
        type: Literal["outgoing", "incoming"],
        status: Literal["read", "unread", "all"] = "read",
        min_age: int = None,
    ) -> int:
        """
        Clear the incoming messages list.
        :param type:
        :param status: the status of the messages to clear
        :param min_age:
        :return:
        """
        number_of_deleted_messages = 0
        message_list = (
            self.list_of_outgoing_messages.copy()
            if type == "outgoing"
            else self.list_of_incoming_messages.copy()
        )
        for message in message_list:
            if message.body["meta"]["status"] == status:
                if min_age is not None:
                    created_at = datetime.datetime.strptime(
                        message.body["meta"]["created_at"], "%Y-%m-%d %H:%M:%S.%f"
                    )
                    if (datetime.datetime.now() - created_at).seconds > min_age:
                        self.list_of_outgoing_messages.remove(
                            message
                        ) if type == "outgoing" else self.list_of_incoming_messages.remove(
                            message
                        )
                        number_of_deleted_messages += 1
                else:
                    self.list_of_outgoing_messages.remove(
                        message
                    ) if type == "outgoing" else self.list_of_incoming_messages.remove(
                        message
                    )
                    number_of_deleted_messages += 1

        return number_of_deleted_messages

    def _answer_role_call(self, sender: str) -> None:
        """Reply to a role call with this node's own role.

        :param sender: the node that issued the role call
        """
        msg = Message(
            message={"role": self.nodeConfig.node_role},
            config=self.nodeConfig,
            outgoing=True,
            flame_logger=self.flame_logger,
            message_number=self.message_number,
            category="role_call_answer",
            recipients=[sender],
        )
        asyncio.run(self.send_message(msg))
