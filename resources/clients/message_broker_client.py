import os
import uuid
import asyncio
import datetime

from httpx import AsyncClient, HTTPError

from resources.analysis_config import AnalysisConfig


class Message:
    def __init__(self, recipients: list[str], message: dict, category: str, config: AnalysisConfig,
                 message_number: int) -> None:
        self.recipients = recipients
        self.meta = self._meta_dict(category, message_number, config)
        self.message_acknowledged = False
        self.message = self._combine_message_and_meta(message, self.meta)

    def _combine_message_and_meta(self, message: dict, meta: dict) -> dict:
        message["meta"] = meta
        return message

    def accnowledge_message(self):
        self.message_acknowledged = True

    def _add_meta_data(self, category: str, message_number: int, config: AnalysisConfig) -> dict:
        messae_meta_data = {}
        messae_meta_data["type"] = "message"
        messae_meta_data["category"] = category
        messae_meta_data["id"] = uuid.uuid4()
        messae_meta_data["akn_msg"] = False
        messae_meta_data["status"] = "unread"
        messae_meta_data["sender"] = config.node_id
        messae_meta_data["created_at"] = str(datetime.datetime.now())
        messae_meta_data["arrived_at"] = None
        messae_meta_data["number"] = message_number
        return messae_meta_data


class MessageBrokerClient:
    def __init__(self, nginx_name, keycloak_token) -> None:
        self._message_broker = AsyncClient(
            base_url=f"http://{nginx_name}/message-broker",
            headers={"Authorization": f"Bearer {keycloak_token}", "Accept": "application/json"}
        )
        asyncio.run(self._connect())
        self.list_of_incoming_messages: list[dict] = []
        self.list_of_outgoing_messages: list[dict] = []

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

    async def _connect(self) -> None:
        response = await self._message_broker.post(
            f'/analyses/{os.getenv("ANALYSIS_ID")}/messages/subscriptions',
            json={'webhookUrl': f'http://nginx-{os.getenv("DEPLOYMENT_NAME")}/analysis/webhook'}
        )
        print(f"message broker connect response  {response}")
        print(f'/analyses/{os.getenv("ANALYSIS_ID")}/messages/subscriptions')
        print({'webhookUrl': f'http://nginx-{os.getenv("DEPLOYMENT_NAME")}/analysis/webhook'})

        response = await self._message_broker.get(f'/analyses/{os.getenv("ANALYSIS_ID")}/participants/self',
                                                  headers=[('Connection', 'close')])
        response.raise_for_status()

    async def send_message(self, message: Message):
        body = {
            "recipients": message.recipients,
            "message": message.message
        }
        print('body type:', type(body))
        print('body:', body)
        response = await self._message_broker.post(f'/analyses/{os.getenv("ANALYSIS_ID")}/messages',
                                                   json=body,
                                                   headers=[('Connection', 'close'),
                                                            ("Content-Type", "application/json")])
        print(f"message broker send response  {response}")
        #print(f"message  send   response json {response}")
        print(f"message  send   {body}")

        self.list_of_outgoing_messages.append(body)

    def receive_message(self, body: dict) -> None:
        self.list_of_incoming_messages.append(body)
        print(f"incoming messages {body}")



class MessageWaiter:
    def __init__(self,message_broker_client: MessageBrokerClient, message: Message, message_orgin: str):
        self.message_orgin = message_orgin
        self._message_broker_client = message_broker_client
        self.message = message

    async def await_message_acknowledgement(self) -> bool:
        pass
        # todo observer message borker client for incoming messages
        # todo check if the message is the acknowledgment of the message we are waiting for
        # return True if the message is the acknowledgment of the message we are waiting for


    def get_origin(self):
        return self.message_orgin




