from abc import abstractmethod
from typing import Any, Optional
import time

from flame.federated.node_base_client import Node, NodeConfig
from resources.clients.message_broker_client import Message, MessageBrokerClient


class Aggregator(Node):
    node: Node
    nodes: list[Node]
    num_iterations: float = 0
    model_params: Optional[dict[str, str | float | int | bool]]
    aggr_weights: Optional[list[Any]]
    gradients: Optional[list[list[float]]]
    converged: bool = False
    is_federated: bool = False

    def __init__(self,
                 node_config: NodeConfig,
                 is_federated: bool,
                 model_params: Optional[dict[str, str | float | int | bool]] = None,
                 weights: Optional[list[Any]] = None,
                 gradients: Optional[list[list[float]]] = None) -> None:
        if node_config.node.node_mode != 'aggregator':
            raise ValueError(f'Attempted to initialize aggregator node with mismatching configuration '
                             f'(expected: node_mode="aggregator", received="{node_config.node.node_mode}").')
        super().__init__(node_config.node.node_id, 'aggregator')

        self.node = node_config.node
        self.is_federated = is_federated
        self.nodes = node_config.partner_nodes
        self.model_params = model_params
        self.aggr_weights = weights
        self.gradients = gradients
        self.is_federated = is_federated

    async def aggregate(self, message_broker: MessageBrokerClient) -> str:
        result = self.aggregation_method([node.get_result(message_broker) for node in self.nodes])
        result_file = self.set_result(result)

        # Check convergence status
        if (not self.is_federated) or self.has_converged(result):
            await self._converge(message_broker)

        self.num_iterations += 1

        return result_file

    @abstractmethod
    def aggregation_method(self, analysis_results: list[Any]) -> Any:
        """
        This method will be used to aggregate the data. It has to be overridden.
        :return: aggregated_result
        """
        pass

    @abstractmethod
    def has_converged(self, aggregator_results: list[Any]) -> bool:
        """
        This method will be used to check if the aggregator has converged. It has to be overridden.
        :return: converged
        """
        pass

    # def _calc_gradients(self, new_weights: list[Any]) -> None:
    #     if self.gradients is not None:
    #         old_weights = self.aggr_weights
    #         self.gradients = old_weights - new_weights
    #         # converged?
    #     else:
    #         self.gradients = new_weights

    def get_weights(self) -> list[Any]:
        return self.aggr_weights

    async def _converge(self, message_broker: MessageBrokerClient) -> None:
        self.converged = True
        await message_broker.send_message(Message([node.node_id for node in self.nodes], {"convStatus": True,
                                                                                    "sender": self.node.node_id}))

    def await_results(self,
                      message_broker: MessageBrokerClient,
                      cutoff: float) -> None:
        # Calculate number of necessary nodes (number of nodes * cutoff)
        num_necessary_nodes = int(len(self.nodes) * cutoff)

        # Await node responses
        node_responded = {node: False for node in self.nodes}
        while sum(node_responded.values()) < num_necessary_nodes:
            for msg in message_broker.list_of_incoming_messages:
                for node in self.nodes:
                    if (node.node_id == msg["sender"]) and ("resultData" in msg.keys()):
                        node_responded[node] = True

            time.sleep(5)
            print(f"Waiting for responses (current count: {sum(node_responded.values())} of {num_necessary_nodes})")
