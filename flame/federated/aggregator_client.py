from abc import abstractmethod
from typing import Any, Callable, Optional
from flame.federated.node_base_client import Node, NodeConfig


class Aggregator(Node):
    nodes: list[Node]
    num_iterations: float = 0
    model_params: Optional[dict[str: str | float | int | bool]]
    aggr_weights: Optional[list[Any]]
    gradients: Optional[list[list[float]]]
    converged: bool = False

    def __init__(self,
                 node_config: NodeConfig,
                 model_params: Optional[dict[str: str | float | int | bool]] = None,
                 weights: Optional[list[Any]] = None,
                 gradients: Optional[list[list[float]]] = None) -> None:
        if node_config.node_mode != 'aggregator':
            raise ValueError(f'Attempted to initialize aggregator node with mismatching configuration '
                             f'(expected: node_mode="aggregator", received="{node_config.node_mode}").')
        super().__init__(node_config.node_id, 'aggregator')
        
        self.nodes = node_config.partner_nodes
        self.model_params = model_params
        self.aggr_weights = weights
        self.gradients = gradients

    async def aggregate(self) -> str:
        result = self.aggregation_method([await node.get_result() for node in self.nodes])
        result_file = self.set_result(result)
        # TODO: Update converged status
        self.num_iterations += 1

        return result_file

    @abstractmethod
    def aggregation_method(self, analysis_results: list[Any]) -> Any:
        """
        This method will be used to aggregate the data. It has to be overridden.
        :return: aggregated_result
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

    def _converge(self):
        self.converged = True
        # TODO: send converged status to hub
