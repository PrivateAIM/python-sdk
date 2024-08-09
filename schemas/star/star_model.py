from io import StringIO
from enum import Enum

from typing import Any, Optional, Type

from flame import FlameCoreSDK
from schemas.star.aggregator_client import Aggregator
from schemas.star.analyzer_client import Analyzer


class _ERROR_MESSAGES(Enum):
    IS_ANALYZER = 'Node is configured as analyzer. Unable to execute command associated to aggregator.'
    IS_AGGREGATOR = 'Node is configured as aggregator. Unable to execute command associated to analyzer.'
    IS_INCORRECT_CLASS = 'The object/class given is incorrect, e.g. is not correctly implementing/inheriting the ' \
                         'intended template class.'


class StarModel:
    flame: FlameCoreSDK

    aggregator: Optional[Aggregator]
    analyzer: Optional[Analyzer]

    def __init__(self) -> None:
        self.flame = FlameCoreSDK()

    def is_aggregator(self) -> bool:
        return self.flame.get_role() == 'aggregator'

    def is_analyzer(self) -> bool:
        return self.flame.get_role() == 'default'

    def start_aggregator(self, aggregator: Type[Aggregator] | Any, simple_analysis: bool = True) -> None:

        if self.is_aggregator():
            if isinstance(aggregator, Aggregator) or issubclass(aggregator, Aggregator):
                # init subclass, if not an object
                # (funfact: isinstance(class, Class) returns False, issubclass(object, Class) raises a TypeError)
                self.aggregator = aggregator if isinstance(aggregator, Aggregator) else aggregator(flame=self.flame)

                while not self.converged():  # (**)
                    # Await number of responses reaching number of necessary nodes
                    node_response_dict = self.flame.await_and_return_responses(node_ids=aggregator.partner_node_ids,
                                                                               message_category='intermediate_results')
                    node_results = [response[-1].body['result']
                                    for response in list(node_response_dict.values()) if response is not None]

                    # Aggregate results
                    aggregated_res, converged = self.aggregator.aggregate(node_results=node_results,
                                                                          simple_analysis=simple_analysis)

                    # If converged send aggregated result over StorageAPI to Hub
                    if converged:
                        self.flame.submit_final_result(StringIO(str(aggregated_res)))
                        self.flame.analysis_finished()  # LOOP BREAK

                    # Else send aggregated results to MinIO for analyzers, loop back to (**)
                    else:
                        self.flame.send_message(self.aggregator.partner_node_ids,
                                                'aggregated_results',
                                                {'result': str(aggregated_res)})
            else:
                raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_ANALYZER.value)

    async def start_analyzer(self, analyzer: Type[Analyzer] | Any, query: str, simple_analysis: bool = True) -> None:

        if self.is_analyzer():
            if isinstance(analyzer, Analyzer) or issubclass(analyzer, Analyzer):
                # init subclass, if not an object
                # (funfact: isinstance(class, Class) returns False, issubclass(object, Class) raises a TypeError)
                self.analyzer = analyzer if isinstance(analyzer, Analyzer) else analyzer(flame=self.flame)

                aggregator_id = self.flame.get_aggregator_id()

                # Get data
                data = self.flame.get_fhir_data(data_id='', queries=[query])  # TODO (get_fhir_data is missing in data_api_client)

                aggregator_results = None
                # Check converged status on Hub
                while not self.converged():  # (**)
                    # Analyze data
                    analyzer_res, converged = await self.analyzer.analyze(data=data,
                                                                          aggregator_results=aggregator_results,
                                                                          simple_analysis=simple_analysis)

                    if not converged:
                        # Send result to MinIO for aggregator
                        self.flame.send_message(receivers=[aggregator_id],
                                                message_category='intermediate_results',
                                                message={'result': str(analyzer_res)})

                        # Check for aggregated results
                        aggregator_results = self.flame.await_and_return_responses(node_ids=[aggregator_id],
                                                                                   message_category='aggregated_results',
                                                                                   timeout=300)[aggregator_id][-1].body['result']

            else:
                raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_AGGREGATOR.value)

    def converged(self) -> bool:
        return self.flame.config.finished
