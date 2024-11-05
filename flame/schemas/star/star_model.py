import time
from io import BytesIO
from enum import Enum
import pickle

from typing import Any, Optional, Type, Literal, Union

from flame import FlameCoreSDK

from flame.schemas.star.aggregator_client import Aggregator
from flame.schemas.star.analyzer_client import Analyzer


class _ERROR_MESSAGES(Enum):
    IS_ANALYZER = 'Node is configured as analyzer. Unable to execute command associated to aggregator.'
    IS_AGGREGATOR = 'Node is configured as aggregator. Unable to execute command associated to analyzer.'
    IS_INCORRECT_CLASS = 'The object/class given is incorrect, e.g. is not correctly implementing/inheriting the ' \
                         'intended template class.'


class StarModel:
    flame: FlameCoreSDK

    def __init__(self,
                 analyzer: Type[Analyzer],
                 aggregator: Type[Aggregator],
                 data_type: Literal['fhir', 's3'],
                 query: Optional[Union[str, list[str]]] = None,
                 simple_analysis: bool = True,
                 output_type: Literal['str', 'bytes', 'pickle'] = 'str',
                 analyzer_kwargs: Optional[dict] = None,
                 aggregator_kwargs: Optional[dict] = None) -> None:
        self.flame = FlameCoreSDK()

        if self._is_analyzer():
            print("Analyzer started")
            self._start_analyzer(analyzer,
                                 data_type=data_type,
                                 query=query,
                                 simple_analysis=simple_analysis,
                                 analyzer_kwargs=analyzer_kwargs)
        elif self._is_aggregator():
            print("Aggregator started")
            self._start_aggregator(aggregator,
                                   simple_analysis=simple_analysis,
                                   output_type=output_type,
                                   aggregator_kwargs=aggregator_kwargs)
        else:
            raise BrokenPipeError("Has to be either analyzer or aggregator")
        print("Analysis finished!")

    def _is_aggregator(self) -> bool:
        return self.flame.get_role() == 'aggregator'

    def _is_analyzer(self) -> bool:
        return self.flame.get_role() == 'default'

    def _start_aggregator(self,
                          aggregator: Type[Aggregator],
                          simple_analysis: bool = True,
                          output_type: Literal['str', 'bytes', 'pickle'] = 'str',
                          aggregator_kwargs: Optional[dict] = None) -> None:
        if self._is_aggregator():
            if issubclass(aggregator, Aggregator):
                # init custom aggregator subclass
                if aggregator_kwargs is None:
                    aggregator = aggregator(flame=self.flame)
                else:
                    aggregator = aggregator(flame=self.flame, **aggregator_kwargs)

                # Ready Check
                self._wait_until_partners_ready()

                while not self._converged():  # (**)
                    # Await number of responses reaching number of necessary nodes
                    node_response_dict = self.flame.await_and_return_responses(node_ids=aggregator.partner_node_ids,
                                                                               message_category='intermediate_results')

                    print(f"Node responses: {node_response_dict}")
                    if all([v for v in list(node_response_dict.values())]):
                        intermediate_res_ids = [response[-1].body['result']
                                                for response in list(node_response_dict.values())
                                                if response is not None]
                        print(f"Intermediate result ids received: {intermediate_res_ids}")
                        node_results = [self.flame.get_intermediate_data(location='global', id=intermediate_res_id)
                                        for intermediate_res_id in intermediate_res_ids]
                        # Aggregate results
                        aggregated_res, converged = aggregator.aggregate(node_results=node_results,
                                                                         simple_analysis=simple_analysis)
                        print(f"Aggregated results: {aggregated_res}")

                        # If converged send aggregated result over StorageAPI to Hub
                        if converged:
                            print("Submitting final results...", end='')
                            response = self.flame.submit_final_result(aggregated_res, output_type)
                            print(f"success (response={response})")
                            self.flame.analysis_finished()  # LOOP BREAK

                        # Else send aggregated results to MinIO for analyzers, loop back to (**)
                        else:
                            self.flame.send_message(aggregator.partner_node_ids,
                                                    'aggregated_results',
                                                    {'result': str(aggregated_res)})
                aggregator.node_finished()
            else:
                raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_ANALYZER.value)

    def _start_analyzer(self,
                        analyzer: Type[Analyzer],
                        data_type: Literal['fhir', 's3'],
                        query: Optional[Union[str, list[str]]] = None,
                        simple_analysis: bool = True,
                        analyzer_kwargs: Optional[dict] = None) -> None:
        if self._is_analyzer():
            if issubclass(analyzer, Analyzer):
                # init custom analyzer subclass
                if analyzer_kwargs is None:
                    analyzer = analyzer(flame=self.flame)
                else:
                    analyzer = analyzer(flame=self.flame, **analyzer_kwargs)

                aggregator_id = self.flame.get_aggregator_id()

                # Ready Check
                self._wait_until_partners_ready()

                # Get data
                data = self._get_data(query=query, data_type=data_type)
                print(f"Data extracted: {data}")

                aggregator_results = None
                converged = False
                # Check converged status on Hub
                while not self._converged():  # (**)
                    if not converged:
                        # Analyze data
                        analyzer_res, converged = analyzer.analyze(data=data,
                                                                   aggregator_results=aggregator_results,
                                                                   simple_analysis=simple_analysis)

                        submission_response = self.flame.save_intermediate_data("global", analyzer_res)

                        # Send result to (MinIO for) aggregator
                        self.flame.send_message(receivers=[aggregator_id],
                                                message_category='intermediate_results',
                                                message={'result': str(submission_response['id'])})
                    if (not self._converged()) and (not converged):
                        # Check for aggregated results
                        aggregator_results = self.flame.await_and_return_responses(node_ids=[aggregator_id],
                                                                                   message_category='aggregated_results',
                                                                                   timeout=300)[aggregator_id][-1].body['result']
                analyzer.node_finished()
            else:
                raise BrokenPipeError(_ERROR_MESSAGES.IS_INCORRECT_CLASS.value)
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_AGGREGATOR.value)

    def _wait_until_partners_ready(self):
        if self._is_analyzer():
            aggregator_id = self.flame.get_aggregator_id()
            print("Awaiting contact with aggregator node...")
            received_list = []
            while aggregator_id not in received_list:
                time.sleep(1)

                received_list, _ = self.flame.send_message(receivers=[aggregator_id],
                                                           message_category='ready_check',
                                                           message={},
                                                           timeout=120)
                print(f"Aggregator ID: {aggregator_id}\n"
                      f"Received List: {received_list}\n"
                      f"ReadyCheck done: {aggregator_id in received_list}")
            if aggregator_id not in received_list:
                raise BrokenPipeError("Could not contact aggregator")

            print("Awaiting contact with aggregator node...success")
        else:
            analyzer_ids = self.flame.get_participant_ids()
            all_received_list = []
            all_received_len = len(all_received_list)
            while len(all_received_list) < len(analyzer_ids):
                time.sleep(1)

                if len(all_received_list) != all_received_len:
                    print(f"Awaiting contact with analyzer nodes...({len(all_received_list)}/{len(analyzer_ids)})")
                    all_received_len = len(all_received_list)

                received_list, _ = self.flame.send_message(receivers=analyzer_ids,
                                                           message_category='ready_check',
                                                           message={},
                                                           timeout=120)
                for received in received_list:
                    if received not in all_received_list:
                        all_received_list.append(received)

                print(f"Analyzer IDs: {analyzer_ids}\n"
                      f"All received List: {all_received_list}\n"
                      f"ReadyCheck done: {len(all_received_list) == len(analyzer_ids)}")

            print("Awaiting contact with analyzer nodes...success")

    def _get_data(self,
                  data_type: Literal['fhir', 's3'],
                  query: Optional[Union[str, list[str]]] = None) -> list[dict[str, Union[dict, str]]]:
        if type(query) == str:
            query = [query]

        if data_type == 'fhir':
            response = self.flame.get_fhir_data(query)
        else:
            response = self.flame.get_s3_data(query)

        return response

    def _converged(self) -> bool:
        return self.flame.config.finished
