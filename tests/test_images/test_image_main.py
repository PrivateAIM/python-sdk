import asyncio
from typing import Any

from flame import (FlameSDK,
                   Aggregator,
                   Analyzer)


class My_Analyzer(Analyzer):
    def __init__(self, node_config):
        super().__init__(node_config)
        pass

    def analysis_method(self, data, aggregator_results) -> Any:
        print(f"type of data in my analyzer {type(data)}")
        print(f"data in my analyser {data}")
        number_of_patients = data[0]['total']
        print(f"number of patients in my analyzer {number_of_patients}")
        return number_of_patients


class My_Aggregator(Aggregator):
    def __init__(self, node_config, is_federated=False):
        super().__init__(node_config, is_federated)
        pass

    def aggregation_method(self, analysis_results) -> Any:
        print(f"analysis_results in my aggregator {analysis_results}")
        sum = 0
        for result in analysis_results:
            print(f"result in my aggregator {result}")
            sum += result
        print(f"sum in my aggregator {sum}")

        return sum

    def has_converged(self, aggregator_results) -> bool:
        print(f"aggregator_results in my aggregator {aggregator_results}")
        # if
        return True


def main():
    flame = FlameSDK()
    flame.send_message(recipients=[node.node_id for node in flame.node_config.partner_nodes], message={"foo": "bar"} )
    if flame.is_analyzer():
        print("Analyzer")
        my_analyzer = My_Analyzer  # or My_Analyzer(flame.node_config, **kwargs), if implemented with custom params
        asyncio.run(flame.start_analyzer(my_analyzer, 'Patient?_summary=count'))
    elif flame.is_aggregator():
        print("Aggregator")
        my_aggregator = My_Aggregator
        asyncio.run(flame.start_aggregator(my_aggregator, is_federated=False))
    else:
        print("What happened??")
        raise BrokenPipeError("Has to be either analyzer or aggregator")


if __name__ == "__main__":
    main()
