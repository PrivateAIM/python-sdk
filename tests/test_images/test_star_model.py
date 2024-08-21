import time

from flame.schemas.star import StarModel, StarAnalyzer, StarAggregator


class MyAnalyzer(StarAnalyzer):
    def __init__(self, flame):
        super().__init__(flame)
        pass

    def analysis_method(self, data, aggregator_results):
        return float(data["total"])


class MyAggregator(StarAggregator):
    def __init__(self, flame):
        super().__init__(flame)
        pass

    def aggregation_method(self, analysis_results):
        return sum([float(res) for res in analysis_results])

    def has_converged(self, result, last_result):
        return True


def main():
    starmodel = StarModel()

    if starmodel.is_analyzer():
        print("Analyzer started")
        my_analyzer = MyAnalyzer  # or MyAnalyzer(starmodel.flame, **kwargs), if implemented with custom params
        starmodel.start_analyzer(my_analyzer, query='Patient?_summary=count')
    elif starmodel.is_aggregator():
        print("Aggregator started")
        my_aggregator = MyAggregator
        starmodel.start_aggregator(my_aggregator)
    else:
        raise BrokenPipeError("Has to be either analyzer or aggregator")

    print("Analysis finished!")
    time.sleep(5)


if __name__ == "__main__":
    main()
