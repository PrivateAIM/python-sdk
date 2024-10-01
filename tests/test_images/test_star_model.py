from flame.schemas.star import StarModel, StarAnalyzer, StarAggregator


class MyAnalyzer(StarAnalyzer):
    def __init__(self, flame):
        super().__init__(flame)

    def analysis_method(self, data, aggregator_results):
        return float(data[0]['Patient?_summary=count']['total'])


class MyAggregator(StarAggregator):
    def __init__(self, flame):
        super().__init__(flame)

    def aggregation_method(self, analysis_results):
        return sum([res for res in analysis_results])

    def has_converged(self, result, last_result):
        return True


def main():
    StarModel(analyzer=MyAnalyzer,
              aggregator=MyAggregator,
              data_type='fhir',
              query='Patient?_summary=count')


if __name__ == "__main__":
    main()
