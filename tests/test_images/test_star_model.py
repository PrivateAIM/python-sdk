from flame.patterns.star import StarModel, StarAnalyzer, StarAggregator


class MyAnalyzer(StarAnalyzer):
    def __init__(self, flame):
        super().__init__(flame)  # Necessity to connect to node components

    def analysis_method(self, data, aggregator_results):
        # data: list[dict[str, Union[dict, str]]]
        #   * list of dictionaries, one dictionary per data source (say multiple fhir/s3 data sources are selected)
        #   * dictionaries contain given queries as keys and either str for s3 data or dict for fhir data as values
        # aggregator_results: Any
        #   * in multi-iterative analysis, this returns None in the first iteration, and any value specified by the
        #     custom aggregator class during the following (i.e. equals the results of the aggregation_method)

        # TODO: Implement your analysis method (here: retrieving first fhir dataset, extract patient counts,
        #  take total number of patients)
        return float(data[0]['Patient?_summary=count']['total'])


class MyAggregator(StarAggregator):
    def __init__(self, flame):
        super().__init__(flame)  # Necessity to connect to node components

    def aggregation_method(self, analysis_results):
        # TODO: Implement your aggregation method (here: sum up total patient counts across all nodes)
        return sum([res for res in analysis_results])

    def has_converged(self, result, last_result, num_iterations):
        # TODO (optional): if the parameter 'simple_analysis' in 'StarModel' is set to False,
        #  this function defines the exit criteria in a multi-iterative analysis (otherwise ignored)
        return True


def main():
    StarModel(analyzer=MyAnalyzer,  # your analyzer class (must inherit StarAnalyzer)
              aggregator=MyAggregator,  # your aggregator class (must inherit StarAggregator)
              data_type='fhir',  # either 'fhir' or 's3' depending on the target data
              query='Patient?_summary=count',  # query or queries for the data (dataset names for s3, fhir queries for fhir)
              simple_analysis=True,  # bool defining a single-iteration (True) or multi-iterative analysis (False)
              output_type='str',  # output type in result file (either 'str', 'bytes', or 'pickle')
              analyzer_kwargs=None,  # additional keyword arguments for custom analyzer class (i.e. MyAnalyzer)
              aggregator_kwargs=None)  # additional keyword arguments for custom aggregator class (i.e. MyAggregator)


if __name__ == "__main__":
    main()
