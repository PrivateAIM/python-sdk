from typing import List, Literal, IO


class StorageAPI:
    def __init__(self):
        pass

    def submit_final_result(self, result: IO) -> str:
        """
        sends the final result to the hub. Making it available for analysts to download.
        This method is only available for nodes for which the method `get_role(self)` returns "aggregator”.
        :param result: the final result
        :return: the request status code
        """
        pass
        # TODO Implement this

    def save_intermediate_data(self, location: Literal["local", "global"], data: IO) -> str:
        """
        saves intermediate results/data either on the hub (location="global"), or locally
        :param location: the location to save the result, local saves in the node, global saves in central instance of MinIO
        :param data: the result to save
        :return: the request status code
        """
        pass
        # TODO Implement this

    def list_intermediate_data(self, location: Literal["local", "global"]) -> List[str]:
        """
        returns a list of all locally/globally saved intermediate data available
        :param location: the location to list the result, local lists in the node, global lists in central instance of MinIO
        :return: the list of results
        """
        pass
        # TODO Implement this

    def get_intermediate_data(self, location: Literal["local", "global"], id: str) -> IO:
        """
        returns the intermediate data with the specified id
        :param location: the location to get the result, local gets in the node, global gets in central instance of MinIO
        :param id: the id of the result to get
        :return: the result
        """
        pass
        # TODO Implement this
