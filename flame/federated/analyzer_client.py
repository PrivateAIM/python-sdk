from typing import Callable


class Analyzer:
    def __init__(self, nodes: list[Node], analysis_method: Callable) -> None:
        self.nodes = nodes
        self.analysis_method = analysis_method

    async def analyze(self, cutoff: float) -> None:
        while sum([await node.responded for node in self.nodes]) < int((len(self.nodes) + 1) * cutoff):
            pass
        self.analysis_method([await node.get_result() for node in self.nodes])
