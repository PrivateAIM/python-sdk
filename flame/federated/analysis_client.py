from typing import Callable

from pydantic import BaseModel


class Analyzer(BaseModel):
    analysis_method: Callable

    def analyze(self):
        self.analysis_method()
