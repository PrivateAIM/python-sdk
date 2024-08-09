import asyncio
from typing import Any

from flame.schemas.star import (FlameSDK,
                                Aggregator,
                                Analyzer)


class My_Analyzer(Analyzer):
    def __init__(self, node_config):
        super().__init__(node_config)
        pass

    def analysis_method(self, data, aggregator_results) -> Any:
        """
        This method will be used to analyze the data. It has to be overridden.
        :param data:
        :param aggregator_results:
        :return:
        """
        #your personal analysis method here

        number_of_patients = data[0]['total']

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
        return True


def main():
    flame = FlameSDK()

    if flame.is_analyzer():
        print("Analyzer")
        my_analyzer = My_Analyzer  # or My_Analyzer(flame.node_config, **kwargs), if implemented with custom params
        asyncio.run(flame.start_analyzer(my_analyzer, query='Patient?_summary=count'))
    elif flame.is_aggregator():
        print("Aggregator")
        my_aggregator = My_Aggregator
        asyncio.run(flame.start_aggregator(my_aggregator, cutoff=1.0, is_federated=False))
    else:
        print("What happened??")
        raise BrokenPipeError("Has to be either analyzer or aggregator")


if __name__ == "__main__":
    main()
    ''' app = FastAPI(title=f"FLAME {'Analysis'}",
                  docs_url="/api/docs",
                  redoc_url="/api/redoc",
                  openapi_url="/api/v1/openapi.json", )
    
    origins = [
        "http://localhost:8080/",
    ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    router = APIRouter()
    
    
    @router.get("/healthz", response_class=JSONResponse)
    def health() -> dict[str, str]:
        return {"status": "true"}
    
    
    async def get_body(request: Request) -> dict[str, Any]:
        print("Received message webhook")
        return await request.json()
    
    
    app.include_router(
        router,
        prefix='',
    )
    
    uvicorn.run(app, host="0.0.0.0", port=8000)'''






