from flame.protocols import StandartNNAggregatorMethod
from flame.flame import FlameSDK


def main():
    # start the communication with the flame message protocols, and alive api
    flame = FlameSDK()
    # test all communication paths
    flame.test_apis()

    # start node in aggregator or analysis mode
    if flame.is_aggregator():
        flame.start_aggregator(StandartNNAggregatorMethod())
    elif flame.is_analyzer():
        flame.start_analyzer()
    else:
        raise ValueError("Fatal: Found unknown value for node mode.")


if __name__ == "__main__":
    main()
