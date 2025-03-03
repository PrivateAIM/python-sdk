from flame.patterns.templates.aggregator_generic import my_Aggregator
from flame.patterns.star import FlameSDK


def main():
    # start the communication with the flame message protocols, and alive api
    flame = FlameSDK()
    node_config = flame.get_node_config()

    # start node in aggregator or analysis mode
    if flame.is_aggregator():
        flame.start_aggregator(my_Aggregator(node_config=node_config, ))
    elif flame.is_analyzer():
        flame.start_analyzer()
    else:
        raise ValueError("Fatal: Found unknown value for node mode.")


if __name__ == "__main__":
    main()
