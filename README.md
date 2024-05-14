# PrivateAIM Python Template

This repository serves as a template for Python-based repositories within the PrivateAIM project.
It comes preconfigured with tools for testing, building, linting and formatting your Python code.

# Python sdk core

## Startup 

Every FLAME analysis starts by connecting itself to the other components of the flame platform and starting the Analysis api.
This is done by creating a FlameSDK object.

```python
from flame import FlameSDK

if __name__ == "__main__":
   flame = FlameSDK()
   
```
During the creation of the FlameSDK object, the connection to the other components of the flame platform is established.

## Message Broker Client
### List of available methods
- `get_list_of_partisepationg (self) -> None`
  - Returns a list of all nodes participating in the analysis.
- `send_message(self,resiver, message: dict) -> None`
    - Sends a message to the specified node.
- `send_message_and_wait_for_response(self,resiver,message: dict) -> dict`
    - Sends a message to the specified node and waits for a response.
- `await_message(self,node_id, timout: int = None) -> dict`
    - Waits for a message to arrive.
- `get_list_of_messages(self) -> list`
    - Returns a list of all messages that have arrived. 

### List of quality of life methods

- `ask_status_nodes(self,timout: int) -> str`
    - Returns the status of all nodes.
    - Status can be: "online", "offline", "not_connected"
    - Timout is the time to wait for a response from the nodes.
- `send_intermediate_result(self,resiver, result: IOstream) -> None`
    - Sends an intermediate result using service and message borker.
### Message structure
```json
{
    "sender": "node_id",
    "time": "datetime",
    "message_nuber": "int",
    "data": "dict"
}
```

## Result Service (temporary name)
### List of available methods
- `send_result(self, result: IOstream) -> None`
  - sends final result to the hub. To be avialable analyist to download.
- `save_intermediate_result(self, result: IOstream) -> result_id` 
  - saves intermediate result to the hub. To be other nodes to download.
- `save_intermediate_result_local(self, result: IOstream) -> result_id`
  - saves intermediate result to the local storage. To be avilabel for later analysis in the same Projket.
- `list_intermediate_local_result(self) -> result_info_local`
  - returns a list of all intermediate results saved locally.
- `list_intermediate_result(self) -> result_info`
  - returns a list of all intermediate results saved on the hub.
- `get_intermediate_result(self, result_id: str) -> IOstream`
  - returns the intermediate result with the specified id.
- `get_intermediate_result_local(self, result_id: str) -> IOstream`
    - returns the intermediate result with the specified id saved locally.
- `get_role(self) -> bool`
    - get the role of the node. True if the node is a aggregator and can subbmit finainal results, else False.
### list of quality of life methods
- store and retrieve specific data types like fhir, torch, pandas, numpy, etc.

### Result structure local
```json
{
  "results" : [
    "result_id": "str",
    "result_name": "str",
    "analyzis_id": "str",
    "result_size": "int",
    "result_time": "datetime",
    "result_owner": "str",
    "other":"dict"
    ]
}
```
### Result structure hub
```json
{
  "results" : [
    "result_id": "str",
    "result_name": "str",
    "node_id": "str",
    "result_size": "int",
    "result_time": "datetime",
    "result_owner": "str",
    "other":"dict"
    ]
}
```

## Data api
### List of available methods
- `get_data_client(self, data_id: str) -> IOstream`
    - Returns the data client for a specific fhir or S3 store used for this project.
- `get_data_sources(self) -> list[data_source]`
  - Returns a list of all data sources available for this project.
  - 