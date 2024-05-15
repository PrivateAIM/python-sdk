# PrivateAIM Python Template
This repository serves as a template for Python-based analyzes within the PrivateAIM project.
It comes preconfigured with tools for testing, building, linting and formatting your Python code.

# Python sdk core
## Startup
Every FLAME analysis starts by connecting itself to the other components of the flame platform and starting an Analysis
REST-API. All of this is done simply by instancing a FlameSDK object.

```python
from flame import FlameSDK

def main():
    flame = FlameSDK()
    # Your code here

if __name__ == "__main__":
    main()
   
```
During the creation of the FlameSDK object, the connection to the other components of the flame platform is established
automatically.

## Message Broker Client
### Purpose
The Message Broker is a service for sending and receiving messages between nodes. It is used as a simplistic 
communication between nodes for control and small data exchange purposes. Note, that Volume Data like ML models should 
be exchanged using the Result Service.

### List of available methods
- `get_list_of_participants(self) -> List[nodeID]`
  - Returns a list of all nodes participating in the analysis.
- `send_message(self, receivers: List[nodeID], message: dict) -> None`
    - Sends a message to all specified nodes.
- `await_message(self, node_ids: List[nodeID], timeout: int = None) -> dict`
    - Waits for a messages to arrive.
- `get_list_of_messages(self) -> List[dict]`
    - Returns a list of all messages that have arrived. 

### List of quality of life methods
- `send_message_and_wait_for_responses(self, receivers: List[nodeID], message: dict, timeout: int = None) -> dict`
    - Sends a message to the specified nodes and waits for responses.
- `ask_status_nodes(self, timeout: int) -> Literal["online", "offline", "not_connected"]`
    - Returns the status of all nodes.
- `send_intermediate_result(self, receivers: List[nodeID], result: IOstream) -> None`
    - Sends an intermediate result using Result Service and Message Broker.

### Message structure
```json
{
    "sender": "node_id",
    "time": "datetime",
    "message_number": "int",
    "data": "dict"
}
```

## Result Service (temporary name "Result Client")
### Purpose
The Result Service is a service for saving and exchanging results between nodes of one analysis and locally between 
different analyzes of the same Project.

### List of available methods
- `submit_final_result(self, result: IOstream) -> Request.Status`
  - sends final result to the hub. Making it available for analysts to download. This method is only available for nodes
    for which the method `get_role(self)` returns "aggregator".
- `save_intermediate_global_result(self, result: IOstream) -> result_id` 
  - saves intermediate result at the hub. Making it available for other nodes to download.
- `save_intermediate_result_local(self, result: IOstream) -> result_id`
  - saves intermediate result to the local storage. Making it available for later analyzes in the same Project.
- `list_intermediate_local_results(self) -> List[result_info]`
  - returns a list of all locally saved intermediate results.
- `list_intermediate_global_results(self) -> List[result_info]`
  - returns a list of all intermediate results saved on the hub.
- `get_intermediate_global_result(self, result_id: str) -> IOstream`
  - returns the global intermediate result with the specified id.
- `get_intermediate_local_result(self, result_id: str) -> IOstream`
    - returns the local intermediate result with the specified id.
- `get_role(self) -> Literal["aggregator", "default"]`
    - get the role of the node. "aggregator" means that the node can submit final results using "submit_final_result", 
    else "default" if not.

### list of quality of life methods
- TODO: store and retrieve specific data types like fhir, torch, pandas, numpy, etc.
- `send_intermediate_result(self, receivers: List[nodeID], result: IOstream) -> None`
    - Sends an intermediate result using Result Service and Message Broker.

### Result structure local
```json
{
  "result_id": "str",
  "result_name": "str",
  "analysis_id": "str",
  "result_size": "int",
  "result_time": "datetime",
  "result_owner": "str",
  "other": "dict"
}
```
### Result structure hub
```json
{
  "result_id": "str",
  "result_name": "str",
  "node_id": "str",
  "result_size": "int",
  "result_time": "datetime",
  "result_owner": "str",
  "other": "dict"
}
```

## Data Source Client
### Purpose
Data Source Client is a service for accessing data from different sources like FHIR or S3 linked to the project. 

### List of available methods
- `get_data_client(self, data_id: str) -> data_client`
  - Returns the data client for a specific fhir or S3 store used for this project.
- `get_data_sources(self) -> List[data_source]`
  - Returns a list of all data sources available for this project.

### List of quality of life methods
- `get_fhir_data(self, queries: List[str]) -> List[dict]`
  - Returns the data from the FHIR store for each of the specified queries.
- `get_s3_data(self, key: str, local_path: str) -> IOstream`
  - Returns the data from the S3 store associated with the given key.
