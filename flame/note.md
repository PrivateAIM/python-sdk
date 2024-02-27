# flame-python-sdk

# Processes:
- read and check tokens for
  - message broker
  - data api
  - storage minio api
- send data to message broker
- listen to message broker
- access the data api 
- getting old information from the storage minio api
- protocols for federated execution
  - await parameter updates
  - update parameters
  - reexecution
  - check if aggregation node (specified in config)
  - aggregation function
  
# Workflows:
at startup check if (also) aggregation node 

### Analysis node
1. read/check tokens (message broker, data api, storage api)
2. perform analysis
3. send parameters to message broker
4. if federated mode and not last epoch, await aggregated parameters
5. if federated mode and not last epoch go to 2.

### Aggregation node (if federated mode)
1. read/check tokens (message broker, storage api?)
2. await parameter updates 
3. aggregate parameters
4. send aggregated parameters to message broker
5. send final result to hub

# Tasks:
1. Data source access
2. Result submission
3. Send meta information through the Node API to the message broker to other nodes
4. Federation Protocols Implementation

# Project structure
src\
|__main.py (check execution mode: analysis/aggregation)\
|\
|__utils\
|&emsp;|__token.py\
|&emsp;&emsp;&emsp;|__def read_token() / receive_token()\
|&emsp;&emsp;&emsp;|__def check_token() / test_token()\
|\
|__clients\
|&emsp;|__minio_client.py\
|&emsp;|__data_api_client.py\
|&emsp;|__message_broker_client.py\
|\
|__federated\
|&emsp;|__aggregator_server.py (for Aggregation node)\
|&emsp;|&emsp;|__class Aggregator()\
|&emsp;|&emsp;&emsp;&emsp;|__def execute()\
|&emsp;|&emsp;&emsp;&emsp;|__def _await_parameters()\
|&emsp;|&emsp;&emsp;&emsp;|__def _aggregate_parameters()\
|&emsp;|&emsp;&emsp;&emsp;|__def _send_parameters()\
|&emsp;|\
|&emsp;|__analysis_client.py (for Analysis node)\
|&emsp;&emsp;&emsp;|__class AnalysisClient()\
|&emsp;&emsp;&emsp;&emsp;&emsp;|__def send_results()\
|&emsp;&emsp;&emsp;&emsp;&emsp;|__def await_aggregation()\
|\
|__protocols\
|&emsp;|__aggregation_paillier.py\
|&emsp;&emsp;&emsp;|__def aggregate()\
|&emsp;&emsp;&emsp;|__def _load_keys.py\
|&emsp;&emsp;&emsp;|__def _encrypt()\
|&emsp;&emsp;&emsp;|__def _enc_add()\
|&emsp;&emsp;&emsp;|__def _invmod()\
|\
|__templates\
&emsp;&emsp;|__main_generic.py\
&emsp;&emsp;|__aggregator_generic.py\
&emsp;&emsp;|&emsp;|__def aggregate()\
&emsp;&emsp;|__analyzer_generic.py()\
&emsp;&emsp;&emsp;&emsp;|__def main()


