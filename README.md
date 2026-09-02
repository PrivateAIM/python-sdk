# flamesdk — FLAME Python SDK

Python SDK for building federated analyses on the [FLAME/PrivateAIM](https://privateaim.net) platform.

An analysis container instantiates `FlameCoreSDK`, which bootstraps the connections to the platform's
sidecar services (MessageBroker, PO service, ResultService/Storage, DataAPI) and exposes methods for
inter-node messaging, result exchange and data source access.

## Installation

Install it into your analysis image straight from the repository:

```bash
pip install git+https://github.com/PrivateAIM/flame-python-sdk.git
```

When working on the SDK itself (dependencies are managed with Poetry, the virtualenv is kept
in-project via `poetry.toml`):

```bash
poetry install                  # with dev dependencies
poetry install --without dev    # runtime only
```

Runtime requirements: Python >= 3.9, `httpx`, `fastapi`, `uvicorn`.

## Getting started

Every FLAME analysis starts by connecting itself to the other components of the FLAME platform and
starting an analysis REST API. All of this is done by instantiating a `FlameCoreSDK` object.

```python
from flamesdk import FlameCoreSDK


def main():
    flame = FlameCoreSDK()

    # Your code here

    flame.analysis_finished()


if __name__ == "__main__":
    main()
```

`analysis_finished()` signals all participating nodes to finish and flags this node as `executed`,
which lets the platform shut the container down.

### Constructor options

```python
FlameCoreSDK(
    aggregator_requires_data: bool = False,
    default_requires_data: bool = True,
    stream_log_level: int = 20,
    silent: bool = False,
    status_sync: Optional[tuple[Literal['executed', 'stopped', 'failed']]] = ('executed', 'stopped', 'failed'),
)
```

- `aggregator_requires_data` — also connect the DataAPI on aggregator nodes (by default only nodes
  with `node_role == 'default'` connect to it).
- `default_requires_data` — if `False`, a default node whose DataAPI connection fails is accepted as
  a proxy node (its role is set to `"proxy"`) instead of failing startup.
- `stream_log_level` — minimum log level submitted to the hub (see [Logging](#logging)).
- `silent` — suppress console output.
- `status_sync` — terminal states the analysis REST API advertises to partner nodes.

### Startup sequence

Constructing `FlameCoreSDK()` performs the platform handshake in a fixed order:

1. Read the node config from the environment (see below).
2. Wait until the local nginx sidecar (`nginx-<DEPLOYMENT_NAME>`) is reachable.
3. Connect to MessageBroker → PO service → ResultService/Storage → DataAPI (the latter only when
   required, see `aggregator_requires_data`).
4. Start the `FlameAPI` (FastAPI + uvicorn) on a background thread for incoming messages, health
   checks, token refresh and partner status updates.
5. If all clients and the API thread came up, the analysis status is flipped to `executing`.

Each connection is wrapped individually — a partial startup is allowed, failures are logged and the
missing client is stored as `None`.

### Environment variables

Read by `NodeConfig` and injected by the platform:

| Variable | Purpose |
| --- | --- |
| `ANALYSIS_ID` | id of the analysis |
| `PROJECT_ID` | id of the project |
| `KEYCLOAK_TOKEN` | bearer token for the sidecar services |
| `DATA_SOURCE_TOKEN` | token for the data sources |
| `DEPLOYMENT_NAME` | used to derive the nginx sidecar host name |

`node_id`, `node_type` and `node_role` are not read from the environment — they are filled in by the
MessageBroker handshake.

## General methods

- `get_id() -> str` — node id of this node.
- `get_analysis_id() -> str` / `get_project_id() -> str`
- `get_type() -> Literal['default', 'aggregator']` — the node type. `"aggregator"` means the node may
  call `submit_final_result(...)`.
- `get_role() -> str` / `set_role(role) -> str` — the node role; equal to the node type right after
  startup unless set manually (or set to `"proxy"`, see `default_requires_data`).
- `get_participants() -> list[dict[str, str]]` — configs of all other participating nodes.
- `get_participant_ids() -> list[str]`
- `get_aggregator_id() -> Optional[str]` — node id of the node dedicated as aggregator.
- `get_self_node_index() -> int` / `get_node_index(node_id) -> Optional[int]` — index of a node in the
  alphanumerically sorted list of all analysis node ids.
- `partner_role_call(node_ids, max_attempts=1, timeout=None, attempt_timeout=10) -> dict[str, Optional[str]]`
  — ask the given partner nodes for their role (`None` per node on timeout/unknown id).
- `full_role_call(max_attempts=1, timeout=None, attempt_timeout=10) -> dict[str, Optional[str]]` —
  `partner_role_call` for all participants.
- `ready_check(nodes='all', attempt_interval=30, timeout=None) -> dict[str, bool]` — repeatedly ping
  the given nodes until they respond or the timeout is reached.
- `node_has_data() -> bool` — whether this node is connected to the DataAPI.
- `analysis_finished() -> bool` — tell all nodes to finish, then mark this node as `executed`.

### Logging

```python
flame.flame_log("training round 3 done")
flame.flame_log("connecting...", end='', halt_submission=True)
flame.flame_log("success", append=True)
```

`flame_log(msg, sep='', end='', log_type='info', append=False, halt_submission=False, hidden_error_msg=None)`
prints to the console and submits the log to the hub (logs created before the PO service connection
is up are queued). Log types are defined by `LogTypeLiteral` in
`flamesdk/resources/utils/constants.py`: `debug` (10), `info` (20), `notice` (25), `warn` (30),
`alert` (33), `emerg` (36), `error` (40), `crit` (50). Only logs at or above `stream_log_level` are
submitted. `log_type='error'` raises the error in addition to logging it; `hidden_error_msg` keeps a
stacktrace in the local log without submitting it to the hub. `halt_submission=True` defers
submission until the following log call, which is how `"…success"` / `"…failed"` end up on one line.

### Progress

- `get_progress() -> int`
- `set_progress(progress: Union[int, float]) -> None` — floats are truncated to integers; finishing
  the node sets it to 100.

### Checkpointing

- `set_checkpoint(kwargs: dict[str, Any], file_paths: Optional[list[str]] = None) -> None` — saves the
  given kwargs plus the diff of the working directory (files created since startup, extended by
  `file_paths`) to local storage under the tag `checkpoint-<n>-end`.
- `load_checkpoint(index: int) -> Optional[dict[str, Any]]` — restores the kwargs and recreates the
  saved files/directories. Returns `None` if the checkpoint does not exist or is ambiguous.

The `checkpoint-` tag prefix is reserved — `save_intermediate_data` refuses tags containing it.

### Analysis status

`AnalysisStatus` (`flamesdk/resources/utils/constants.py`) defines `starting`, `started`, `stuck`,
`stopping`, `stopped`, `executing`, `executed`, `failed`. Always pass `AnalysisStatus.<NAME>.value`
rather than a hardcoded string.

## Message Broker

The Message Broker is used for control flow and small data exchange between nodes. Volume data (ML
models, datasets) should be exchanged via the Result Service instead.

- `send_message(receivers, message_category, message, max_attempts=1, timeout=None, attempt_timeout=10) -> tuple[list[str], list[str]]`
  — returns the node ids that acknowledged and those that did not.
- `await_messages(senders, message_category, message_id=None, timeout=None) -> dict[str, Optional[list[Message]]]`
- `send_message_and_wait_for_responses(receivers, message_category, message, max_attempts=1, timeout=None, attempt_timeout=10) -> dict[str, Optional[list[Message]]]`
- `get_messages(status='unread') -> list[Message]`
- `delete_messages(message_ids) -> int`
- `clear_messages(status='read', min_age=None) -> int` — delete messages by status, optionally only
  those older than `min_age` seconds.

### Message structure

A `Message` carries the user payload in `message.body`. The `meta` field is reserved and added by the
SDK — a message body must not contain it.

```json
{
  "meta": {
    "type": "outgoing | incoming",
    "category": "str",
    "id": "str",
    "akn_id": "str | null",
    "status": "unread | read",
    "sender": "node_id",
    "created_at": "datetime",
    "arrived_at": "datetime | null",
    "number": "int"
  },
  "...": "your payload"
}
```

The message categories `role_call`, `role_call_answer`, `ready_check`, `analysis_finished` and
`intermediate_data` are used internally by the SDK.

## Result Service / Storage

Saves and exchanges results between the nodes of one analysis and locally between analyses of the
same project.

- `submit_final_result(result, output_type='str', multiple_results=False, filename=None, local_dp=None) -> Union[dict[str, str], list[dict[str, str]]]`
  — sends the final result to the hub for analysts to download. Only available on nodes whose
  `get_type()` is `"aggregator"`. `output_type` is `'str'`, `'bytes'` or `'pickle'` (or a list of
  those when `multiple_results=True`); `local_dp` takes
  `LocalDifferentialPrivacyParams({"epsilon": float, "sensitivity": float})` and applies to
  floating-point results only.
- `save_intermediate_data(data, location, remote_node_ids=None, tag=None) -> Optional[dict]` —
  `location='local'` stores in the node, `location='global'` stores in the central MinIO instance.
  Global saves require `remote_node_ids` (the recipients' public keys are used for encryption) and
  return one dict per recipient.
- `get_intermediate_data(location, query=None, tag=None, tag_option='all') -> Any` — `tag_option` is
  `'all'`, `'last'` or `'first'` when multiple entries share a tag.
- `get_local_tags(filter=None) -> list[str]`
- `send_intermediate_data(receivers, data, message_category='intermediate_data', max_attempts=1, timeout=None, attempt_timeout=10) -> tuple[list[str], list[str]]`
  — saves the data globally and notifies the receivers via the Message Broker.
- `await_intermediate_data(senders, message_category='intermediate_data', timeout=None) -> dict[str, Any]`
  — waits for such notifications and retrieves the referenced data; senders that did not deliver in
  time map to `None`.

```python
flame.send_intermediate_data(receivers=flame.get_participant_ids(), data={"weights": weights})
partner_data = flame.await_intermediate_data(senders=flame.get_participant_ids(), timeout=300)
```

## Data Source Client

Access to the FHIR and S3 stores linked to the project. All of these return `None` and log a warning
when the node is not connected to the DataAPI (check with `node_has_data()`).

- `get_data_sources() -> Optional[list[dict[str, Any]]]`
- `get_data_client(data_id) -> Optional[AsyncClient]` — raw httpx client for a specific store.
- `get_fhir_data(fhir_queries=[]) -> Optional[list[dict[str, Union[str, dict]]]]`
- `get_s3_data(s3_keys=[]) -> Optional[list[dict[str, bytes]]]`
- `fhir_to_csv(fhir_data, col_key_seq, value_key_seq, input_resource, row_key_seq=None, row_id_filters=None, col_id_filters=None, row_col_name='', separator=',', output_type='file') -> Optional[Union[StringIO, dict]]`
  — pivots a FHIR bundle into CSV (`output_type='file'` returns a `StringIO`, `'dict'` a nested dict).

## Analysis REST API

The background `FlameAPI` thread exposes:

| Route | Purpose |
| --- | --- |
| `GET /healthz` | health check, reports the current analysis status |
| `POST /webhook` | incoming messages from the Message Broker |
| `POST /partner_status` | partner node status updates |
| `POST /token_refresh` | refreshes the Keycloak token on all clients |

## Repository layout

```
flamesdk/
  flame_core.py                     # FlameCoreSDK — the public API
  resources/
    node_config.py                  # env var config, node id/type/role
    rest_api.py                     # FlameAPI (FastAPI)
    client_apis/
      message_broker_api.py         # high-level API layer: orchestration,
      storage_api.py                #   retries, timeouts, type conversion
      data_api.py
      po_api.py
      clients/*_client.py           # thin httpx transports per sidecar service
    utils/
      logging.py                    # FlameLogger
      constants.py                  # AnalysisStatus, LogTypeLiteral
      fhir.py, utils.py
tests/
  unit_test/                        # pytest unit tests
  test_images/                      # end-to-end scripts run inside containers
```

When adding a capability, extend the `*_api.py` layer and keep `clients/*_client.py` limited to
request/response plumbing.

## Development

```bash
poetry run pytest                                # unit tests (tests/test_images is ignored)
poetry run pytest tests/unit_test/test_util.py   # a single test module
poetry run ruff check .                          # lint
poetry run ruff format .                         # format
poetry run pre-commit run --all-files
```

Run pytest from the repository root — fixtures reference their data files relative to it.

Commits are checked by `conventional-pre-commit`, so use Conventional Commits (`feat:`, `fix:`,
`chore:`, `refactor:` …).

## License

Apache-2.0 — see [LICENSE](LICENSE).