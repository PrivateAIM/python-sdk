import pytest

from flamesdk.resources.client_apis.clients.storage_client import EXT_TO_OUTPUT_TYPE
from flamesdk.resources.client_apis.storage_api import StorageAPI
from flamesdk.resources.utils.constants import LogTypeLiteral
from flamesdk.resources.utils.logging import FlameLogger


class _RaisingFlameLogger(FlameLogger):
    """FlameLogger.raise_error only logs and then sleeps (fail-slow, so the
    platform can reap the container). Surface the error here instead, so the
    `pytest.raises` cases assert the validation rather than blocking."""

    def __init__(self):
        super().__init__(silent=True)

    def raise_error(
        self, message: str, hidden_error_msg=None, seconds: int = 1000
    ) -> None:
        raise ValueError(message)


class _MockStorageClient:
    def __init__(self):
        self.calls: list[dict] = []

    def push_result(self, result, **kwargs):
        self.calls.append({"result": result, **kwargs})
        return {"status": "success"}


@pytest.fixture()
def api_and_mock():
    flame_logger = _RaisingFlameLogger()
    api = StorageAPI.__new__(StorageAPI)
    api.flame_logger = flame_logger
    api.storage_client = _MockStorageClient()
    return api


def _capture_warnings(api):
    warnings: list[str] = []
    api.flame_logger.new_log = lambda msg, log_type=None, **kw: (
        warnings.append(msg) if log_type == LogTypeLiteral.WARNING.value else None
    )
    return warnings


# -- _warn_filename_extension unit tests --


@pytest.mark.parametrize(
    "filename,output_type",
    [
        ("result.pkl", "str"),
        ("result.pickle", "bytes"),
        ("result.txt", "pickle"),
        ("result.csv", "pickle"),
        ("result.json", "pickle"),
        ("result.bin", "str"),
        ("result.bin", "pickle"),
        ("result.unknown", "str"),
        ("result.unknown", "pickle"),
    ],
)
def test_warn_on_mismatch(api_and_mock, filename, output_type):
    api = api_and_mock
    warnings = _capture_warnings(api)
    api._warn_filename_extension(filename, output_type)
    assert len(warnings) == 1, f"Expected 1 warning, got {warnings}"
    assert "output_type" in warnings[0]


@pytest.mark.parametrize(
    "filename,output_type",
    [
        ("result.pkl", "pickle"),
        ("result.pickle", "pickle"),
        ("result.txt", "str"),
        ("result.csv", "str"),
        ("result.json", "str"),
        ("result.bin", "bytes"),
        ("result_no_ext", "bytes"),
    ],
)
def test_no_warn_on_match_or_unknown(api_and_mock, filename, output_type):
    api = api_and_mock
    warnings = _capture_warnings(api)
    api._warn_filename_extension(filename, output_type)
    assert warnings == []


# -- EXT_TO_OUTPUT_TYPE shape --


def test_ext_to_output_type_map_coverage():
    for output_type, exts in EXT_TO_OUTPUT_TYPE.items():
        assert output_type in (
            "str",
            "bytes",
            "pickle",
        ), f"Unknown type {output_type!r}"
        assert (
            isinstance(exts, list) and exts
        ), f"Expected non-empty list for {output_type!r}"
        for ext in exts:
            assert ext.startswith("."), f"Extension {ext!r} should start with '.'"


# -- submit_final_result: single --


def test_submit_single_result_warns_mismatch(api_and_mock):
    api = api_and_mock
    warnings = _capture_warnings(api)
    api.submit_final_result("hello", output_type="pickle", filename="result.txt")
    assert any("output_type" in w for w in warnings)


def test_submit_single_result_no_warn_on_match(api_and_mock):
    api = api_and_mock
    warnings = _capture_warnings(api)
    api.submit_final_result("hello", output_type="str", filename="result.txt")
    assert [w for w in warnings if "expected extensions" in w] == []


def test_submit_single_result_passes_filename_through(api_and_mock):
    api = api_and_mock
    api.submit_final_result("hello", output_type="str", filename="result.txt")
    assert api.storage_client.calls == [
        {
            "result": "hello",
            "type": "final",
            "output_type": "str",
            "filename": "result.txt",
            "local_dp": None,
        }
    ]


def test_submit_single_with_filename_list_warns_and_uses_first(api_and_mock):
    api = api_and_mock
    warnings = _capture_warnings(api)
    api.submit_final_result("hello", output_type="str", filename=["a.txt", "b.txt"])
    assert any("single result submission" in w for w in warnings)
    assert api.storage_client.calls[0]["filename"] == "a.txt"


# -- submit_final_result: multiple, with filename list --


def test_submit_multiple_results_with_filename_list(api_and_mock):
    api = api_and_mock
    api.submit_final_result(
        ["a", "b"],
        output_type="str",
        multiple_results=True,
        filename=["out_0.csv", "out_1.json"],
    )
    names = [c["filename"] for c in api.storage_client.calls]
    assert names == ["out_0.csv", "out_1.json"]


def test_submit_multiple_results_warns_per_item(api_and_mock):
    api = api_and_mock
    warnings = _capture_warnings(api)
    api.submit_final_result(
        ["a", "b"],
        output_type="pickle",
        multiple_results=True,
        filename=["result_0.txt", "result_1.csv"],
    )
    ext_warnings = [w for w in warnings if "expected extensions" in w]
    assert len(ext_warnings) == 2


def test_submit_multiple_results_no_warn_on_match(api_and_mock):
    api = api_and_mock
    warnings = _capture_warnings(api)
    api.submit_final_result(
        ["a", "b"],
        output_type="str",
        multiple_results=True,
        filename=["out_0.csv", "out_1.json"],
    )
    ext_warnings = [w for w in warnings if "expected extensions" in w]
    assert ext_warnings == []


# -- submit_final_result: multiple, with single-string filename base --


def test_submit_multiple_results_string_filename_no_ext(api_and_mock):
    api = api_and_mock
    api.submit_final_result(
        ["a", "b", "c"],
        output_type="str",
        multiple_results=True,
        filename="result",
    )
    names = [c["filename"] for c in api.storage_client.calls]
    assert names == ["result_1.txt", "result_2.txt", "result_3.txt"]


def test_submit_multiple_results_string_filename_with_ext_keeps_ext(api_and_mock):
    """Regression: filename with extension must preserve it across all items."""
    api = api_and_mock
    api.submit_final_result(
        ["a", "b", "c"],
        output_type="str",
        multiple_results=True,
        filename="result.csv",
    )
    names = [c["filename"] for c in api.storage_client.calls]
    assert names == ["result_1.csv", "result_2.csv", "result_3.csv"]


def test_submit_multiple_results_string_filename_with_mismatched_ext_uses_default(
    api_and_mock,
):
    api = api_and_mock
    api.submit_final_result(
        ["a", "b"],
        output_type="pickle",
        multiple_results=True,
        filename="result.csv",
    )
    names = [c["filename"] for c in api.storage_client.calls]
    # ".csv" is not in EXT_TO_OUTPUT_TYPE['pickle'] → fallback to canonical
    assert names == ["result_1.pkl", "result_2.pkl"]


# -- submit_final_result: multiple_results=True but result is not a list/tuple --


def test_multiple_results_with_non_list_result_warns_and_falls_back(api_and_mock):
    api = api_and_mock
    warnings = _capture_warnings(api)
    api.submit_final_result(
        "scalar",
        output_type="str",
        multiple_results=True,
        filename="result.txt",
    )
    assert any("multiple_results will be ignored" in w for w in warnings)
    assert len(api.storage_client.calls) == 1
    call = api.storage_client.calls[0]
    assert call["output_type"] == "str"
    assert call["filename"] == "result.txt"


def test_multiple_results_non_list_with_list_args_collapses(api_and_mock):
    """When result isn't a list, list-typed output_type/filename must collapse to scalar."""
    api = api_and_mock
    _capture_warnings(api)
    api.submit_final_result(
        "scalar",
        output_type=["str", "str"],
        multiple_results=True,
        filename=["a.txt", "b.txt"],
    )
    assert len(api.storage_client.calls) == 1
    call = api.storage_client.calls[0]
    assert isinstance(call["output_type"], str)
    assert call["output_type"] == "str"
    assert isinstance(call["filename"], str)


# -- _check_multi_result_validity --


def test_invalid_output_type_raises(api_and_mock):
    api = api_and_mock
    with pytest.raises(Exception):
        api._check_multi_result_validity(
            multiple_result=False,
            output_type="not_a_type",
            filename=None,
            result_length=1,
        )


def test_invalid_output_type_in_list_raises(api_and_mock):
    api = api_and_mock
    with pytest.raises(Exception):
        api._check_multi_result_validity(
            multiple_result=True,
            output_type=["str", "bogus"],
            filename=None,
            result_length=2,
        )


def test_output_type_short_list_raises(api_and_mock):
    api = api_and_mock
    with pytest.raises(Exception):
        api._check_multi_result_validity(
            multiple_result=True,
            output_type=["str", "str"],
            filename=None,
            result_length=3,
        )


def test_uniform_short_output_type_list_raises(api_and_mock):
    """A too-short list is rejected whether or not its elements are uniform."""
    api = api_and_mock
    with pytest.raises(Exception):
        api._check_multi_result_validity(
            multiple_result=True,
            output_type=["str", "str"],
            filename=None,
            result_length=3,
        )


def test_uniform_short_filename_list_raises(api_and_mock):
    api = api_and_mock
    with pytest.raises(Exception):
        api._check_multi_result_validity(
            multiple_result=True,
            output_type="str",
            filename=["a.txt", "a.txt"],
            result_length=3,
        )


def test_output_type_long_list_truncates(api_and_mock):
    api = api_and_mock
    warnings = _capture_warnings(api)
    output_type, filename = api._check_multi_result_validity(
        multiple_result=True,
        output_type=["str", "str", "pickle", "bytes"],
        filename=None,
        result_length=2,
    )
    assert output_type == ["str", "str"]
    assert filename is None
    assert any("larger than result list length" in w for w in warnings)


def test_uniform_list_collapses_to_scalar(api_and_mock):
    api = api_and_mock
    _capture_warnings(api)
    output_type, filename = api._check_multi_result_validity(
        multiple_result=True,
        output_type=["str", "str", "str"],
        filename=["x.txt", "x.txt", "x.txt"],
        result_length=3,
    )
    assert output_type == "str"
    assert filename == "x.txt"


def test_single_result_with_list_args_reduces(api_and_mock):
    api = api_and_mock
    warnings = _capture_warnings(api)
    output_type, filename = api._check_multi_result_validity(
        multiple_result=False,
        output_type=["str", "pickle"],
        filename=["a.txt", "b.txt"],
        result_length=1,
    )
    assert output_type == "str"
    assert filename == "a.txt"
    assert any("single result submission" in w for w in warnings)


# -- Checkpoints --


class _MockTaggedStorageClient:
    """Simulates the local tag storage: appends versions per tag, retrieves and deletes by tag."""

    def __init__(self):
        self.store: dict[str, list] = {}

    def push_result(self, result, tag=None, type=None, **kwargs):
        assert type == "local"
        assert tag is not None
        self.store.setdefault(tag, []).append(result)
        return {"status": "success", "url": f"http://x/local/{tag}", "id": tag}

    def get_intermediate_data(self, tag=None, type=None, tag_option="all", **kwargs):
        assert type == "local"
        items = self.store.get(tag, [])
        if tag_option == "last":
            return items[-1:]
        if tag_option == "first":
            return items[:1]
        return list(items)

    def delete_local_tag(self, tag):
        existed = tag in self.store
        self.store.pop(tag, None)
        return {"status": "success" if existed else "not_found", "tag": tag}

    def get_local_tags(self, filter=None):
        return [t for t in self.store if (filter is None or filter in t)]


@pytest.fixture()
def api_and_tagmock():
    flame_logger = _RaisingFlameLogger()
    api = StorageAPI.__new__(StorageAPI)
    api.flame_logger = flame_logger
    api.storage_client = _MockTaggedStorageClient()
    return api


@pytest.mark.skip(
    reason="StorageAPI has no named-checkpoint API. Checkpoints live on "
    "FlameCoreSDK as index-based set_checkpoint/load_checkpoint; this "
    "name+metadata API is not implemented yet."
)
def test_save_checkpoint_writes_data_and_meta_tags(api_and_tagmock):
    api = api_and_tagmock
    api.save_checkpoint({"w": 1}, "round-1", metadata={"iteration": 3})
    assert api.storage_client.store["checkpoint-round-1"] == [{"w": 1}]
    meta = api.storage_client.store["checkpoint-round-1-meta"][0]
    assert meta["checkpoint"] == "round-1"
    assert meta["iteration"] == 3
    assert "created_at" in meta


@pytest.mark.skip(
    reason="StorageAPI has no named-checkpoint API. Checkpoints live on "
    "FlameCoreSDK as index-based set_checkpoint/load_checkpoint; this "
    "name+metadata API is not implemented yet."
)
def test_load_checkpoint_returns_latest_payload(api_and_tagmock):
    api = api_and_tagmock
    api.save_checkpoint({"v": 1}, "state")
    api.save_checkpoint({"v": 2}, "state")
    assert api.load_checkpoint("state") == {"v": 2}
    assert api.load_checkpoint("state", version="first") == {"v": 1}
    assert api.load_checkpoint("state", version="all") == [{"v": 1}, {"v": 2}]


@pytest.mark.skip(
    reason="StorageAPI has no named-checkpoint API. Checkpoints live on "
    "FlameCoreSDK as index-based set_checkpoint/load_checkpoint; this "
    "name+metadata API is not implemented yet."
)
def test_load_missing_checkpoint_raises(api_and_tagmock):
    api = api_and_tagmock
    with pytest.raises(Exception):
        api.load_checkpoint("does-not-exist")


@pytest.mark.skip(
    reason="StorageAPI has no named-checkpoint API. Checkpoints live on "
    "FlameCoreSDK as index-based set_checkpoint/load_checkpoint; this "
    "name+metadata API is not implemented yet."
)
def test_get_checkpoint_metadata_without_loading_payload(api_and_tagmock):
    api = api_and_tagmock
    api.save_checkpoint(list(range(1000)), "big", metadata={"note": "hi"})
    meta = api.get_checkpoint_metadata("big")
    assert meta["note"] == "hi"
    assert meta["checkpoint"] == "big"
    assert api.get_checkpoint_metadata("missing") is None


@pytest.mark.skip(
    reason="StorageAPI has no named-checkpoint API. Checkpoints live on "
    "FlameCoreSDK as index-based set_checkpoint/load_checkpoint; this "
    "name+metadata API is not implemented yet."
)
def test_delete_checkpoint_removes_both_tags(api_and_tagmock):
    api = api_and_tagmock
    api.save_checkpoint({"v": 1}, "temp")
    api.delete_checkpoint("temp")
    assert "checkpoint-temp" not in api.storage_client.store
    assert "checkpoint-temp-meta" not in api.storage_client.store


@pytest.mark.skip(
    reason="StorageAPI has no named-checkpoint API. Checkpoints live on "
    "FlameCoreSDK as index-based set_checkpoint/load_checkpoint; this "
    "name+metadata API is not implemented yet."
)
def test_list_checkpoints_excludes_meta_tags(api_and_tagmock):
    api = api_and_tagmock
    api.save_checkpoint(1, "alpha")
    api.save_checkpoint(2, "beta")
    assert sorted(api.list_checkpoints()) == ["alpha", "beta"]


@pytest.mark.skip(
    reason="StorageAPI has no named-checkpoint API. Checkpoints live on "
    "FlameCoreSDK as index-based set_checkpoint/load_checkpoint; this "
    "name+metadata API is not implemented yet."
)
@pytest.mark.parametrize(
    "name",
    [
        "Bad_Name",  # uppercase + underscore
        "-leading",  # leading hyphen
        "trailing-",  # trailing hyphen
        "double--hyphen",  # repeated hyphen
        "ends-meta",  # reserved meta suffix
        "a-very-long-checkpoint-name-that-overflows",  # exceeds tag length limit
    ],
)
def test_invalid_checkpoint_name_raises(api_and_tagmock, name):
    api = api_and_tagmock
    with pytest.raises(Exception):
        api.save_checkpoint({"v": 1}, name)
