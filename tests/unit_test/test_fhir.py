import ast
from io import StringIO

import pytest

from flamesdk.resources.utils.fhir import (
    fhir_to_csv,
    _dict_to_csv,
    _search_fhir_resource,
)
from flamesdk.resources.utils.logging import FlameLogger


@pytest.fixture
def logger():
    return FlameLogger()


# ---------------------------------------------------------------------------
# Minimal FHIR fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def observation_data():
    """Small Observation bundle with two entries, each having a simple value."""
    return {
        "id": "test-bundle",
        "type": "searchset",
        "total": 2,
        "entry": [
            {
                "resource": {
                    "resourceType": "Observation",
                    "subject": {"reference": "Patient/P1"},
                    "code": {"coding": [{"code": "weight"}]},
                    "valueQuantity": {"value": 70.0, "unit": "kg"},
                }
            },
            {
                "resource": {
                    "resourceType": "Observation",
                    "subject": {"reference": "Patient/P2"},
                    "code": {"coding": [{"code": "weight"}]},
                    "valueQuantity": {"value": 85.5, "unit": "kg"},
                }
            },
        ],
    }


@pytest.fixture
def observation_component_data():
    """Observation bundle with component arrays matching real FHIR gene data.

    Each entry has a ``component`` list with two elements:
    - one holding ``valueCodeableConcept`` (gene ID) but *no* ``valueQuantity``
    - one holding ``valueQuantity`` (expression count) but *no* ``valueCodeableConcept``

    This mirrors the structure seen in production where searching for
    ``component.valueQuantity`` logs a harmless warning on the first component
    before finding the value in the second.
    """
    return {
        "id": "test-component",
        "type": "searchset",
        "total": 3,
        "entry": [
            {
                "resource": {
                    "resourceType": "Observation",
                    "subject": {"reference": "Patient/P1"},
                    "code": {"coding": [{"code": "69548-6"}]},
                    "component": [
                        {
                            "code": {
                                "coding": [
                                    {"system": "http://loinc.org", "code": "48018-6"}
                                ]
                            },
                            "valueCodeableConcept": {
                                "coding": [
                                    {
                                        "system": "http://ensembl.org",
                                        "code": "ENSG00000000003",
                                    }
                                ]
                            },
                        },
                        {
                            "code": {
                                "coding": [
                                    {"system": "http://loinc.org", "code": "48003-8"}
                                ]
                            },
                            "valueQuantity": {"value": 1192, "unit": "count"},
                        },
                    ],
                }
            },
            {
                "resource": {
                    "resourceType": "Observation",
                    "subject": {"reference": "Patient/P1"},
                    "code": {"coding": [{"code": "69548-6"}]},
                    "component": [
                        {
                            "code": {
                                "coding": [
                                    {"system": "http://loinc.org", "code": "48018-6"}
                                ]
                            },
                            "valueCodeableConcept": {
                                "coding": [
                                    {
                                        "system": "http://ensembl.org",
                                        "code": "ENSG00000000005",
                                    }
                                ]
                            },
                        },
                        {
                            "code": {
                                "coding": [
                                    {"system": "http://loinc.org", "code": "48003-8"}
                                ]
                            },
                            "valueQuantity": {"value": 754, "unit": "count"},
                        },
                    ],
                }
            },
            {
                "resource": {
                    "resourceType": "Observation",
                    "subject": {"reference": "Patient/P2"},
                    "code": {"coding": [{"code": "69548-6"}]},
                    "component": [
                        {
                            "code": {
                                "coding": [
                                    {"system": "http://loinc.org", "code": "48018-6"}
                                ]
                            },
                            "valueCodeableConcept": {
                                "coding": [
                                    {
                                        "system": "http://ensembl.org",
                                        "code": "ENSG00000000003",
                                    }
                                ]
                            },
                        },
                        {
                            "code": {
                                "coding": [
                                    {"system": "http://loinc.org", "code": "48003-8"}
                                ]
                            },
                            "valueQuantity": {"value": 980, "unit": "count"},
                        },
                    ],
                }
            },
        ],
    }


@pytest.fixture
def observation_simple_data():
    """Observation bundle with simple top-level valueQuantity (no component)."""
    return {
        "id": "test-simple",
        "type": "searchset",
        "total": 2,
        "entry": [
            {
                "resource": {
                    "resourceType": "Observation",
                    "subject": {"reference": "Patient/P1"},
                    "code": {"coding": [{"code": "weight"}]},
                    "valueQuantity": {"value": 70.0, "unit": "kg"},
                }
            },
            {
                "resource": {
                    "resourceType": "Observation",
                    "subject": {"reference": "Patient/P1"},
                    "code": {"coding": [{"code": "height"}]},
                    "valueQuantity": {"value": 175.0, "unit": "cm"},
                }
            },
        ],
    }


@pytest.fixture
def questionnaire_data():
    """Small QuestionnaireResponse bundle."""
    return {
        "id": "test-qr",
        "type": "searchset",
        "total": 2,
        "entry": [
            {
                "resource": {
                    "resourceType": "QuestionnaireResponse",
                    "item": [
                        {"linkId": "age", "answer": [{"valueDecimal": 25.0}]},
                        {"linkId": "sex", "answer": [{"valueString": "F"}]},
                    ],
                }
            },
            {
                "resource": {
                    "resourceType": "QuestionnaireResponse",
                    "item": [
                        {"linkId": "age", "answer": [{"valueDecimal": 40.0}]},
                        {"linkId": "sex", "answer": [{"valueString": "M"}]},
                    ],
                }
            },
        ],
    }


# ---------------------------------------------------------------------------
# _search_fhir_resource
# ---------------------------------------------------------------------------


class TestSearchFhirResource:
    def test_simple_dict_lookup(self, logger):
        entry = {"resource": {"subject": {"reference": "Patient/P1"}}}
        result = _search_fhir_resource(
            entry, logger, "resource.subject.reference".split(".")
        )
        assert result == "Patient/P1"

    def test_list_key_sequence(self, logger):
        """Pre-split list should behave identically to a string key sequence."""
        entry = {"resource": {"subject": {"reference": "Patient/P1"}}}
        result = _search_fhir_resource(
            entry, logger, ["resource", "subject", "reference"]
        )
        assert result == "Patient/P1"

    def test_traverse_list(self, logger):
        entry = {"coding": [{"code": "ABC"}, {"code": "DEF"}]}
        result = _search_fhir_resource(entry, logger, "coding.code".split("."))
        assert result == "ABC"

    def test_missing_key_returns_none(self, logger):
        entry = {"resource": {"status": "final"}}
        result = _search_fhir_resource(
            entry, logger, "resource.subject.reference".split(".")
        )
        assert result is None

    def test_partial_key_match_at_leaf(self, logger):
        """The leaf-level fallback matches 'value' against 'valueDecimal'."""
        entry = {"answer": [{"valueDecimal": 3.14}]}
        result = _search_fhir_resource(entry, logger, "answer.value".split("."))
        assert result == 3.14

    def test_with_current_offset(self, logger):
        """When current > 0, earlier keys are skipped."""
        item = {"linkId": "age", "answer": [{"valueDecimal": 25.0}]}
        keys = ["resource", "item", "linkId"]
        result = _search_fhir_resource(item, logger, keys, current=2)
        assert result == "age"

    def test_non_dict_non_list_returns_none(self, logger):
        result = _search_fhir_resource("some_string", logger, ["key"])
        assert result is None

    def test_component_skips_missing_key(self, logger):
        """When a list has elements with different keys, the search skips
        elements that don't match and returns the first hit."""
        entry = {
            "component": [
                {"valueCodeableConcept": {"coding": [{"code": "ENSG001"}]}},
                {"valueQuantity": {"value": 42}},
            ]
        }
        result = _search_fhir_resource(
            entry, logger, "component.valueQuantity.value".split(".")
        )
        assert result == 42

    def test_component_returns_first_match_from_list(self, logger):
        """Verify we get valueCodeableConcept from the first component."""
        entry = {
            "component": [
                {"valueCodeableConcept": {"coding": [{"code": "ENSG001"}]}},
                {"valueQuantity": {"value": 42}},
            ]
        }
        result = _search_fhir_resource(
            entry, logger, "component.valueCodeableConcept.coding.code".split(".")
        )
        assert result == "ENSG001"


# ---------------------------------------------------------------------------
# _dict_to_csv
# ---------------------------------------------------------------------------


class TestDictToCsv:
    def test_basic_output(self, logger):
        data = {"col_a": {"row1": "v1", "row2": "v2"}, "col_b": {"row1": "v3"}}
        result = _dict_to_csv(
            data, row_col_name="id", separator=",", flame_logger=logger
        )
        assert isinstance(result, StringIO)
        content = result.read()
        lines = content.split("\n")
        assert lines[0] == "id,col_a,col_b"
        # row1 has values in both cols
        assert "row1,v1,v3" in lines
        # row2 only in col_a, col_b should be empty
        assert "row2,v2," in lines

    def test_empty_data(self, logger):
        result = _dict_to_csv({}, row_col_name="", separator=",", flame_logger=logger)
        content = result.read()
        assert content == ""

    def test_custom_separator(self, logger):
        data = {"col": {"row": "val"}}
        result = _dict_to_csv(
            data, row_col_name="id", separator=";", flame_logger=logger
        )
        content = result.read()
        assert "id;col" in content
        assert "row;val" in content

    def test_visited_rows_dedup(self, logger):
        """A row_id appearing in multiple columns should only produce one CSV row."""
        data = {"A": {"r1": "1", "r2": "2"}, "B": {"r1": "3", "r2": "4"}}
        result = _dict_to_csv(data, row_col_name="", separator=",", flame_logger=logger)
        content = result.read()
        lines = content.strip().split("\n")
        # header + 2 data rows (r1 and r2), no duplicates
        assert len(lines) == 3


# ---------------------------------------------------------------------------
# fhir_to_csv — Observation
# ---------------------------------------------------------------------------


class TestFhirToCsvObservation:
    def test_basic_observation(self, logger, observation_data):
        result = fhir_to_csv(
            fhir_data=observation_data,
            col_key_seq="resource.subject.reference",
            row_key_seq="resource.code.coding.code",
            value_key_seq="resource.valueQuantity.value",
            input_resource="Observation",
            flame_logger=logger,
        )
        assert isinstance(result, StringIO)
        content = result.read()
        assert "Patient/P1" in content
        assert "Patient/P2" in content
        assert "70.0" in content
        assert "85.5" in content

    def test_dict_output(self, logger, observation_data):
        result = fhir_to_csv(
            fhir_data=observation_data,
            col_key_seq="resource.subject.reference",
            row_key_seq="resource.code.coding.code",
            value_key_seq="resource.valueQuantity.value",
            input_resource="Observation",
            flame_logger=logger,
            output_type="dict",
        )
        assert isinstance(result, dict)
        assert "Patient/P1" in result
        assert "Patient/P2" in result

    def test_component_observation(self, logger, observation_component_data):
        """Real-world pattern: component array with mixed value types.

        The search for ``component.valueQuantity`` must skip the first
        component (which only has ``valueCodeableConcept``) and find the
        value in the second component.
        """
        result = fhir_to_csv(
            fhir_data=observation_component_data,
            col_key_seq="resource.subject.reference",
            row_key_seq="resource.component.valueCodeableConcept.coding.code",
            value_key_seq="resource.component.valueQuantity.value",
            row_id_filters=["ENSG"],
            input_resource="Observation",
            flame_logger=logger,
            output_type="dict",
        )
        assert isinstance(result, dict)
        assert "Patient/P1" in result
        assert "Patient/P2" in result
        # Verify correct gene→value mapping
        assert result["Patient/P1"]["ENSG00000000003"] == 1192
        assert result["Patient/P1"]["ENSG00000000005"] == 754
        assert result["Patient/P2"]["ENSG00000000003"] == 980

    def test_component_observation_csv(self, logger, observation_component_data):
        """CSV output from component observations should have proper structure."""
        result = fhir_to_csv(
            fhir_data=observation_component_data,
            col_key_seq="resource.subject.reference",
            row_key_seq="resource.component.valueCodeableConcept.coding.code",
            value_key_seq="resource.component.valueQuantity.value",
            row_id_filters=["ENSG"],
            input_resource="Observation",
            flame_logger=logger,
        )
        assert isinstance(result, StringIO)
        content = result.read()
        lines = content.split("\n")
        # header + 2 gene rows (ENSG003 and ENSG005)
        assert len(lines) == 3
        assert "Patient/P1" in lines[0]
        assert "Patient/P2" in lines[0]
        assert "ENSG00000000003" in content
        assert "ENSG00000000005" in content

    def test_row_id_filter(self, logger, observation_component_data):
        result = fhir_to_csv(
            fhir_data=observation_component_data,
            col_key_seq="resource.subject.reference",
            row_key_seq="resource.component.valueCodeableConcept.coding.code",
            value_key_seq="resource.component.valueQuantity.value",
            input_resource="Observation",
            flame_logger=logger,
            row_id_filters=["ENSG00000000003"],
            output_type="dict",
        )
        # Only rows matching ENSG00000000003 should be present
        for col_rows in result.values():
            for row_id in col_rows:
                assert "ENSG00000000003" in row_id

    def test_col_id_filter(self, logger, observation_component_data):
        """col_id_filters is only evaluated when row_id_filters is None."""
        result = fhir_to_csv(
            fhir_data=observation_component_data,
            col_key_seq="resource.subject.reference",
            row_key_seq="resource.component.valueCodeableConcept.coding.code",
            value_key_seq="resource.component.valueQuantity.value",
            input_resource="Observation",
            flame_logger=logger,
            col_id_filters=["P1"],
            output_type="dict",
        )
        assert "Patient/P1" in result
        assert "Patient/P2" not in result

    def test_simple_observation_no_component(self, logger, observation_simple_data):
        """Entries with top-level valueQuantity (no component array)."""
        result = fhir_to_csv(
            fhir_data=observation_simple_data,
            col_key_seq="resource.subject.reference",
            row_key_seq="resource.code.coding.code",
            value_key_seq="resource.valueQuantity.value",
            input_resource="Observation",
            flame_logger=logger,
            output_type="dict",
        )
        assert "Patient/P1" in result
        assert result["Patient/P1"]["weight"] == 70.0
        assert result["Patient/P1"]["height"] == 175.0


# ---------------------------------------------------------------------------
# fhir_to_csv — QuestionnaireResponse
# ---------------------------------------------------------------------------


class TestFhirToCsvQuestionnaire:
    def test_basic_questionnaire(self, logger, questionnaire_data):
        result = fhir_to_csv(
            fhir_data=questionnaire_data,
            col_key_seq="resource.item.linkId",
            value_key_seq="resource.item.answer.value",
            input_resource="QuestionnaireResponse",
            flame_logger=logger,
        )
        assert isinstance(result, StringIO)
        content = result.read()
        assert "age" in content
        assert "sex" in content

    def test_questionnaire_dict_output(self, logger, questionnaire_data):
        result = fhir_to_csv(
            fhir_data=questionnaire_data,
            col_key_seq="resource.item.linkId",
            value_key_seq="resource.item.answer.value",
            input_resource="QuestionnaireResponse",
            flame_logger=logger,
            output_type="dict",
        )
        assert isinstance(result, dict)
        assert "age" in result
        assert "sex" in result
        # entry 0 → "0", entry 1 → "1"
        assert result["age"]["0"] == 25.0
        assert result["age"]["1"] == 40.0
        assert result["sex"]["0"] == "F"
        assert result["sex"]["1"] == "M"

    def test_questionnaire_col_filter(self, logger, questionnaire_data):
        result = fhir_to_csv(
            fhir_data=questionnaire_data,
            col_key_seq="resource.item.linkId",
            value_key_seq="resource.item.answer.value",
            input_resource="QuestionnaireResponse",
            flame_logger=logger,
            col_id_filters=["age"],
            output_type="dict",
        )
        assert "age" in result
        assert "sex" not in result


# ---------------------------------------------------------------------------
# Integration tests using the full JSON fixtures
# ---------------------------------------------------------------------------


class TestFhirIntegration:
    def test_observation_json(self, logger):
        with open("tests/unit_test/stream_observation.json", "r") as f:
            fhir_data = ast.literal_eval(f.read())
        result = fhir_to_csv(
            fhir_data,
            col_key_seq="resource.subject.reference",
            row_key_seq="resource.component.valueCodeableConcept.coding.code",
            value_key_seq="resource.component.valueQuantity.value",
            row_id_filters=["ENSG"],
            input_resource="Observation",
            flame_logger=logger,
        )
        assert isinstance(result, StringIO)
        content = result.read()
        assert len(content) > 0
        lines = content.split("\n")
        assert len(lines) > 1  # at least header + one data row

    def test_questionnaire_json(self, logger):
        with open("tests/unit_test/stream_ques.json", "r") as f:
            fhir_data = ast.literal_eval(f.read())
        result = fhir_to_csv(
            fhir_data,
            col_key_seq="resource.item.linkId",
            value_key_seq="resource.item.answer.value",
            input_resource="QuestionnaireResponse",
            flame_logger=logger,
        )
        assert isinstance(result, StringIO)
        content = result.read()
        assert len(content) > 0
        lines = content.split("\n")
        assert len(lines) > 1

    def test_observation_json_dict_output(self, logger):
        with open("tests/unit_test/stream_observation.json", "r") as f:
            fhir_data = ast.literal_eval(f.read())
        result = fhir_to_csv(
            fhir_data,
            col_key_seq="resource.subject.reference",
            row_key_seq="resource.component.valueCodeableConcept.coding.code",
            value_key_seq="resource.component.valueQuantity.value",
            row_id_filters=["ENSG"],
            input_resource="Observation",
            flame_logger=logger,
            output_type="dict",
        )
        assert isinstance(result, dict)
        assert len(result) > 0
        # All row keys should contain "ENSG" due to the filter
        for col_rows in result.values():
            for row_id in col_rows:
                assert "ENSG" in row_id
