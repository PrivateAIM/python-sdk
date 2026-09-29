import ast
from io import StringIO

import pytest

from flamesdk.resources.utils.fhir import fhir_to_csv as fhir_to_csv_old
from flamesdk.resources.utils.logging import FlameLogger

# The fhirpathpy-backed rewrite does not exist yet: neither
# flamesdk.resources.utils.fhir_fhirpathpy nor the fhirpathpy dependency is in
# the tree. Skip the module instead of failing collection for the whole suite.
fhir_fhirpathpy = pytest.importorskip(
    "flamesdk.resources.utils.fhir_fhirpathpy",
    reason="fhirpathpy-based fhir_to_csv rewrite is not implemented yet",
)

fhir_to_csv_new = fhir_fhirpathpy.fhir_to_csv
_extract_value = fhir_fhirpathpy._extract_value
_extract_all = fhir_fhirpathpy._extract_all


@pytest.fixture
def logger():
    return FlameLogger()


# ---------------------------------------------------------------------------
# Shared FHIR fixtures (same as test_fhir.py)
# ---------------------------------------------------------------------------


@pytest.fixture
def observation_data():
    return {
        "id": "test-bundle",
        "type": "searchset",
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
    return {
        "id": "test-component",
        "type": "searchset",
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
    return {
        "id": "test-simple",
        "type": "searchset",
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
    return {
        "id": "test-qr",
        "type": "searchset",
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
# _extract_value / _extract_all unit tests
# ---------------------------------------------------------------------------


class TestExtractValue:
    def test_simple_path(self):
        resource = {"subject": {"reference": "Patient/P1"}}
        assert _extract_value(resource, "subject.reference") == "Patient/P1"

    def test_list_traversal(self):
        resource = {"coding": [{"code": "ABC"}, {"code": "DEF"}]}
        assert _extract_value(resource, "coding.code") == "ABC"

    def test_missing_key_returns_none(self):
        resource = {"status": "final"}
        assert _extract_value(resource, "subject.reference") is None

    def test_nested_component(self):
        resource = {
            "component": [
                {"valueCodeableConcept": {"coding": [{"code": "ENSG001"}]}},
                {"valueQuantity": {"value": 42}},
            ]
        }
        assert _extract_value(resource, "component.valueQuantity.value") == 42

    def test_component_concept(self):
        resource = {
            "component": [
                {"valueCodeableConcept": {"coding": [{"code": "ENSG001"}]}},
                {"valueQuantity": {"value": 42}},
            ]
        }
        assert (
            _extract_value(resource, "component.valueCodeableConcept.coding.code")
            == "ENSG001"
        )


class TestExtractAll:
    def test_returns_all_matches(self):
        resource = {"coding": [{"code": "A"}, {"code": "B"}]}
        assert _extract_all(resource, "coding.code") == ["A", "B"]

    def test_empty_on_no_match(self):
        resource = {"status": "final"}
        assert _extract_all(resource, "subject.reference") == []

    def test_component_values(self):
        resource = {
            "component": [
                {"valueQuantity": {"value": 1}},
                {"valueQuantity": {"value": 2}},
            ]
        }
        assert _extract_all(resource, "component.valueQuantity.value") == [1, 2]


# ---------------------------------------------------------------------------
# fhir_to_csv — Observation (new implementation)
# ---------------------------------------------------------------------------


class TestFhirpathpyObservation:
    def test_basic_observation(self, logger, observation_data):
        result = fhir_to_csv_new(
            fhir_data=observation_data,
            col_key_seq="subject.reference",
            row_key_seq="code.coding.code",
            value_key_seq="valueQuantity.value",
            flame_logger=logger,
        )
        assert isinstance(result, StringIO)
        content = result.read()
        assert "Patient/P1" in content
        assert "Patient/P2" in content

    def test_dict_output(self, logger, observation_data):
        result = fhir_to_csv_new(
            fhir_data=observation_data,
            col_key_seq="subject.reference",
            row_key_seq="code.coding.code",
            value_key_seq="valueQuantity.value",
            flame_logger=logger,
            output_type="dict",
        )
        assert isinstance(result, dict)
        assert "Patient/P1" in result
        assert "Patient/P2" in result

    def test_component_observation(self, logger, observation_component_data):
        result = fhir_to_csv_new(
            fhir_data=observation_component_data,
            col_key_seq="subject.reference",
            row_key_seq="component.valueCodeableConcept.coding.code",
            value_key_seq="component.valueQuantity.value",
            row_id_filters=["ENSG"],
            flame_logger=logger,
            output_type="dict",
        )
        assert isinstance(result, dict)
        assert "Patient/P1" in result
        assert "Patient/P2" in result
        assert result["Patient/P1"]["ENSG00000000003"] == 1192
        assert result["Patient/P1"]["ENSG00000000005"] == 754
        assert result["Patient/P2"]["ENSG00000000003"] == 980

    def test_component_observation_csv(self, logger, observation_component_data):
        result = fhir_to_csv_new(
            fhir_data=observation_component_data,
            col_key_seq="subject.reference",
            row_key_seq="component.valueCodeableConcept.coding.code",
            value_key_seq="component.valueQuantity.value",
            row_id_filters=["ENSG"],
            flame_logger=logger,
        )
        assert isinstance(result, StringIO)
        content = result.read()
        lines = content.split("\n")
        assert len(lines) == 3
        assert "Patient/P1" in lines[0]
        assert "Patient/P2" in lines[0]
        assert "ENSG00000000003" in content
        assert "ENSG00000000005" in content

    def test_row_id_filter(self, logger, observation_component_data):
        result = fhir_to_csv_new(
            fhir_data=observation_component_data,
            col_key_seq="subject.reference",
            row_key_seq="component.valueCodeableConcept.coding.code",
            value_key_seq="component.valueQuantity.value",
            flame_logger=logger,
            row_id_filters=["ENSG00000000003"],
            output_type="dict",
        )
        for col_rows in result.values():
            for row_id in col_rows:
                assert "ENSG00000000003" in row_id

    def test_col_id_filter(self, logger, observation_component_data):
        result = fhir_to_csv_new(
            fhir_data=observation_component_data,
            col_key_seq="subject.reference",
            row_key_seq="component.valueCodeableConcept.coding.code",
            value_key_seq="component.valueQuantity.value",
            flame_logger=logger,
            col_id_filters=["P1"],
            output_type="dict",
        )
        assert "Patient/P1" in result
        assert "Patient/P2" not in result

    def test_simple_observation_no_component(self, logger, observation_simple_data):
        result = fhir_to_csv_new(
            fhir_data=observation_simple_data,
            col_key_seq="subject.reference",
            row_key_seq="code.coding.code",
            value_key_seq="valueQuantity.value",
            flame_logger=logger,
            output_type="dict",
        )
        assert "Patient/P1" in result
        # fhirpathpy returns Decimal for numeric values, convert for comparison
        assert float(result["Patient/P1"]["weight"]) == 70.0
        assert float(result["Patient/P1"]["height"]) == 175.0


# ---------------------------------------------------------------------------
# fhir_to_csv — QuestionnaireResponse (new implementation using items_path)
# ---------------------------------------------------------------------------


class TestFhirpathpyQuestionnaire:
    def test_basic_questionnaire(self, logger, questionnaire_data):
        result = fhir_to_csv_new(
            fhir_data=questionnaire_data,
            col_key_seq="linkId",
            value_key_seq="answer.children()",
            items_path="item",
            flame_logger=logger,
        )
        assert isinstance(result, StringIO)
        content = result.read()
        assert "age" in content
        assert "sex" in content

    def test_questionnaire_dict_output(self, logger, questionnaire_data):
        result = fhir_to_csv_new(
            fhir_data=questionnaire_data,
            col_key_seq="linkId",
            value_key_seq="answer.children()",
            items_path="item",
            flame_logger=logger,
            output_type="dict",
        )
        assert isinstance(result, dict)
        assert "age" in result
        assert "sex" in result
        assert float(result["age"]["0"]) == 25.0
        assert float(result["age"]["1"]) == 40.0
        assert result["sex"]["0"] == "F"
        assert result["sex"]["1"] == "M"

    def test_questionnaire_col_filter(self, logger, questionnaire_data):
        result = fhir_to_csv_new(
            fhir_data=questionnaire_data,
            col_key_seq="linkId",
            value_key_seq="answer.children()",
            items_path="item",
            flame_logger=logger,
            col_id_filters=["age"],
            output_type="dict",
        )
        assert "age" in result
        assert "sex" not in result


# ---------------------------------------------------------------------------
# Comparison tests: old vs new produce same results
# ---------------------------------------------------------------------------


class TestComparisonObservation:
    """Verify the new fhirpathpy implementation produces the same dict output
    as the original custom implementation for Observation resources."""

    def test_basic_observation_same_output(self, logger, observation_data):
        # Old implementation requires 'total' field
        observation_data["total"] = len(observation_data["entry"])
        old = fhir_to_csv_old(
            fhir_data=observation_data,
            col_key_seq="resource.subject.reference",
            row_key_seq="resource.code.coding.code",
            value_key_seq="resource.valueQuantity.value",
            input_resource="Observation",
            flame_logger=logger,
            output_type="dict",
        )
        new = fhir_to_csv_new(
            fhir_data=observation_data,
            col_key_seq="subject.reference",
            row_key_seq="code.coding.code",
            value_key_seq="valueQuantity.value",
            flame_logger=logger,
            output_type="dict",
        )
        assert set(old.keys()) == set(new.keys())
        for col in old:
            assert set(old[col].keys()) == set(new[col].keys())
            for row in old[col]:
                assert float(old[col][row]) == float(new[col][row])

    def test_component_observation_same_output(
        self, logger, observation_component_data
    ):
        observation_component_data["total"] = len(observation_component_data["entry"])
        old = fhir_to_csv_old(
            fhir_data=observation_component_data,
            col_key_seq="resource.subject.reference",
            row_key_seq="resource.component.valueCodeableConcept.coding.code",
            value_key_seq="resource.component.valueQuantity.value",
            row_id_filters=["ENSG"],
            input_resource="Observation",
            flame_logger=logger,
            output_type="dict",
        )
        new = fhir_to_csv_new(
            fhir_data=observation_component_data,
            col_key_seq="subject.reference",
            row_key_seq="component.valueCodeableConcept.coding.code",
            value_key_seq="component.valueQuantity.value",
            row_id_filters=["ENSG"],
            flame_logger=logger,
            output_type="dict",
        )
        assert set(old.keys()) == set(new.keys())
        for col in old:
            assert set(old[col].keys()) == set(new[col].keys())
            for row in old[col]:
                assert float(old[col][row]) == float(new[col][row])

    def test_simple_observation_same_output(self, logger, observation_simple_data):
        observation_simple_data["total"] = len(observation_simple_data["entry"])
        old = fhir_to_csv_old(
            fhir_data=observation_simple_data,
            col_key_seq="resource.subject.reference",
            row_key_seq="resource.code.coding.code",
            value_key_seq="resource.valueQuantity.value",
            input_resource="Observation",
            flame_logger=logger,
            output_type="dict",
        )
        new = fhir_to_csv_new(
            fhir_data=observation_simple_data,
            col_key_seq="subject.reference",
            row_key_seq="code.coding.code",
            value_key_seq="valueQuantity.value",
            flame_logger=logger,
            output_type="dict",
        )
        assert set(old.keys()) == set(new.keys())
        for col in old:
            assert set(old[col].keys()) == set(new[col].keys())
            for row in old[col]:
                assert float(old[col][row]) == float(new[col][row])


class TestComparisonQuestionnaire:
    """Verify the new fhirpathpy implementation produces the same dict output
    as the original custom implementation for QuestionnaireResponse resources.

    Note: The old API uses 'resource.item.answer.value' with partial key matching
    (value -> valueDecimal/valueString). The new API uses items_path='item' with
    'answer.children()' to achieve the same polymorphic value extraction.
    """

    def test_questionnaire_same_output(self, logger, questionnaire_data):
        questionnaire_data["total"] = len(questionnaire_data["entry"])
        old = fhir_to_csv_old(
            fhir_data=questionnaire_data,
            col_key_seq="resource.item.linkId",
            value_key_seq="resource.item.answer.value",
            input_resource="QuestionnaireResponse",
            flame_logger=logger,
            output_type="dict",
        )
        new = fhir_to_csv_new(
            fhir_data=questionnaire_data,
            col_key_seq="linkId",
            value_key_seq="answer.children()",
            items_path="item",
            flame_logger=logger,
            output_type="dict",
        )
        assert set(old.keys()) == set(new.keys())
        for col in old:
            assert set(old[col].keys()) == set(new[col].keys())
            for row in old[col]:
                # Handle type differences (Decimal vs float vs str)
                old_val = old[col][row]
                new_val = new[col][row]
                if isinstance(old_val, (int, float)):
                    assert float(old_val) == float(new_val)
                else:
                    assert str(old_val) == str(new_val)


class TestComparisonPerformance:
    """Benchmark old vs new implementation on the full JSON fixture files."""

    def test_observation_speed(self, logger):
        import time

        with open("tests/unit_test/stream_observation.json", "r") as f:
            fhir_data = ast.literal_eval(f.read())

        iterations = 5

        # Warm-up
        fhir_to_csv_old(
            fhir_data,
            col_key_seq="resource.subject.reference",
            row_key_seq="resource.component.valueCodeableConcept.coding.code",
            value_key_seq="resource.component.valueQuantity.value",
            row_id_filters=["ENSG"],
            input_resource="Observation",
            flame_logger=logger,
            output_type="dict",
        )
        fhir_to_csv_new(
            fhir_data,
            col_key_seq="subject.reference",
            row_key_seq="component.valueCodeableConcept.coding.code",
            value_key_seq="component.valueQuantity.value",
            row_id_filters=["ENSG"],
            flame_logger=logger,
            output_type="dict",
        )

        # Time old implementation
        start = time.perf_counter()
        for _ in range(iterations):
            fhir_to_csv_old(
                fhir_data,
                col_key_seq="resource.subject.reference",
                row_key_seq="resource.component.valueCodeableConcept.coding.code",
                value_key_seq="resource.component.valueQuantity.value",
                row_id_filters=["ENSG"],
                input_resource="Observation",
                flame_logger=logger,
                output_type="dict",
            )
        old_time = (time.perf_counter() - start) / iterations

        # Time new implementation
        start = time.perf_counter()
        for _ in range(iterations):
            fhir_to_csv_new(
                fhir_data,
                col_key_seq="subject.reference",
                row_key_seq="component.valueCodeableConcept.coding.code",
                value_key_seq="component.valueQuantity.value",
                row_id_filters=["ENSG"],
                flame_logger=logger,
                output_type="dict",
            )
        new_time = (time.perf_counter() - start) / iterations

        ratio = new_time / old_time
        print(
            f"\n  Observation benchmark ({len(fhir_data['entry'])} entries, {iterations} iterations):"
        )
        print(f"    Old: {old_time:.4f}s | New: {new_time:.4f}s | Ratio: {ratio:.2f}x")

    def test_questionnaire_speed(self, logger):
        import time

        with open("tests/unit_test/stream_ques.json", "r") as f:
            fhir_data = ast.literal_eval(f.read())

        iterations = 5

        # Warm-up
        fhir_to_csv_old(
            fhir_data,
            col_key_seq="resource.item.linkId",
            value_key_seq="resource.item.answer.value",
            input_resource="QuestionnaireResponse",
            flame_logger=logger,
            output_type="dict",
        )
        fhir_to_csv_new(
            fhir_data,
            col_key_seq="linkId",
            value_key_seq="answer.children()",
            items_path="item",
            flame_logger=logger,
            output_type="dict",
        )

        # Time old implementation
        start = time.perf_counter()
        for _ in range(iterations):
            fhir_to_csv_old(
                fhir_data,
                col_key_seq="resource.item.linkId",
                value_key_seq="resource.item.answer.value",
                input_resource="QuestionnaireResponse",
                flame_logger=logger,
                output_type="dict",
            )
        old_time = (time.perf_counter() - start) / iterations

        # Time new implementation
        start = time.perf_counter()
        for _ in range(iterations):
            fhir_to_csv_new(
                fhir_data,
                col_key_seq="linkId",
                value_key_seq="answer.children()",
                items_path="item",
                flame_logger=logger,
                output_type="dict",
            )
        new_time = (time.perf_counter() - start) / iterations

        ratio = new_time / old_time
        print(
            f"\n  QuestionnaireResponse benchmark ({len(fhir_data['entry'])} entries, {iterations} iterations):"
        )
        print(f"    Old: {old_time:.4f}s | New: {new_time:.4f}s | Ratio: {ratio:.2f}x")


class TestComparisonIntegration:
    """Compare old and new implementations on the full JSON fixture files."""

    def test_observation_json_same_output(self, logger):
        with open("tests/unit_test/stream_observation.json", "r") as f:
            fhir_data = ast.literal_eval(f.read())

        old = fhir_to_csv_old(
            fhir_data,
            col_key_seq="resource.subject.reference",
            row_key_seq="resource.component.valueCodeableConcept.coding.code",
            value_key_seq="resource.component.valueQuantity.value",
            row_id_filters=["ENSG"],
            input_resource="Observation",
            flame_logger=logger,
            output_type="dict",
        )
        new = fhir_to_csv_new(
            fhir_data,
            col_key_seq="subject.reference",
            row_key_seq="component.valueCodeableConcept.coding.code",
            value_key_seq="component.valueQuantity.value",
            row_id_filters=["ENSG"],
            flame_logger=logger,
            output_type="dict",
        )
        assert set(old.keys()) == set(
            new.keys()
        ), f"Column mismatch: {set(old.keys()) ^ set(new.keys())}"
        for col in old:
            assert (
                set(old[col].keys()) == set(new[col].keys())
            ), f"Row mismatch in col {col}: {set(old[col].keys()) ^ set(new[col].keys())}"
            for row in old[col]:
                assert (
                    float(old[col][row]) == float(new[col][row])
                ), f"Value mismatch at [{col}][{row}]: old={old[col][row]} new={new[col][row]}"

    def test_questionnaire_json_same_output(self, logger):
        with open("tests/unit_test/stream_ques.json", "r") as f:
            fhir_data = ast.literal_eval(f.read())

        old = fhir_to_csv_old(
            fhir_data,
            col_key_seq="resource.item.linkId",
            value_key_seq="resource.item.answer.value",
            input_resource="QuestionnaireResponse",
            flame_logger=logger,
            output_type="dict",
        )
        new = fhir_to_csv_new(
            fhir_data,
            col_key_seq="linkId",
            value_key_seq="answer.children()",
            items_path="item",
            flame_logger=logger,
            output_type="dict",
        )
        assert set(old.keys()) == set(
            new.keys()
        ), f"Column mismatch: {set(old.keys()) ^ set(new.keys())}"
        for col in old:
            assert set(old[col].keys()) == set(
                new[col].keys()
            ), f"Row mismatch in col {col}"
            for row in old[col]:
                old_val = old[col][row]
                new_val = new[col][row]
                if isinstance(old_val, (int, float)):
                    assert float(old_val) == float(
                        new_val
                    ), f"Value mismatch at [{col}][{row}]: old={old_val} new={new_val}"
                else:
                    assert str(old_val) == str(
                        new_val
                    ), f"Value mismatch at [{col}][{row}]: old={old_val} new={new_val}"


# ---------------------------------------------------------------------------
# Generic resource tests (proving resource-agnostic behavior)
# ---------------------------------------------------------------------------


class TestGenericResources:
    """Test with FHIR resource types that the old implementation does NOT support."""

    def test_patient_resource(self, logger):
        """Flatten Patient resources — not supported by old implementation."""
        bundle = {
            "type": "searchset",
            "entry": [
                {
                    "resource": {
                        "resourceType": "Patient",
                        "id": "P1",
                        "gender": "female",
                        "birthDate": "1990-01-15",
                        "name": [{"family": "Smith", "given": ["Alice"]}],
                    }
                },
                {
                    "resource": {
                        "resourceType": "Patient",
                        "id": "P2",
                        "gender": "male",
                        "birthDate": "1985-06-20",
                        "name": [{"family": "Jones", "given": ["Bob"]}],
                    }
                },
            ],
        }
        result = fhir_to_csv_new(
            fhir_data=bundle,
            col_key_seq="id",
            row_key_seq="name.family",
            value_key_seq="gender",
            flame_logger=logger,
            output_type="dict",
        )
        assert "P1" in result
        assert "P2" in result
        assert result["P1"]["Smith"] == "female"
        assert result["P2"]["Jones"] == "male"

    def test_condition_resource(self, logger):
        """Flatten Condition resources with nested code/category."""
        bundle = {
            "type": "searchset",
            "entry": [
                {
                    "resource": {
                        "resourceType": "Condition",
                        "subject": {"reference": "Patient/P1"},
                        "code": {
                            "coding": [
                                {
                                    "system": "http://snomed.info/sct",
                                    "code": "73211009",
                                    "display": "Diabetes",
                                }
                            ]
                        },
                        "clinicalStatus": {"coding": [{"code": "active"}]},
                    }
                },
                {
                    "resource": {
                        "resourceType": "Condition",
                        "subject": {"reference": "Patient/P2"},
                        "code": {
                            "coding": [
                                {
                                    "system": "http://snomed.info/sct",
                                    "code": "38341003",
                                    "display": "Hypertension",
                                }
                            ]
                        },
                        "clinicalStatus": {"coding": [{"code": "resolved"}]},
                    }
                },
            ],
        }
        result = fhir_to_csv_new(
            fhir_data=bundle,
            col_key_seq="subject.reference",
            row_key_seq="code.coding.display",
            value_key_seq="clinicalStatus.coding.code",
            flame_logger=logger,
            output_type="dict",
        )
        assert "Patient/P1" in result
        assert "Patient/P2" in result
        assert result["Patient/P1"]["Diabetes"] == "active"
        assert result["Patient/P2"]["Hypertension"] == "resolved"

    def test_medication_request_resource(self, logger):
        """Flatten MedicationRequest with dosage arrays."""
        bundle = {
            "type": "searchset",
            "entry": [
                {
                    "resource": {
                        "resourceType": "MedicationRequest",
                        "subject": {"reference": "Patient/P1"},
                        "medicationCodeableConcept": {
                            "coding": [{"display": "Metformin"}]
                        },
                        "dosageInstruction": [{"text": "500mg twice daily"}],
                    }
                },
                {
                    "resource": {
                        "resourceType": "MedicationRequest",
                        "subject": {"reference": "Patient/P2"},
                        "medicationCodeableConcept": {
                            "coding": [{"display": "Lisinopril"}]
                        },
                        "dosageInstruction": [{"text": "10mg once daily"}],
                    }
                },
            ],
        }
        result = fhir_to_csv_new(
            fhir_data=bundle,
            col_key_seq="subject.reference",
            row_key_seq="medicationCodeableConcept.coding.display",
            value_key_seq="dosageInstruction.text",
            flame_logger=logger,
            output_type="dict",
        )
        assert result["Patient/P1"]["Metformin"] == "500mg twice daily"
        assert result["Patient/P2"]["Lisinopril"] == "10mg once daily"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_bundle(self, logger):
        bundle = {"type": "searchset", "entry": []}
        result = fhir_to_csv_new(
            fhir_data=bundle,
            col_key_seq="subject.reference",
            value_key_seq="valueQuantity.value",
            flame_logger=logger,
            output_type="dict",
        )
        assert result == {}

    def test_missing_fields_skipped(self, logger):
        """Entries where FHIRPath finds nothing should not crash."""
        bundle = {
            "type": "searchset",
            "entry": [
                {"resource": {"resourceType": "Observation", "status": "final"}},
                {
                    "resource": {
                        "resourceType": "Observation",
                        "subject": {"reference": "Patient/P1"},
                        "code": {"coding": [{"code": "weight"}]},
                        "valueQuantity": {"value": 70.0},
                    }
                },
            ],
        }
        result = fhir_to_csv_new(
            fhir_data=bundle,
            col_key_seq="subject.reference",
            row_key_seq="code.coding.code",
            value_key_seq="valueQuantity.value",
            flame_logger=logger,
            output_type="dict",
        )
        # First entry has no subject/code/value, should produce a None-keyed entry or be handled
        # Second entry should be present
        assert "Patient/P1" in result

    def test_bundle_without_total_field(self, logger):
        """Bundles without 'total' should not crash."""
        bundle = {
            "type": "searchset",
            "entry": [
                {
                    "resource": {
                        "resourceType": "Observation",
                        "subject": {"reference": "Patient/P1"},
                        "valueQuantity": {"value": 1.0},
                    }
                },
            ],
        }
        result = fhir_to_csv_new(
            fhir_data=bundle,
            col_key_seq="subject.reference",
            value_key_seq="valueQuantity.value",
            flame_logger=logger,
            output_type="dict",
        )
        assert "Patient/P1" in result
