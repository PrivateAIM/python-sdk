from flamesdk.resources.utils.fhir import fhir_to_csv, _search_fhir_resource
from flamesdk.resources.utils.utils import extract_remaining_time_from_token, flame_log
import ast
import time

def test_extract_remaining_time_from_token():
    token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJBWWdqdWV5T09pRVZjM2pOYVdweWtZWWptQjhOaEVkTTRlbFhzUHN5SDhvIn0.eyJleHAiOjE3NDE2ODYxMzcsImlhdCI6MTc0MTY4NDMzNywianRpIjoiMzUwZWM2ZWUtMWYwMi00NDdhLTgyOWYtOTE5MzMxODNhNGY1IiwiaXNzIjoiaHR0cDovL2ZsYW1lLW5vZGUta2V5Y2xvYWsvcmVhbG1zL2ZsYW1lIiwiYXVkIjoiYWNjb3VudCIsInN1YiI6IjIzYzI5ODQxLThhNzUtNGFjNi1hMjM4LTAyN2QyMDJjN2FjYyIsInR5cCI6IkJlYXJlciIsImF6cCI6ImJiOWFhMTY1LTkwOTYtNGFhZS1iNTE5LTgxN2Y4NTdlNjNiYSIsImFjciI6IjEiLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsib2ZmbGluZV9hY2Nlc3MiLCJkZWZhdWx0LXJvbGVzLWZsYW1lIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6ImVtYWlsIHByb2ZpbGUiLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsImNsaWVudEhvc3QiOiIxMC4yNDQuMTc0LjI0NiIsInByZWZlcnJlZF91c2VybmFtZSI6InNlcnZpY2UtYWNjb3VudC1iYjlhYTE2NS05MDk2LTRhYWUtYjUxOS04MTdmODU3ZTYzYmEiLCJjbGllbnRBZGRyZXNzIjoiMTAuMjQ0LjE3NC4yNDYiLCJjbGllbnRfaWQiOiJiYjlhYTE2NS05MDk2LTRhYWUtYjUxOS04MTdmODU3ZTYzYmEifQ.o-jq4eMASfwigw83k5XWpaGrl1_omUNP9onkGqa1LWhY_j8Ziv45A4c1IUjcCdSBBXMwylFoNxvA97lHKHsOFH5Bv3EeVDeIUA3YyCPFPyVAH8Woi26E0iGTmUoFyW8Vn6_Xk_jRfK280BHORL6SxjH5nvGQuVIkXHCgaTo2YTRN4ze4i1xpCnNwBcdC7y94y5MrVT9xDGalgB7qfho0lIGzdgXNJjwBwnDXRjrszShsvkW2TCphql0kS7pEMDptWd2WHavIHAQqQritFfe5VylEdhkH2u_FeNksESAZJlYHPxNSz1XWYtDLymnFw_oQbOF_kf0PI_d4-gJ96W8h2g"
    remaining_time = extract_remaining_time_from_token(token)
    assert (remaining_time == 0)


def test_flame_log():
    with open('stream.tar', 'rb') as file:
        file_content = file.read()
    flame_log(file_content, False)
    flame_log("file_content", True, suppress_head=True)

def test_multi_param(in_type):
    if in_type == 'Observation':
        filename = "stream_observation.json"
    elif in_type == 'QuestionnaireResponse':
        filename = "stream_questionnaire.json"
    else:
        raise IOError('Unable to recognize input resource type')
    with open(filename, "r") as f:
        content = f.read()
        fhir_data = ast.literal_eval(content)

    if in_type == 'Observation':
        return fhir_to_csv(fhir_data,
                           col_key_seq="resource.subject.reference",
                           row_key_seq="resource.component.valueCodeableConcept.coding.code",
                           value_key_seq="resource.component.valueQuantity.value",
                           row_id_filters=["ENSG"],
                           input_resource=in_type)
    elif in_type == 'QuestionnaireResponse':
        return fhir_to_csv(fhir_data,
                           col_key_seq="resource.item.linkId",
                           value_key_seq="resource.item.answer.value",
                           input_resource=in_type)

start_time = time.time()
#print(_search_in_fhir_entry({'fullUrl': 'http://nginx-analysis-671b3985-f901-48c5-9cba-ee62bc6f393d-1/fhir/Observation/gene-observation-C00039-ENSG00000005156', 'resource': {'category': [{'coding': [{'code': 'laboratory', 'display': 'Laboratory', 'system': 'http://terminology.hl7.org/CodeSystem/observation-category'}]}], 'code': {'coding': [{'code': '69548-6', 'display': 'Genetic variant assessment', 'system': 'http://loinc.org'}]}, 'component': [{'code': {'coding': [{'code': '48018-6', 'display': 'Gene studied ' '[ID]', 'system': 'http://loinc.org'}]}, 'valueCodeableConcept': {'coding': [{'code': 'ENSG00000005156', 'system': 'http://ensembl.org'}]}}, {'code': {'coding': [{'code': '48003-8', 'display': 'DNA sequence ' 'variation ' 'identifier ' '[Identifier]', 'system': 'http://loinc.org'}]}, 'valueQuantity': {'code': 'count', 'system': 'http://unitsofmeasure.org', 'unit': 'count', 'value': 2452}}], 'id': 'gene-observation-C00039-ENSG00000005156', 'meta': {'lastUpdated': '2025-06-12T11:24:08.247Z', 'versionId': '3'}, 'resourceType': 'Observation', 'status': 'final', 'subject': {'reference': 'Patient/C00039'}}, 'search': {'mode': 'match'}}, 'resource.component.valueQuantity.value' ))
print("output: " + test_multi_param('Observation').read())
print(f"Elapsed time: {time.time() - start_time} secs")
print(f"Estimated time: {(time.time() - start_time) * ((41886 * 118) / 500) / 60} minutes\n\n")

start_time = time.time()
print("output: " + test_multi_param('QuestionnaireResponse').read())
print(f"Elapsed time: {time.time() - start_time} secs")
