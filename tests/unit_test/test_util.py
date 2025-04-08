from flamesdk.resources.utils import extract_remaining_time_from_token , flame_log


def test_extract_remaining_time_from_token():
    token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJBWWdqdWV5T09pRVZjM2pOYVdweWtZWWptQjhOaEVkTTRlbFhzUHN5SDhvIn0.eyJleHAiOjE3NDE2ODYxMzcsImlhdCI6MTc0MTY4NDMzNywianRpIjoiMzUwZWM2ZWUtMWYwMi00NDdhLTgyOWYtOTE5MzMxODNhNGY1IiwiaXNzIjoiaHR0cDovL2ZsYW1lLW5vZGUta2V5Y2xvYWsvcmVhbG1zL2ZsYW1lIiwiYXVkIjoiYWNjb3VudCIsInN1YiI6IjIzYzI5ODQxLThhNzUtNGFjNi1hMjM4LTAyN2QyMDJjN2FjYyIsInR5cCI6IkJlYXJlciIsImF6cCI6ImJiOWFhMTY1LTkwOTYtNGFhZS1iNTE5LTgxN2Y4NTdlNjNiYSIsImFjciI6IjEiLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsib2ZmbGluZV9hY2Nlc3MiLCJkZWZhdWx0LXJvbGVzLWZsYW1lIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6ImVtYWlsIHByb2ZpbGUiLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsImNsaWVudEhvc3QiOiIxMC4yNDQuMTc0LjI0NiIsInByZWZlcnJlZF91c2VybmFtZSI6InNlcnZpY2UtYWNjb3VudC1iYjlhYTE2NS05MDk2LTRhYWUtYjUxOS04MTdmODU3ZTYzYmEiLCJjbGllbnRBZGRyZXNzIjoiMTAuMjQ0LjE3NC4yNDYiLCJjbGllbnRfaWQiOiJiYjlhYTE2NS05MDk2LTRhYWUtYjUxOS04MTdmODU3ZTYzYmEifQ.o-jq4eMASfwigw83k5XWpaGrl1_omUNP9onkGqa1LWhY_j8Ziv45A4c1IUjcCdSBBXMwylFoNxvA97lHKHsOFH5Bv3EeVDeIUA3YyCPFPyVAH8Woi26E0iGTmUoFyW8Vn6_Xk_jRfK280BHORL6SxjH5nvGQuVIkXHCgaTo2YTRN4ze4i1xpCnNwBcdC7y94y5MrVT9xDGalgB7qfho0lIGzdgXNJjwBwnDXRjrszShsvkW2TCphql0kS7pEMDptWd2WHavIHAQqQritFfe5VylEdhkH2u_FeNksESAZJlYHPxNSz1XWYtDLymnFw_oQbOF_kf0PI_d4-gJ96W8h2g"
    remaining_time = extract_remaining_time_from_token(token)
    assert (remaining_time == 0)


def test_flame_log():
    with open('stream.tar', 'rb') as file:
        file_content = file.read()
    flame_log(file_content, False)
    flame_log("file_content", True, suppress_head=True)
