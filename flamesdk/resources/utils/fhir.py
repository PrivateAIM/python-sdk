from io import StringIO
from typing import Optional, Any, Literal, Union

from flamesdk.resources.client_apis.data_api import DataAPI


_KNOWN_RESOURCES = ['Observation', 'QuestionnaireResponse']


def fhir_to_csv(fhir_data: dict[str, Any],
                col_key_seq: str,
                value_key_seq: str,
                input_resource: str,
                row_key_seq: Optional[str] = None,
                row_id_filters: Optional[list[str]] = None,
                col_id_filters: Optional[list[str]] = None,
                row_col_name: str = '',
                separator: str = ',',
                output_type: Literal["file", "dict"] = "file",
                data_client: Optional[DataAPI] = None) -> Union[StringIO, dict[Any, dict[Any, Any]]]:
    if input_resource not in _KNOWN_RESOURCES:
        raise IOError(f"Unknown resource specified (given={input_resource}, known={_KNOWN_RESOURCES})")

    df_dict = {}
    while True:
        # extract from resource
        if input_resource == 'Observation':
            for entry in fhir_data['entry']:
                col_id = _search_fhir_resource(entry, key_sequence=col_key_seq)
                row_id = _search_fhir_resource(entry, key_sequence=row_key_seq)
                value = _search_fhir_resource(entry, key_sequence=value_key_seq)
                if row_id_filters is not None:
                    if (row_id is None) or (not any([row_id_filter in row_id for row_id_filter in row_id_filters])):
                        continue
                elif col_id_filters is not None:
                    if (col_id is None) or (not any([col_id_filter in col_id for col_id_filter in col_id_filters])):
                        continue
                if col_id not in df_dict.keys():
                    df_dict[col_id] = {}
                if row_id not in df_dict[col_id].keys():
                    df_dict[col_id][row_id] = ''
                df_dict[col_id][row_id] = value
        elif input_resource == 'QuestionnaireResponse':
            for i, entry in enumerate(fhir_data['entry']):
                for item in entry['resource']['item']:
                    col_id = _search_fhir_resource(item, key_sequence=col_key_seq, current=2)
                    value = _search_fhir_resource(item, key_sequence=value_key_seq, current=2)
                    if col_id_filters is not None:
                        if (col_id is None) or (not any([col_id_filter in col_id for col_id_filter in col_id_filters])):
                            continue
                    if col_id not in df_dict.keys():
                        df_dict[col_id] = {}
                    df_dict[col_id][str(i)] = value
        else:
            raise IOError(f"Unknown resource specified (given={input_resource}, known={_KNOWN_RESOURCES})")

        # get next data
        if data_client is None:
            break
        else:
            next_query = ''
            for e in fhir_data['link']:
                link_relation, link_url = str(e['relation']), str(e['url'])
                if link_relation == 'next':
                    next_query = link_url.split('/fhir/')[-1]
            if next_query:
                fhir_data = [r for r in data_client.get_fhir_data([next_query]) if r][0][next_query]
            else:
                break

    # set output format
    if output_type == "file":
        output = _dict_to_csv(df_dict, row_col_name=row_col_name, separator=separator)
    else:
        output = df_dict

    return output


def _dict_to_csv(data: dict[Any, dict[Any, Any]], row_col_name: str, separator: str) -> StringIO:
    io = StringIO()
    headers = [f"{row_col_name}"]
    headers.extend(list(data.keys()))
    headers = [f"{header}" for header in headers]
    file_content = separator.join(headers)

    visited_rows = []
    for rows in data.values():
        for row_id in rows.keys():
            if row_id in visited_rows:
                continue
            line_list = [row_id]
            visited_rows.append(row_id)
            for col_id in data.keys():
                try:
                    line_list.append(data[col_id][row_id])
                except KeyError:
                    line_list.append('')
            line_list = [f"{e}" for e in line_list]
            file_content += '\n' + separator.join(line_list)

    io.write(file_content)
    io.seek(0)
    return io


def _search_fhir_resource(fhir_entry: Union[dict[str, Any], list[Any]],
                          key_sequence: str,
                          current: int = 0) -> Optional[Any]:
    keys = key_sequence.split('.')
    key = keys[current]
    if (current < (len(keys) - 1)) or (type(fhir_entry) == list):
        if type(fhir_entry) == dict:
            for field in fhir_entry.keys():
                try:
                    if field == key:
                        fhir_entry = fhir_entry[key]
                        next_value = _search_fhir_resource(fhir_entry, key_sequence, current + 1)
                        if next_value is not None:
                            return next_value
                except KeyError:
                    print(f"KeyError: Unable to find field '{key}' in fhir data at level={current + 1} "
                          f"(keys found: {fhir_entry.keys()})")
                    return None
        elif type(fhir_entry) == list:
            for e in fhir_entry:
                next_value = _search_fhir_resource(e, key_sequence, current)
                if next_value is not None:
                    return next_value
    else:
        if current == (len(keys) - 1):
            if type(fhir_entry) == dict:
                try:
                    value = fhir_entry[key]
                except KeyError:
                    key = [k for k in fhir_entry.keys() if key in k][0]
                    if key:
                        value = fhir_entry[key]
                    else:
                        print(f"KeyError: Unable to find field '{key}' in fhir data at level={current + 1} "
                              f"(keys found: {fhir_entry.keys()})")
                        return None
                return value
        else:
            print(f"Unexpected data type found (found type={type(fhir_entry)})")
