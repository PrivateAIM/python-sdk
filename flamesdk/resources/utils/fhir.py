from io import StringIO
from typing import Optional, Any

# import fhir-kindling


def fhir_to_csv(fhir_data: dict[str, Any],
                col_key_seq: str,
                row_key_seq: str,
                value_key_seq: str,
                row_id_filters: Optional[list[str]] = None,
                col_id_filters: Optional[list[str]] = None,
                row_col_name: str = '',
                separator: str = ',') -> dict[Any, dict[Any, Any]]:
    df_dict = {}

    for i, entry in enumerate(fhir_data['entry']):
        col_id = _search_in_fhir_entry(entry, key_sequence=col_key_seq)
        row_id = _search_in_fhir_entry(entry, key_sequence=row_key_seq)
        if row_id_filters is not None:
            if (row_id is None) or (not all([row_id_filter in row_id for row_id_filter in row_id_filters])):
                continue
        elif col_id_filters is not None:
            if (col_id is None) or (not all([col_id_filter in col_id for col_id_filter in col_id_filters])):
                continue
        if col_id not in df_dict.keys():
            df_dict[col_id] = {}
        if row_id not in df_dict[col_id].keys():
            df_dict[col_id][row_id] = ''

        value = _search_in_fhir_entry(entry, key_sequence=value_key_seq)
        df_dict[col_id][row_id] = value

    return df_dict


def _search_in_fhir_entry(fhir_entry: dict[str, Any] | list[Any],
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
                        next = _search_in_fhir_entry(fhir_entry, key_sequence, current + 1)
                        if next is not None:
                            return next
                except KeyError:
                    print(f"KeyError: Unable to find field '{key}' in fhir data at level={current + 1} "
                          f"(keys found: {fhir_entry.keys()})")
                    return None
        elif type(fhir_entry) == list:
            for e in fhir_entry:
                next = _search_in_fhir_entry(e, key_sequence, current)
                if next is not None:
                    return next
    else:
        if current == (len(keys) - 1):
            if type(fhir_entry) == dict:
                return fhir_entry[key]
        else:
            print(f"Unexpected data type found (found type={type(fhir_entry)})")
