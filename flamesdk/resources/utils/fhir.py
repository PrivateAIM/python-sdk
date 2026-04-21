from io import StringIO
from typing import Optional, Any, Literal, Union

from flamesdk.resources.client_apis.data_api import DataAPI
from flamesdk.resources.utils.logging import FlameLogger


_KNOWN_RESOURCES = ['Observation', 'QuestionnaireResponse']


def fhir_to_csv(fhir_data: dict[str, Any],
                col_key_seq: str,
                value_key_seq: str,
                input_resource: str,
                flame_logger: FlameLogger,
                row_key_seq: Optional[str] = None,
                row_id_filters: Optional[list[str]] = None,
                col_id_filters: Optional[list[str]] = None,
                row_col_name: str = '',
                separator: str = ',',
                output_type: Literal["file", "dict"] = "file",
                data_client: Optional[Union[DataAPI, bool]] = None) -> Union[StringIO, dict[Any, dict[Any, Any]]]:
    if input_resource not in _KNOWN_RESOURCES:
        flame_logger.raise_error(f"Unknown resource specified (given={input_resource}, known={_KNOWN_RESOURCES})")
    if input_resource == 'Observation' and not row_key_seq:
        flame_logger.raise_error(f"Resource 'Observation' specified, but no valid row key sequence was given "
                                 f"(given={row_key_seq})")

    df_dict = {}
    col_keys = col_key_seq.split('.')
    value_keys = value_key_seq.split('.')
    row_keys = row_key_seq.split('.') if row_key_seq else None
    flame_logger.new_log(f"Converting fhir data resource of type={input_resource} to csv")
    total_count = int(fhir_data['total'])
    count_mod = 10 ** (len(str(total_count)) - 2)
    count_mod = count_mod if count_mod > 1 else 1
    current_count = 0
    while True:
        for i, entry in enumerate(fhir_data['entry']):
            current_count += 1
            if (current_count == 1) or not (current_count % count_mod):
                flame_logger.new_log(f"Parsing fhir data entry no={current_count} of {total_count}")

            # extract from resource
            if input_resource == 'Observation':
                col_id = _search_fhir_resource(fhir_entry=entry, flame_logger=flame_logger, keys=col_keys)
                row_id = _search_fhir_resource(fhir_entry=entry, flame_logger=flame_logger, keys=row_keys)
                value = _search_fhir_resource(fhir_entry=entry, flame_logger=flame_logger, keys=value_keys)
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
                for item in entry['resource']['item']:
                    col_id = _search_fhir_resource(fhir_entry=item,
                                                   flame_logger=flame_logger,
                                                   keys=col_keys,
                                                   current=2)
                    value = _search_fhir_resource(fhir_entry=item,
                                                  flame_logger=flame_logger,
                                                  keys=value_keys,
                                                  current=2)
                    if col_id_filters is not None:
                        if (col_id is None) or (not any([col_id_filter in col_id for col_id_filter in col_id_filters])):
                            continue
                    if col_id not in df_dict.keys():
                        df_dict[col_id] = {}
                    df_dict[col_id][str(i)] = value
            else:
                try:
                    raise IOError(f"Unknown resource specified (given={input_resource}, known={_KNOWN_RESOURCES})")
                except IOError as e:
                    flame_logger.raise_error(f"Error while parsing fhir data: {repr(e)}")

        # get next data
        if (data_client is None) or (isinstance(data_client, bool)):
            break
        else:
            next_query = ''
            for e in fhir_data['link']:
                link_relation, link_url = str(e['relation']), str(e['url'])
                if link_relation == 'next':
                    next_query = link_url.split('/fhir/')[-1]
                    flame_logger.new_log(f"Parsing next batch query={next_query}")
            if next_query:
                fhir_data = [r for r in data_client.get_fhir_data([next_query]) if r][0][next_query]
            else:
                flame_logger.new_log("Fhir data parsing finished")
                break

    # set output format
    if output_type == "file":
        output = _dict_to_csv(data=df_dict, row_col_name=row_col_name, separator=separator, flame_logger=flame_logger)
    else:
        output = df_dict

    return output


def _dict_to_csv(data: dict[Any, dict[Any, Any]],
                 row_col_name: str,
                 separator: str,
                 flame_logger: FlameLogger) -> StringIO:
    flame_logger.new_log("Writing fhir data dict to csv...")
    columns = list(data.keys())
    row_ids = dict.fromkeys(row_id for col in data.values() for row_id in col)
    lines = [separator.join([row_col_name] + [str(c) for c in columns])]
    for row_id in row_ids:
        line = [str(row_id)]
        for col in columns:
            line.append(str(data[col].get(row_id, '')))
        lines.append(separator.join(line))

    io = StringIO()
    io.write('\n'.join(lines))
    io.seek(0)
    flame_logger.new_log("Fhir data converted to csv")
    return io


def _search_fhir_resource(fhir_entry: Union[dict[str, Any], list[Any]],
                          flame_logger: FlameLogger,
                          keys: list[str],
                          current: int = 0) -> Optional[Any]:
    key = keys[current]
    if (current < (len(keys) - 1)) or (type(fhir_entry) == list):
        if type(fhir_entry) == dict:
            if key in fhir_entry.keys():
                next_value = _search_fhir_resource(fhir_entry[key], flame_logger, keys, current + 1)
                if next_value is not None:
                    return next_value
            else:
                return None
        elif type(fhir_entry) == list:
            for e in fhir_entry:
                next_value = _search_fhir_resource(e, flame_logger, keys, current)
                if next_value is not None:
                    return next_value
        else:
            return None
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
                        flame_logger.new_log(f"Unable to find field '{key}' in fhir data at level={current + 1} "
                                             f"(keys found: fhir_entry.keys())",
                                             log_type='warning')
                        return None
                return value
            else:
                return None
        else:
            flame_logger.new_log(f"Unexpected data type found (found type={type(fhir_entry)})",
                                 log_type='warning')
            return None
