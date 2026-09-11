"""Conversion of FHIR search bundles into tabular (csv) form."""

from io import StringIO
from typing import Optional, Any, Literal, Union

from flamesdk.resources.client_apis.data_api import DataAPI
from flamesdk.resources.utils.logging import FlameLogger
from flamesdk.resources.utils.constants import LogTypeLiteral


_KNOWN_RESOURCES = ["Observation", "QuestionnaireResponse"]


def fhir_to_csv(
    fhir_data: dict[str, Any],
    col_key_seq: str,
    value_key_seq: str,
    input_resource: str,
    flame_logger: FlameLogger,
    row_key_seq: Optional[str] = None,
    row_id_filters: Optional[list[str]] = None,
    col_id_filters: Optional[list[str]] = None,
    row_col_name: str = "",
    separator: str = ",",
    output_type: Literal["file", "dict"] = "file",
    data_client: Optional[Union[DataAPI, bool]] = None,
) -> Union[StringIO, dict[Any, dict[Any, Any]]]:
    """Flatten a FHIR search bundle into a table.

    Each entry in the bundle contributes one cell: ``col_key_seq`` locates the
    value used as the column, ``row_key_seq`` the one used as the row, and
    ``value_key_seq`` the cell value itself. Key sequences are dotted paths
    such as ``'resource.subject.reference'``, matched against the entry with
    :func:`_search_fhir_resource`.

    For ``'Observation'`` a row key is required, since each entry carries its
    own subject. For ``'QuestionnaireResponse'`` the entry's position in the
    bundle is used as the row instead, and each item within the entry becomes
    its own column.

    When ``data_client`` is a :class:`DataAPI`, the bundle's ``next`` link is
    followed so that paginated searches are consumed in full.

    :param fhir_data: the FHIR search bundle to convert
    :param col_key_seq: dotted key sequence locating the column id
    :param value_key_seq: dotted key sequence locating the cell value
    :param input_resource: resource type in the bundle, one of
        ``'Observation'`` or ``'QuestionnaireResponse'``
    :param flame_logger: logger used to report progress and unusable entries
    :param row_key_seq: dotted key sequence locating the row id; required for
        ``'Observation'``
    :param row_id_filters: keep only rows whose id contains one of these
    :param col_id_filters: keep only columns whose id contains one of these;
        only applied when ``row_id_filters`` is not given
    :param row_col_name: header for the row-id column
    :param separator: field separator used for csv output
    :param output_type: ``'file'`` for csv in a buffer, ``'dict'`` for the
        nested ``{column: {row: value}}`` mapping
    :param data_client: data api used to follow pagination links; pass ``None``
        or a bool to convert only the bundle given
    :return: the csv buffer, or the nested mapping when ``output_type='dict'``
    """
    if input_resource not in _KNOWN_RESOURCES:
        flame_logger.raise_error(
            f"Unknown resource specified (given={input_resource}, known={_KNOWN_RESOURCES})"
        )
    if input_resource == "Observation" and not row_key_seq:
        flame_logger.raise_error(
            f"Resource 'Observation' specified, but no valid row key sequence was given "
            f"(given={row_key_seq})"
        )

    df_dict = {}
    col_keys = col_key_seq.split(".")
    value_keys = value_key_seq.split(".")
    row_keys = row_key_seq.split(".") if row_key_seq else None
    flame_logger.new_log(
        f"Converting fhir data resource of type={input_resource} to csv"
    )
    total_count = int(fhir_data["total"])
    count_mod = 10 ** (len(str(total_count)) - 2)
    count_mod = count_mod if count_mod > 1 else 1
    current_count = 0
    while True:
        for i, entry in enumerate(fhir_data["entry"]):
            current_count += 1
            if (current_count == 1) or not (current_count % count_mod):
                flame_logger.new_log(
                    f"Parsing fhir data entry no={current_count} of {total_count}"
                )

            # extract from resource
            if input_resource == "Observation":
                col_id = _search_fhir_resource(
                    fhir_entry=entry, flame_logger=flame_logger, keys=col_keys
                )
                row_id = _search_fhir_resource(
                    fhir_entry=entry, flame_logger=flame_logger, keys=row_keys
                )
                value = _search_fhir_resource(
                    fhir_entry=entry, flame_logger=flame_logger, keys=value_keys
                )
                if row_id_filters is not None:
                    if (row_id is None) or (
                        not any(
                            [
                                row_id_filter in row_id
                                for row_id_filter in row_id_filters
                            ]
                        )
                    ):
                        continue
                elif col_id_filters is not None:
                    if (col_id is None) or (
                        not any(
                            [
                                col_id_filter in col_id
                                for col_id_filter in col_id_filters
                            ]
                        )
                    ):
                        continue
                if col_id not in df_dict.keys():
                    df_dict[col_id] = {}
                if row_id not in df_dict[col_id].keys():
                    df_dict[col_id][row_id] = ""
                df_dict[col_id][row_id] = value

            elif input_resource == "QuestionnaireResponse":
                for item in entry["resource"]["item"]:
                    col_id = _search_fhir_resource(
                        fhir_entry=item,
                        flame_logger=flame_logger,
                        keys=col_keys,
                        current=2,
                    )
                    value = _search_fhir_resource(
                        fhir_entry=item,
                        flame_logger=flame_logger,
                        keys=value_keys,
                        current=2,
                    )
                    if col_id_filters is not None:
                        if (col_id is None) or (
                            not any(
                                [
                                    col_id_filter in col_id
                                    for col_id_filter in col_id_filters
                                ]
                            )
                        ):
                            continue
                    if col_id not in df_dict.keys():
                        df_dict[col_id] = {}
                    df_dict[col_id][str(i)] = value
            else:
                flame_logger.raise_error(
                    f"Error while parsing fhir data: Unknown resource specified "
                    f"(given={input_resource}, known={_KNOWN_RESOURCES})"
                )

        # get next data
        if (data_client is None) or (isinstance(data_client, bool)):
            break
        else:
            next_query = ""
            for e in fhir_data["link"]:
                link_relation, link_url = str(e["relation"]), str(e["url"])
                if link_relation == "next":
                    next_query = link_url.split("/fhir/")[-1]
                    flame_logger.new_log(
                        f"Parsing next batch query={next_query}",
                        log_type=LogTypeLiteral.DEBUG.value,
                    )
            if next_query:
                fhir_data = [r for r in data_client.get_fhir_data([next_query]) if r][
                    0
                ][next_query]
            else:
                flame_logger.new_log("Fhir data parsing finished")
                break

    # set output format
    if output_type == "file":
        output = _dict_to_csv(
            data=df_dict,
            row_col_name=row_col_name,
            separator=separator,
            flame_logger=flame_logger,
        )
    else:
        output = df_dict

    return output


def _dict_to_csv(
    data: dict[Any, dict[Any, Any]],
    row_col_name: str,
    separator: str,
    flame_logger: FlameLogger,
) -> StringIO:
    """Render a nested ``{column: {row: value}}`` mapping as csv.

    Rows are emitted in first-seen order across all columns, and a cell missing
    from a column is written as an empty field.

    :param data: nested mapping of column id to row id to value
    :param row_col_name: header for the row-id column
    :param separator: field separator
    :param flame_logger: logger used to report progress
    :return: a rewound buffer holding the csv text
    """
    flame_logger.new_log("Writing fhir data dict to csv...", halt_submission=True)
    columns = list(data.keys())
    row_ids = dict.fromkeys(row_id for col in data.values() for row_id in col)
    lines = [separator.join([row_col_name] + [str(c) for c in columns])]
    for row_id in row_ids:
        line = [str(row_id)]
        for col in columns:
            line.append(str(data[col].get(row_id, "")))
        lines.append(separator.join(line))

    io = StringIO()
    io.write("\n".join(lines))
    io.seek(0)
    flame_logger.new_log("success")
    return io


def _search_fhir_resource(
    fhir_entry: Union[dict[str, Any], list[Any]],
    flame_logger: FlameLogger,
    keys: list[str],
    current: int = 0,
) -> Optional[Any]:
    """Walk a key sequence through a FHIR entry and return the first match.

    Lists are searched element by element, so a path like ``'coding.code'``
    resolves against ``{"coding": [{"code": "ABC"}]}``. At the final key a
    prefix match is accepted, which is how ``'answer.value'`` reaches a
    concretely typed field such as ``valueDecimal``.

    Note that ``keys`` is an already-split list, not a dotted string; callers
    split the sequence themselves.

    :param fhir_entry: the entry, or the sub-structure currently being searched
    :param flame_logger: logger used to report unexpected data shapes
    :param keys: the key sequence, already split on ``'.'``
    :param current: index of the key to match at this level
    :return: the first value found, or ``None`` if the path does not resolve
    """
    key = keys[current]
    if (current < (len(keys) - 1)) or isinstance(fhir_entry, list):
        if isinstance(fhir_entry, dict):
            if key in fhir_entry.keys():
                next_value = _search_fhir_resource(
                    fhir_entry[key], flame_logger, keys, current + 1
                )
                if next_value is not None:
                    return next_value
            else:
                return None
        elif isinstance(fhir_entry, list):
            for e in fhir_entry:
                next_value = _search_fhir_resource(e, flame_logger, keys, current)
                if next_value is not None:
                    return next_value
        else:
            return None
    else:
        if current == (len(keys) - 1):
            if isinstance(fhir_entry, dict):
                try:
                    value = fhir_entry[key]
                except KeyError:
                    key = [k for k in fhir_entry.keys() if key in k][0]
                    if key:
                        value = fhir_entry[key]
                    else:
                        flame_logger.new_log(
                            f"Unable to find field '{key}' in fhir data at level={current + 1} "
                            f"(keys found: fhir_entry.keys())",
                            log_type=LogTypeLiteral.WARNING.value,
                        )
                        return None
                return value
            else:
                return None
        else:
            flame_logger.new_log(
                f"Unexpected data type found (found type={type(fhir_entry)})",
                log_type=LogTypeLiteral.WARNING.value,
            )
            return None
