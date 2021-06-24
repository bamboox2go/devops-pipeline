import json
import logging
from itertools import groupby
from pathlib import Path

import requests

import pytest

import mi
from mi.connectors.blob_storage import blob_data, storage_tool
from mi.utils.config import mi_config

LOG = logging.getLogger(__name__)

EXPECTED_FILE_PATH = Path(__file__).resolve().parent.parent / 'resources/metadata_result.json'
ROW_IDENTIFIER = 'interaction-id'
SKIPPED_COLUMNS = ['accept-timestamp', 'agent-accept-timestamp', 'agent-abandon-timestamp', 'abandon-timestamp']


@pytest.mark.skip(reason="Need to fix single row ingestion")
def test_create_all_interactions_pipeline(adf_pipeline_run, storage_tool, mi_config):
    start_date = "20210329"
    end_date = "20210406"
    run_input = {
        'startDate': start_date,
        'endDate': end_date,
        'sourceName': 'test-adf-{}-eightbyeight'.format(mi_config[mi.utils.config.BUILD_VERSION]),
        'sourceSubName': 'allinteractions'
    }
    test_container = "{}-{}".format(run_input["sourceName"], run_input["sourceSubName"])
    this_run = adf_pipeline_run("ExE_allInteraction_load", run_inputs=run_input)
    # Delete test container after tests
    storage_tool.register_deletion(mi_config[mi.utils.config.MI_LANDING_KEY], test_container)
    storage_tool.register_deletion(mi_config[mi.utils.config.MI_PERSISTENT_KEY], test_container)

    assert this_run.status == "Succeeded"
    verify_container(storage_tool, mi_config, test_container, "2021-03/", 1)
    verify_container(storage_tool, mi_config, test_container, "2021-04/", 1)

    validate_request_data(storage_tool, mi_config, test_container)


def validate_request_data(storage_tool, config, container_name):
    resp = request_data('allinteractions', '20210406', '20210406', config, 0)
    json_resp = resp.json()
    api_data = json_resp['interactions']['interaction']
    extracted_data = get_data_from_container(storage_tool, config, container_name, "2021-04/")
    grouped_list_lambda = group_interactions(api_data)

    for data_row in extracted_data:
        json_row = json.loads(data_row)
        row_found = next((x for x in grouped_list_lambda[json_row[ROW_IDENTIFIER]] if item_exist_single(json_row, x)),
                         None)
        assert row_found is not None


def group_interactions(data):
    grouped_list_lambda = groupby(data, lambda a: a['interaction-id'])
    grouped_list = {}
    for key, group_list in grouped_list_lambda:
        grouped_list[key] = []
        grouped_list[key].extend(group_list)

    return grouped_list


def item_exist_single(encoded_input, expected_element):
    fields = []
    if expected_element is None:
        return False
    for key in expected_element.keys():
        expected_value = expected_element[key]
        if key in SKIPPED_COLUMNS:
            continue
        try:
            if str(expected_value) != str(encoded_input[key]):
                fields.append(key)
        except KeyError:
            try:
                if expected_value != 0 and len(expected_value) != 0:
                    fields.append(key)
            except TypeError:
                print("key error", key)
    return len(fields) == 0


def request_data(endpoint, init_date, final_date, config, offset=0):
    end_date = final_date

    date_filter = init_date + "," + end_date
    final_path = 'https://vcc-eu4.8x8.com/api/stats/{endpoint}.json?n={page}&d={date}' \
        .format(endpoint=endpoint, page=offset, date=date_filter)
    resp = requests.get(final_path, auth=(config[mi.utils.config.EIGHTBYEIGHT_API_USER],
                                          config[mi.utils.config.EIGHTBYEIGHT_API_PASSWORD]))

    if resp.status_code != 200:
        # This means something went wrong.
        return
    return resp


def get_data_from_container(storage_tool, config, container, prefix):
    blobs = storage_tool.list_blobs(config[mi.utils.config.MI_PERSISTENT_KEY], container, prefix)
    total_records = []
    for blob in blobs:
        total_records.extend(storage_tool.read_blob_data(config[mi.utils.config.MI_PERSISTENT_KEY],
                                                         container, blob).splitlines())
    return total_records


def verify_container(storage_tool, config, container, prefix, expected_rows):
    blobs = storage_tool.list_blobs(config[mi.utils.config.MI_PERSISTENT_KEY], container, prefix)
    total_ingested_records = 0
    for blob in blobs:
        total_ingested_records += storage_tool.get_blob_lines(config[mi.utils.config.MI_PERSISTENT_KEY],
                                                              container, blob)
    assert total_ingested_records == expected_rows


def assert_file_content(file, content):
    with open(file, 'r', encoding='utf-8') as file_content:
        expected_content = file_content.read()
        assert expected_content == content.strip()
