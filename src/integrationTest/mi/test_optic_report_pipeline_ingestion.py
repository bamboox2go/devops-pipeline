import logging
import time

import json
from json import JSONDecodeError

import jwt
import requests

import mi
from mi.connectors.blob_storage import storage_tool
from mi.utils.config import mi_config

LOG = logging.getLogger(__name__)


def test_optic_report_sdp0001(adf_pipeline_run, storage_tool, mi_config):
    test_container = run_optic_report_pipeline(adf_pipeline_run, storage_tool, mi_config, '66501', '20210430',
                                               '20210504')
    verify_container(storage_tool, mi_config, test_container, '66501', '2021-04-30', '2021-05-04')


def test_optic_report_sdp0002(adf_pipeline_run, storage_tool, mi_config):
    test_container = run_optic_report_pipeline(adf_pipeline_run, storage_tool, mi_config, '66502', '20210518',
                                               '20210520')
    verify_container(storage_tool, mi_config, test_container, '66502', '2021-05-18', '2021-05-20')


def test_optic_report_sdp0003(adf_pipeline_run, storage_tool, mi_config):
    test_container = run_optic_report_pipeline(adf_pipeline_run, storage_tool, mi_config, '66503', '20210428',
                                               '20210507')
    verify_container(storage_tool, mi_config, test_container, '66503', '2021-04-28', '2021-05-07')


def test_optic_report_sdp0004(adf_pipeline_run, storage_tool, mi_config):
    test_container = run_optic_report_pipeline(adf_pipeline_run, storage_tool, mi_config, '66601', '20210419',
                                               '20210421')
    verify_container(storage_tool, mi_config, test_container, '66601', '2021-04-19', '2021-04-21')


def test_optic_report_sdp0005(adf_pipeline_run, storage_tool, mi_config):
    test_container = run_optic_report_pipeline(adf_pipeline_run, storage_tool, mi_config, '66701', '20210413',
                                               '20210416')
    verify_container(storage_tool, mi_config, test_container, '66701', '2021-04-13', '2021-04-16')


def test_optic_report_sdp0006(adf_pipeline_run, storage_tool, mi_config):
    test_container = run_optic_report_pipeline(adf_pipeline_run, storage_tool, mi_config, '66702', '20210430',
                                               '20210502')
    verify_container(storage_tool, mi_config, test_container, '66702', '2021-04-30', '2021-05-02')


def test_optic_report_sdp0007(adf_pipeline_run, storage_tool, mi_config):
    test_container = run_optic_report_pipeline(adf_pipeline_run, storage_tool, mi_config, '66602', '20210430',
                                               '20210502')
    verify_container(storage_tool, mi_config, test_container, '66602', '2021-04-30', '2021-05-02')


def run_optic_report_pipeline(adf_pipeline_run, storage_tool, config, source_sub_name, start_date, end_date):
    run_input = {
        'startDate': start_date,
        'endDate': end_date,
        'sourceName': 'test-adf-{}-optic-report-{}'.format(config[mi.utils.config.BUILD_VERSION], source_sub_name),
        'sourceSubName': source_sub_name
    }
    test_container = "{}-{}".format(run_input["sourceName"], run_input["sourceSubName"])
    this_run = adf_pipeline_run("PL_Optic_Ingestion_Persistence_And_Export", run_inputs=run_input)
    # Delete test container after tests
    storage_tool.register_deletion(config[mi.utils.config.MI_LANDING_KEY], test_container)
    storage_tool.register_deletion(config[mi.utils.config.MI_PERSISTENT_KEY], test_container)

    assert this_run.status == "Succeeded"
    return test_container


# Note verification only works on the pipeline or in an environment where the IP is whitelisted on UAT Optic
def verify_container(storage_tool, config, container, source_sub_name, start_date, end_date):
    response_token = build_optic_jwt(config).json()
    rest_json_data = get_data_from_rest_endpoint(source_sub_name, start_date, end_date, response_token)

    verify_container_data(storage_tool, rest_json_data, config[mi.utils.config.MI_LANDING_KEY], container)
    verify_container_data(storage_tool, rest_json_data, config[mi.utils.config.MI_PERSISTENT_KEY], container)


def build_optic_jwt(config):
    encoded_jwt = jwt.encode(
        {"iss": config[mi.utils.config.OPTIC_API_KEY], "aud": "https://uatportal.icasework.com/token?db=hmcts", "iat": int(time.time()), "exp": int(time.time()) + 3600},
        config[mi.utils.config.OPTIC_API_SECRET],
        algorithm="HS256"
    )

    return requests.post(
        "https://uatportal.icasework.com/token?db=hmcts",
        data={'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer', 'assertion': encoded_jwt}
    )


def get_data_from_rest_endpoint(source_sub_name, start_date, end_date, response_token):
    try:
        return requests.get("https://uat.icasework.com/getreport?db=hmcts&Format=json&ReportId=" + source_sub_name
                            + "&FromTime=" + start_date + "T00:00:00.000"
                            + "&UntilTime=" + end_date + "T23:59:59.999",
                            headers={"Authorization": "Bearer " + response_token['access_token']}).json()
    except JSONDecodeError:
        return []


def verify_container_data(storage_tool, rest_json_data, storage_account_key, container):
    blobs = storage_tool.list_blobs(storage_account_key, container)
    count = 0
    for blob in blobs:
        if blob != '_metadata' and blob != '_metadata.json':
            data = storage_tool.read_blob_data(storage_account_key, container, blob)
            data_as_list = [json.loads(item) for item in data.splitlines()]

            for record in data_as_list:
                count += 1
                if hasattr(record, 'extraction_date'):
                    delattr(record, 'extraction_date')
                assert record in rest_json_data

    assert count == len(rest_json_data)
