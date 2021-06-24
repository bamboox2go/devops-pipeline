# import mi
from mi.connectors.blob_storage import blob_data, storage_tool
from mi.utils.config import config

SOURCE = 'test-source'
SUB_SOURCE = '66501'
ADF_OPTIC_PIPELINE = 'Optic_load_report_01'


def test_optic_66501_pipeline(adf_pipeline_run, storage_tool, config):
    start_date = "20210301"
    end_date = "20210425"
    run_input = get_config(SOURCE, SUB_SOURCE, start_date, end_date, config)
    test_container = "{}-{}".format(run_input["sourceName"], run_input["sourceSubName"])
    this_run = adf_pipeline_run(ADF_OPTIC_PIPELINE, run_inputs=run_input)
    # Delete test container after tests
    storage_tool.register_deletion(config[mi.utils.config.MI_LANDING_KEY], test_container)
    storage_tool.register_deletion(config[mi.utils.config.MI_PERSISTENT_KEY], test_container)

    assert this_run.status == "Succeeded"
    verify_container(storage_tool, config, test_container, "2021-03/", 371)
    verify_container(storage_tool, config, test_container, "2021-04/", 18)
    validate_request_data(storage_tool, config, test_container)


def get_config(source, sub_source, start_date, end_date, config):
    return {
        'startDate': start_date,
        'endDate': end_date,
        'sourceName': 'test-adf-{}-{}'.format(config['BUILD_VERSION'], source),
        'sourceSubName': sub_source
    }