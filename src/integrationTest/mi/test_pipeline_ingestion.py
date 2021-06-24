import logging

LOG = logging.getLogger(__name__)

import os
from pathlib import Path

import pytest

import mi
from mi.connectors.blob_storage import blob_data
from mi.utils.config import mi_config

EXPECTED_FILE_PATH = Path(__file__).resolve().parent.parent / 'resources/metadata_result.json'


@pytest.mark.skip(reason="Test not ready")
def test_create_metadata_file_pipeline(adf_pipeline_run, blob_data, blob_config):
    """Test that pipeline has data"""
    test_container = "test-adf-metadata"
    this_run = adf_pipeline_run("createMetadataFilePipeline", run_inputs={'containerName': 'test-adf-metadata'})

    assert this_run.status == "Succeeded"
    LOG.info(os.path.abspath(__file__))
    blob_content = blob_data(blob_config[mi.utils.config.MI_LANDING_KEY], test_container, "_metadata.json", False)
    assert_file_content(EXPECTED_FILE_PATH, blob_content)


def assert_file_content(file, content):
    with open(file, 'r', encoding='utf-8') as file_content:
        expected_content = file_content.read()
        assert expected_content == content.strip()
