import logging

from mi.utils.config import mi_config

LOG = logging.getLogger(__name__)


def test_optic_connectivity(adf_pipeline_run, mi_config):
    this_run = adf_pipeline_run("SMOKE_Optic_REST", run_inputs={})

    assert this_run.status == "Succeeded"
