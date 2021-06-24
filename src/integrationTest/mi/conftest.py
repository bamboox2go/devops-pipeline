"""Test fixtures"""

import pyodbc
from dotenv import load_dotenv
import pytest
import os
import time
import logging
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.identity import AzureCliCredential
from azure.mgmt.resource import SubscriptionClient

LOG = logging.getLogger(__name__)
load_dotenv(verbose=True)

# pytest_plugins = 'dataconnectors.blob_storage', 'dataconnectors.sql'

OPTIONAL_ARGS = ["AZ_SERVICE_PRINCIPAL_ID", "AZ_SERVICE_PRINCIPAL_SECRET"]


def pytest_addoption(parser):
    group = parser.getgroup('adf')
    group.addoption(
        '--sp_id',
        action='store',
        dest='AZ_SERVICE_PRINCIPAL_ID',
        help='Service Principal Id use to connect to Azure'
    )
    group.addoption(
        '--sp_password',
        action='store',
        dest='AZ_SERVICE_PRINCIPAL_SECRET',
        help='Service Principal password/secret use to connect to Azure'
    )
    group.addoption(
        '--sp_tenant_id',
        action='store',
        dest='AZ_SERVICE_PRINCIPAL_TENANT_ID',
        help='Tenant Id of Service Principal use to connect to Azure'
    )
    group.addoption(
        '--sub_id',
        action='store',
        dest='AZ_SUBSCRIPTION_ID',
        help='Azure subscription id of Azure Data Factory'
    )
    group.addoption(
        '--rg_name',
        action='store',
        dest='AZ_RESOURCE_GROUP_NAME',
        help='Azure resource group of Azure Data Factory'
    )
    group.addoption(
        '--adf_name',
        action='store',
        dest='AZ_DATAFACTORY_NAME',
        help='Azure Data Factory name'
    )
    group.addoption(
        '--poll_interval',
        action='store',
        dest='AZ_DATAFACTORY_POLL_INTERVAL_SEC',
        help='Poll interval to query Data Factory status in seconds'
    )
# TODO  fix  configuration fixure
# @pytest.fixture(scope="session")
# def adf_config(request):
#     # Order of importance
#     # 1. cmdline args
#     # 2. environment variables
#     def _default(val, default):
#         return default if val is None else val
#     r = request.config.option
#     DEFAULT_AZ_DATAFACTORY_POLL_INTERVAL_SEC = 5
#     config = {
#         "AZ_SERVICE_PRINCIPAL_ID": _default(r.AZ_SERVICE_PRINCIPAL_ID, os.getenv("AZ_SERVICE_PRINCIPAL_ID")),
#         "AZ_SERVICE_PRINCIPAL_SECRET": _default(r.AZ_SERVICE_PRINCIPAL_SECRET,
#                                                 os.getenv("AZ_SERVICE_PRINCIPAL_SECRET")),
#         "AZ_SERVICE_PRINCIPAL_TENANT_ID": _default(r.AZ_SERVICE_PRINCIPAL_TENANT_ID,
#                                                    os.getenv("AZ_SERVICE_PRINCIPAL_TENANT_ID")),
#         "AZ_SUBSCRIPTION_ID": _default(r.AZ_SUBSCRIPTION_ID, os.getenv("AZ_SUBSCRIPTION_ID")),
#         "AZ_RESOURCE_GROUP_NAME": _default(r.AZ_RESOURCE_GROUP_NAME, os.getenv("AZ_RESOURCE_GROUP_NAME")),
#         "AZ_DATAFACTORY_NAME": _default(r.AZ_DATAFACTORY_NAME, os.getenv("AZ_DATAFACTORY_NAME")),
#         "AZ_DATAFACTORY_POLL_INTERVAL_SEC": int(_default(r.AZ_DATAFACTORY_POLL_INTERVAL_SEC,
#                                                          os.getenv("AZ_DATAFACTORY_POLL_INTERVAL_SEC",
#                                                                    DEFAULT_AZ_DATAFACTORY_POLL_INTERVAL_SEC)))
#     }
#     # Ensure all required config is set.
#     for config_key, value in config.items():
#         if value is None and config_key not in OPTIONAL_ARGS:
#             raise ValueError("Required config: {config_key} is not set.".format(config_key=config_key))
#
#     return config


@pytest.fixture(scope="session")
def adf_config(request):
    # Order of importance
    # 1. cmdline args
    # 2. environment variables
    def _default(val, default):
        return default if val is None else val
    r = request.config.option
    DEFAULT_AZ_DATAFACTORY_POLL_INTERVAL_SEC = 5
    config = {
        "AZ_SERVICE_PRINCIPAL_ID": _default(None, os.getenv("AZ_SERVICE_PRINCIPAL_ID")),
        "AZ_SERVICE_PRINCIPAL_SECRET": _default(None,
                                                os.getenv("AZ_SERVICE_PRINCIPAL_SECRET")),
        "AZ_SERVICE_PRINCIPAL_TENANT_ID": _default(None,
                                                   os.getenv("AZ_SERVICE_PRINCIPAL_TENANT_ID")),
        "AZ_SUBSCRIPTION_ID": _default(None, os.getenv("AZ_SUBSCRIPTION_ID")),
        "AZ_RESOURCE_GROUP_NAME": _default(None, os.getenv("AZ_RESOURCE_GROUP_NAME")),
        "AZ_DATAFACTORY_NAME": _default(None, os.getenv("AZ_DATAFACTORY_NAME")),
        "AZ_DATAFACTORY_POLL_INTERVAL_SEC": int(_default(DEFAULT_AZ_DATAFACTORY_POLL_INTERVAL_SEC,
                                                         os.getenv("AZ_DATAFACTORY_POLL_INTERVAL_SEC",
                                                                   DEFAULT_AZ_DATAFACTORY_POLL_INTERVAL_SEC)))
    }
    # Ensure all required config is set.
    for config_key, value in config.items():
        if value is None and config_key not in OPTIONAL_ARGS:
            raise ValueError("Required config: {config_key} is not set.".format(config_key=config_key))

    return config

@pytest.fixture(scope="session")
def config():
    return {
        "SQL_SERVER_NAME": os.getenv("SQL_SERVER_NAME"),
        "SQL_SERVER_USERNAME": os.getenv("SQL_SERVER_USERNAME"),
        "SQL_SERVER_PASSWORD": os.getenv("SQL_SERVER_PASSWORD"),
        "SQL_DATABASE_NAME": os.getenv("SQL_DATABASE_NAME"),
    }


@pytest.fixture(scope="module")
def sql_connection(config):
    """Create a Connection Object object"""
    server = config["SQL_SERVER_NAME"]
    database = config["SQL_DATABASE_NAME"]
    username = config["SQL_SERVER_USERNAME"]
    password = config["SQL_SERVER_PASSWORD"]

    connection = """Driver={{ODBC Driver 17 for SQL Server}};Server=tcp:{dbServer}.database.windows.net,1433; \
    Database={database};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'"""\
        .format(dbServer=server, username=username, password=password, database=database)
    cnxn = pyodbc.connect(connection)

    yield cnxn
    cnxn.close()



@pytest.fixture(scope="session")
def adf_client(adf_config):
    """Creates an DataFactoryManagementClient object"""
    if "AZ_SERVICE_PRINCIPAL_ID" not in adf_config.keys() or adf_config["AZ_SERVICE_PRINCIPAL_ID"] is None:
        credentials = AzureCliCredential()
    else:
        credentials = ServicePrincipalCredentials(client_id=adf_config["AZ_SERVICE_PRINCIPAL_ID"],
                                                  secret=adf_config["AZ_SERVICE_PRINCIPAL_SECRET"],
                                                  tenant=adf_config["AZ_SERVICE_PRINCIPAL_TENANT_ID"])

    subscription_client = SubscriptionClient(credentials)
    sub = next(filter(lambda subscription: (subscription.display_name == adf_config["AZ_SUBSCRIPTION_ID"]),
                      subscription_client.subscriptions.list()))
    return DataFactoryManagementClient(credentials, sub.subscription_id)


@pytest.fixture(scope="session")
def adf_pipeline_run(adf_client, adf_config):
    """Factory function for triggering an ADF Pipeline run given run_inputs, and polls for results."""
    # Because ADF pipeline can be expensive to execute, you may want to cache pipeline_runs.
    # Cache pipeline runs are identified by pipeline_name and cached_run_name
    cached_pipeline_runs = {}

    def make_adf_pipeline_run(pipeline_name, run_inputs: dict, cached_run_name: str = "", rerun=False):
        # Check if pipeline run was already previous executed, simply return cached run
        if (cached_run_name != ""
            and not rerun
            and pipeline_name in cached_pipeline_runs.keys()
                and cached_run_name in cached_pipeline_runs[pipeline_name].keys()):
            LOG.info("""Previously executed cached pipeline run found for pipeline: {pipeline}
                     with run_name: {run_name}""".format(pipeline=pipeline_name, run_name=cached_run_name))
            return cached_pipeline_runs[pipeline_name][cached_run_name]

        # Trigger an ADF run
        az_resource_group = adf_config["AZ_RESOURCE_GROUP_NAME"]
        adf_name = adf_config["AZ_DATAFACTORY_NAME"]
        run_response = adf_client.pipelines.create_run(
            az_resource_group, adf_name, pipeline_name, parameters=run_inputs)
        pipeline_run = _poll_adf_until(adf_client, az_resource_group, adf_name, run_response.run_id,
                                       poll_interval=int(adf_config["AZ_DATAFACTORY_POLL_INTERVAL_SEC"]))

        # Store run in cache, if run_name is specified
        if cached_run_name != "":
            LOG.info("Caching pipeline: {pipeline} with run_name: {run_name}".format(
                pipeline=pipeline_name, run_name=cached_run_name))
            cached_pipeline_runs[pipeline_name] = {}
            cached_pipeline_runs[pipeline_name][cached_run_name] = pipeline_run
        return pipeline_run

    return make_adf_pipeline_run


def _poll_adf_until(adf_client, az_resource_group, adf_name, pipeline_run_id,
                    until_status=["Succeeded", "TimedOut", "Failed", "Cancelled"], poll_interval=5):
    """Helper function that polls ADF pipeline until specific status is achieved.
    Warning! may continue polling indefinitely if until_status is not set correctly.

    Args:
        adf_client (DataFactoryManagementClient): Azure Data Factory Management Client
        az_resource_group (str): Name of Azure Resource Group
        adf_name: Name of Azure Data Factory (ADF)
        pipeline_run_id (int): Run id of ADF pipeline to poll
        until_status (list[str], optional): List of ADF pipeline run status that will trigger end of polling.
            Defaults to ["Succeeded", "TimedOut", "Failed", "Cancelled"]
        poll_interval (int, optional): Poll interval in seconds. Defaults to 5.

    Returns:
        PipelineRun
    """
    pipeline_run_status = ""
    while (pipeline_run_status not in until_status):
        LOG.info("Polling pipeline with run id {} for status in {}".format(pipeline_run_id, ", ".join(until_status)))
        pipeline_run = adf_client.pipeline_runs.get(
            az_resource_group, adf_name, pipeline_run_id)
        pipeline_run_status = pipeline_run.status  # InProgress, Succeeded, Queued, TimedOut, Failed, Cancelled
        time.sleep(poll_interval)
    return pipeline_run
