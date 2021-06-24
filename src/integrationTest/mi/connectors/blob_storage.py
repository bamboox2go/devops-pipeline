import os
import logging
import pytest
from azure.storage.blob import BlobServiceClient

LOG = logging.getLogger(__name__)
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


class StorageUtil:
    def __init__(self, request):
        self.request = request

    def list_blobs(self, con_str, container, prefix=None):
        blob_service_client = BlobServiceClient.from_connection_string(con_str)
        container_client = blob_service_client.get_container_client(container)
        blob_list = container_client.list_blobs(name_starts_with=prefix)
        blobs = []
        for blob in blob_list:
            blobs.append(blob['name'])
        return blobs

    """Register container to be deleted after test execution"""
    def register_deletion(self, con_str, container):
        blob_service_client = BlobServiceClient.from_connection_string(con_str)

        def finalizer():
            _delete_container(blob_service_client, container)
        self.request.addfinalizer(finalizer)

    def read_blob_data(self, con_str, container, blob):
        blob_service_client = BlobServiceClient.from_connection_string(con_str)
        return _read_blob(blob_service_client, container, blob)

    def get_blob_lines(self, con_str, container, blob):
        blob_service_client = BlobServiceClient.from_connection_string(con_str)
        read_data = _read_blob(blob_service_client, container, blob)
        return len(read_data.splitlines())


# @pytest.fixture(scope="session")
# def blob_data(config, request):
#     """Factory function for uploading input data to blob storage"""
#     def make_blob_test_data(src_file_path, dest_file_path, dest_cnstr, dest_container):
#         blob_service_client = BlobServiceClient.from_connection_string(dest_cnstr)
#         # Upload to blob
#         _upload_to_blob(blob_service_client, dest_container,
#                         local_file_path=os.path.join(DATA_DIR, src_file_path), dest_file_path=dest_file_path)
#
#         # Add finalizer that deletes test data from blob storage
#         def finalizer():
#             _delete_from_blob(blob_service_client, dest_container, dest_file_path)
#         request.addfinalizer(finalizer)
#         return dest_container, dest_file_path
#
#     return make_blob_test_data


@pytest.fixture(scope="session")
def blob_data(request):
    """Factory function for uploading input data to blob storage"""
    def read_blob_test_data(con_str, container, blob):
        blob_service_client = BlobServiceClient.from_connection_string(con_str)
        read_data = _read_blob(blob_service_client, container, blob)
        return read_data

    return read_blob_test_data


@pytest.fixture(scope="session")
def storage_tool(request):
    return StorageUtil(request)


def _read_blob(blob_service_client, container, blob):
    blob_client = blob_service_client.get_blob_client(container, blob)
    all_data = blob_client.download_blob().readall()
    data = all_data.decode('utf-8-sig')
    return data


def _upload_to_blob(blob_service_client, container, local_file_path,
                    dest_file_path, create_container=True, overwrite=True):
    """Helper function that uploads a file to blob storage.

    Parameters:
        blob_service_client (BlobServiceClient): Azure Blob Service Client
        container (str): Name of dest storage container.
        local_file_path (str): File path to local file
        dest_file_path (str): "File path" to blob storage destination
        create_container (bool, optional): Whether to create container if not exists. Default: True
        overwrite (bool, optional): Whether to overwrite destination file.
    Returns:
        dict[str, Any]: Blob-updated property dict (Etag and last modified)
    """
    if create_container and not _container_exists(blob_service_client, container):
        LOG.info("Creating container: {}".format(container))
        blob_service_client.create_container(container)
    blob_client = blob_service_client.get_blob_client(container=container, blob=dest_file_path)
    with open(local_file_path, "rb") as data:
        out = blob_client.upload_blob(data, overwrite=overwrite)
    return out


def _delete_from_blob(blob_service_client, container, file_path):
    """Helper function to delete a file in blob storage

    Args:
        blob_service_client (BlobServiceClient): Azure Blob Service Client
        container (str): Name of storage container.
        file_path (str): File path to file in blob storage
    Returns:
        None
    """
    if _blob_exists(blob_service_client, container, file_path):
        blob_client = blob_service_client.get_blob_client(container=container, blob=file_path)
        blob_client.delete_blob()


def _delete_container(blob_service_client, container):
    """Helper function to delete a container in blob storage

    Args:
        blob_service_client (BlobServiceClient): Azure Blob Service Client
        container (str): Name of storage container.
    Returns:
        None
    """
    # if _blob_exists(blob_service_client, container, file_path):
    blob_client = blob_service_client.get_container_client(container=container)
    blob_client.delete_container()


def _blob_exists(blob_service_client, container, file_path):
    """Check if blob exists"""
    blob_container_client = blob_service_client.get_container_client(container)
    return next(blob_container_client.list_blobs(file_path), None) is not None


def _container_exists(blob_service_client, container):
    """Check if container exists"""
    return next(blob_service_client.list_containers(container), None) is not None
