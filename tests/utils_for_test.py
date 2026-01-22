import os
import json
from google.cloud import storage
from tap_tester import menagerie, connections


def get_resources_path(file_path, folder_path=None):
    """
    Get the full path to a resource file in the tests/resources directory.

    Args:
        file_path (str): Name of the resource file
        folder_path (str, optional): Subfolder within resources directory

    Returns:
        str: Full path to the resource file
    """
    if folder_path:
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources', folder_path, file_path)
    else:
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources', file_path)


def delete_and_push_file(properties, resource_names, folder_path=None, search_prefix_index=0):
    """
    Delete the file from GCS Bucket first and then upload it again.

    Args:
        properties (dict): config.json
        resource_names (list): List of file names (available in resources directory)
        folder_path (str, optional): Subfolder within resources directory
        search_prefix_index (int): Index of the table in tables config
    """
    # Initialize GCS client from environment variables
    from google.oauth2 import service_account

    # Get private key and replace literal \n with actual newlines
    private_key = os.getenv('TAP_GCS_PRIVATE_KEY', '')
    if private_key and '\\n' in private_key:
        private_key = private_key.replace('\\n', '\n')

    credentials_info = {
        'type': 'service_account',
        'project_id': os.getenv('TAP_GCS_PROJECT_ID'),
        'private_key_id': os.getenv('TAP_GCS_PRIVATE_KEY_ID'),
        'private_key': private_key,
        'client_email': os.getenv('TAP_GCS_CLIENT_EMAIL'),
        'client_id': os.getenv('TAP_GCS_CLIENT_ID'),
        'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
        'token_uri': 'https://oauth2.googleapis.com/token',
        'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs'
    }

    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    storage_client = storage.Client(credentials=credentials, project=credentials_info['project_id'])

    # Parse the tables configuration
    tables = json.loads(properties['tables'])
    bucket_name = properties['bucket']
    bucket = storage_client.bucket(bucket_name)

    for resource_name in resource_names:
        # Construct the GCS path
        search_prefix = tables[search_prefix_index].get('search_prefix', '')
        if search_prefix:
            gcs_path = search_prefix + '/' + resource_name
        else:
            gcs_path = resource_name

        blob = bucket.blob(gcs_path)        # Attempt to delete the file before we start
        print(f"Attempting to delete GCS file: {gcs_path}")
        try:
            blob.delete()
            print(f"Deleted existing file: {gcs_path}")
        except Exception as e:
            print(f"GCS File does not exist or could not be deleted: {e}")

        # Upload file to GCS bucket
        local_file_path = get_resources_path(resource_name, folder_path)
        blob.upload_from_filename(local_file_path)
        print(f"Uploaded file: {gcs_path}")


def get_file_handle(config, gcs_path):
    """
    Get a file handle for reading from GCS.

    Args:
        config (dict): Configuration with bucket and key_file
        gcs_path (str): Path to the file in GCS

    Returns:
        File-like object: Readable stream from GCS
    """
    bucket = config['bucket']
    storage_client = storage.Client.from_service_account_json(config['key_file'])

    bucket_obj = storage_client.bucket(bucket)
    blob = bucket_obj.blob(gcs_path)

    return blob.open('rb')


def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
    """
    Select all streams and all fields within streams.

    Args:
        conn_id: Connection ID from tap-tester
        catalogs: List of catalog entries
        select_all_fields (bool): If False, no fields will be selected
    """
    for catalog in catalogs:
        schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

        non_selected_properties = []
        if not select_all_fields:
            # Get a list of all properties so that none are selected
            non_selected_properties = schema.get('annotated-schema', {}).get(
                'properties', {}).keys()

        connections.select_catalog_and_fields_via_metadata(
            conn_id, catalog, schema, [], non_selected_properties)
