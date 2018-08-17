import re
import json

from google.cloud import bigquery
from google.oauth2 import service_account


def parse_bq_path(bq_path):
    """
    Parses a BigQuery table with the ff. patterns:
      - project-name:dataset_name.table_name
      - project-name.dataset_name.table_name
    :param bq_path: the BigQuery table path
    :return: the project, dataset, and table names as a tuple
    """
    split = tuple(re.split("[.:]", bq_path))
    length = len(split)
    if length not in (2, 3):
        raise ValueError('Invalid bq path: {}'.format(bq_path))
    if length == 2:
        return (None,) + split
    return split


def get_table(bq_path, private_key):
    """
    Gets the BigQuery table object
    :param bq_path: the BigQuery path
    :type bq_path: str
    :param private_key: the path to the private key. Uses the application default credentials if not passed.
    :type private_key: str
    :return:
    """
    project, dataset, table = parse_bq_path(bq_path)
    client = get_bq_client(bq_path=bq_path, private_key=private_key)
    table_ref = client.dataset(dataset).table(table)
    return client.get_table(table_ref), client


def get_bq_client(bq_path=None, project=None, private_key=None):
    """
    Creates the BigQuery Client object
    :param bq_path: the BigQuery path
    :param project: the project name. Optional if the project name is specified in the BigQuery path
    :param private_key: the string contents of the private key, or the path to the private key.
    Uses the application default credentials if not passed.
    :return:
    """
    path_project, _, _ = parse_bq_path(bq_path)
    if project is not None and project != path_project:
        raise ValueError('Conflicting project values: {} and {}'.format(bq_path, project))
    project = path_project
    try:
        key_dict = json.loads(private_key)
        credentials = service_account.Credentials.from_service_account_info(key_dict)
    except ValueError:
        credentials = service_account.Credentials.from_service_account_file(private_key)
    return bigquery.Client(project=project, credentials=credentials)


def query_view(view_path, private_key, limit=None):
    """
    Gets the rows from a view
    :param view_path: the BigQuery path to the view
    :param private_key: the path to the private key. Uses the application default credentials if not passed.
    :param limit: the maximum number of rows to be downloaded
    :return: the rows of the view
    """
    client = get_bq_client(bq_path=view_path, private_key=private_key)
    job_config = bigquery.QueryJobConfig()
    query = 'SELECT * FROM `{}`'.format(view_path)
    if limit is not None:
        query += " LIMIT {}".format(limit)
    return list(client.query(query, job_config=job_config))
