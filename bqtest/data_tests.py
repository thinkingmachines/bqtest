import logging

from google.api_core.exceptions import GoogleAPIError

from bqtest.helpers.bq_helper import query_view, parse_bq_path, list_dataset_tables


def run_data_tests(bq_path, private_key):
    """
    Run data tests given the BQ path. The BQ path can either be a single view path, or a dataset path.
    - If the BQ path is a view, run that view as a data test.
    - If the path is a dataset, run all views in that dataset as data tests.
    :param bq_path: The view or dataset path
    :param private_key: Private key to be used for BQ auth
    :return: a list of test results
    """
    test_views = _get_data_tests(bq_path, private_key)
    results = [_run_single_data_test(view_path, private_key) for view_path in test_views]
    return results


def _get_data_tests(bq_path, private_key):
    """
    Get the list of data tests from the BQ path. If the BQ path is a view, run that view as a data test.
    If the path is a dataset, run all views in that dataset as data tests.
    :param bq_path: The view or dataset path
    :param private_key: Private key to be used for BQ auth
    :return: the list of view paths for data testing
    """
    is_dataset_path = parse_bq_path(bq_path)[2] is None
    if not is_dataset_path:
        return [bq_path]
    table_list = list_dataset_tables(bq_path, private_key)
    view_list = [t for t in table_list if t.table_type == 'VIEW']
    non_views = set(table_list) - set(view_list)
    if len(non_views) != 0:
        names = '\n'.join([t.full_table_id for t in non_views])
        logging.warning("Some tables were detected in the dataset. These will be ignored in testing:\n{}".format(names))
    return [v.full_table_id for v in view_list]


def _run_single_data_test(view_path, private_key):
    """
    Run a single data test view
    :param view_path: BQ path to the view
    :param private_key: Private key to be used for BQ auth
    :return: the result of testing the view
    """
    try:
        result = query_view(view_path, private_key, limit=2)
    except GoogleAPIError as e:
        return _create_test_result(view_path, False, str(e))

    if len(result) != 1:
        return _create_test_result(view_path, False, "Data test result should only have one row.")

    result_row = result.pop()
    test_passed = all(result_row.values())

    if not test_passed:
        tests = dict(result_row.items())
        failed_tests = [test[0] for test in tests.items() if not test[1]]
        err_msg = "Failed tests: {}".format(', '.join(failed_tests))
        return _create_test_result(view_path, False, err_msg)
    else:
        return _create_test_result(view_path, True)


def _create_test_result(test_name, success, err_msg=None):
    """
    Create a test result dictionary
    :param test_name: The name of the test
    :type test_name: str
    :param success: Whether the test succeeded
    :param err_msg: Error message for failed tests
    :return: the dictionary
    """
    return {
        'test_name': test_name,
        'success': success,
        'err_msg': err_msg,
    }
