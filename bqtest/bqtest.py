"""Run a BQ test.
Usage:
  bqtest datatest <bq_table> [<private_key>]

Arguments:
  bq_table      the BigQuery table, in the form project.dataset.table
  private_key   (optional) the private key to be used for BigQuery auth
"""

import logging
import sys

from docopt import docopt

from helpers.bq_helper import query_view


def run_datatest(bq_path, private_key):
    result = query_view(bq_path, private_key, limit=2)

    if len(result) != 1:
        err_msg = "Data test result should only have one row."

        return {'success': False, 'err_msg': err_msg}

    result = result.pop()
    test_passed = all(result)

    if not test_passed:
        tests = dict(result.items())
        failed_tests = [test[0] for test in tests.items() if not test[1]]
        err_msg = "Failed in data tests: {}".format(', '.join(failed_tests))

        return {'success': False, 'err_msg': err_msg}

    return {'success': test_passed}


def cli_print_result(result):
    if result['success']:
        logging.info("Test passed")
    else:
        logging.info("Test failed. Reason: {}".format(result['err_msg']))


def main():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    args = docopt(__doc__)

    if args["datatest"]:
        result = run_datatest(bq_path=args['<bq_table>'], private_key=args['<private_key>'])
        cli_print_result(result)
    else:
        raise NotImplementedError("That feature is not yet implemented.")
