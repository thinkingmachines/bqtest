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

from bqtest.data_tests import run_data_tests


def cli_print_result(results):
    for result in results:
        logging.info("Test name: {}".format(result['test_name']))
        if result['success']:
            logging.info("Result: PASS")
        else:
            logging.info("Result: FAIL. Reason: {}".format(result['err_msg']))


def main():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    args = docopt(__doc__)

    if args["datatest"]:
        results = run_data_tests(bq_path=args['<bq_table>'], private_key=args['<private_key>'])
        cli_print_result(results)
    else:
        raise NotImplementedError("That feature is not yet implemented.")
