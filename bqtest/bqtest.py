"""Run a BQ test.
Usage:
  bqtest <a> <b>

Arguments:
  a     param a
  b     param b
"""

from docopt import docopt


def main():
    args = docopt(__doc__)
    a = args["<a>"]
    b = args["<b>"]
    print(str(a + b))
