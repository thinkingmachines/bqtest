# bqtest
bqtest is a CLI tool and python library for data warehouse testing in BigQuery.

Specifically, it supports:
- Unit testing of BigQuery views and queries
- Data testing of BigQuery tables

## Usage
Tests are defined in a config json file, which is passed to bqtest.

```bash
# Run all tests in the file
bqtest run tests.json

# Run a specific test in the file
bqtest run -n sales_check_totals tests.json
```

## Writing Tests

The config file specifies the list of tests to be ran.
Here is the basic structure of the file:

```
{
  "test_suite": "dw_etl",
  "cases": [
    {
      "name": "verify_sales_table_categories",
      "input": "bq-project:tests.sales_table_cats",
      "expected": "bq-project:tests.expected_sales_table_cats"
    },
    {
      "name": "unittest_metric_conversions",
      "input": {
        "view": "bq-project:kpi.monthly_sales_view",
        "test_dataset_prefix": "test_"
      },
      "expected": "bq-project:test_kpi.monthly_sales_view"
    },
    // ...and other test cases
  ]
}
```

## Unit Testing
```bash

```

## Data Testing
```bash

```

## Custom Usage
```bash
bqtest test 
```


## Development

Install package:

```bash
pip install .
```

Refresh package without dependencies:

```bash
pip install --upgrade --no-deps .
```
