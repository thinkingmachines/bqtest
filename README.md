# bqtest
bqtest is a CLI tool and python library for data warehouse testing in BigQuery.

Specifically, it supports:
- Unit testing of BigQuery views and queries
- Data testing of BigQuery tables

## Usage

```bash
bqtest datatest cloversense-dashboard.data_tests secrets/key.json
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
