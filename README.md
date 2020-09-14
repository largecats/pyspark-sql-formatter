# pyspark-sql-formatter
A formatter for Pyspark code with SQL queries. It relies on Python formatter [yapf](https://github.com/google/yapf) and SparkSQL formatter [sparksqlformatter](https://github.com/largecats/sparksql-formatter), both working indepdendently. User can specify configurations for either formatter separately.

The queries should be in the form `spark.sql(query)` or `spark.sql('xxx')`. Cases like `spark.sql('xxx'.format())`, `spark.sql('xxx'.replace())` may raise Exceptions.

- [pyspark-sql-formatter](#pyspark-sql-formatter)
- [Installation](#installation)
  - [Install using pip](#install-using-pip)
  - [Install from source](#install-from-source)
- [Compatibility](#compatibility)
- [Usage](#usage)
  - [Use as command-line tool](#use-as-command-line-tool)
  - [Use as Python library](#use-as-python-library)

# Installation

## Install using pip
```
pip install pysqlformatter
```

## Install from source
1. Download source code.
2. Navigate to the source code directory.
3. Do `python setup.py install` or `pip install .`.

# Compatibility
Supports Python 2.7 and 3.6+.

# Usage
`pysqlformatter` can be used as either a command-line tool or a Python library.

## Use as command-line tool
```
usage: pysqlformatter [-h] [-f FILES [FILES ...]] [-i] [--query-names QUERY_NAMES [QUERY_NAMES ...]] [--python-style PYTHON_STYLE] [--sparksql-style SPARKSQL_CONFIG]

Formatter for Pyspark code and SparkSQL queries.

optional arguments:
  -h, --help            show this help message and exit
  -f FILES [FILES ...], --files FILES [FILES ...]
                        Paths to files to format.
  -i, --in-place        Format the files in place.
  --python-style PYTHON_STYLE
                        Style for Python formatting, interface to https://github.com/google/yapf.
  --sparksql-style SPARKSQL_CONFIG
                        Style for SparkSQL formatting, interface to https://github.com/largecats/sparksql-formatter.
  --query-names QUERY_NAMES [QUERY_NAMES ...]
                        String variables with names containing these strings will be formatted as SQL queries. Default to 'query'.
```
E.g.,
```
$ pysqlformatter -f <path_to_file> --python-style='pep8' --sparksql-style="{'reservedKeywordUppercase': False}" --query-names query
```
Or using config files:
```
$ pysqlformatter -f <path_to_file> --python-style="<path_to_python_style_config_file>" --sparksql-style="<path_to_sparksql_config_file>" --query-names query
```

## Use as Python library

Call `pysqlformatter.api.format_script()` to format script passed as string:
```
>>> from pysqlformatter import api
>>> script = '''query = 'select * from t0'\nspark.sql(query)'''
>>> api.format_script(script=script, pythonStyle='pep8', sparksqlConfig=sparksqlConfig(), queryNames=['query'])
"query = '''\nSELECT\n    *\nFROM\n    t0\n'''\nspark.sql(query)\n"
```
Call `pysqlformatter.api.format_file()` to format script in file:
```
>>> from pysqlformatter import api
>>> api.format_file(filePath=<path_to_file>, pythonStyle='pep8', sparksqlConfig=sparksqlConfig(), queryNames=['query'], inPlace=False)
...
```