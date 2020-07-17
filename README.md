# pyspark-sql-formatter
A formatter for Pyspark code with SQL queries. It relies on Python formatter [yapf](https://github.com/google/yapf) and HiveQL formatter [sparksqlformatter](https://github.com/largecats/sparksql-formatter), both working indepdendently. User can specify configurations for either language separately.

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
View package at https://test.pypi.org/project/pysqlformatter-largecats.
```
python3 -m pip install --index-url https://test.pypi.org/simple/ --no-deps pysqlformatter-largecats
```
TBD:
```
pip install pysqlformatter
```

## Install from source
See [here](https://docs.python.org/2/install/index.html#splitting-the-job-up).
```
python setup.py install
```

# Compatibility
Supports Python 2.7 and 3.6+.

# Usage
`pysqlformatter` can be used as either a command-line tool or a Python library.

## Use as command-line tool
```
usage: pysqlformatter [-h] [-f FILES [FILES ...]] [-i] [--query-names QUERY_NAMES [QUERY_NAMES ...]] [--python-style PYTHON_STYLE] [--sparksql-config SPARKSQL_CONFIG]

Formatter for Pyspark code and SparkSQL queries.

optional arguments:
  -h, --help            show this help message and exit
  -f FILES [FILES ...], --files FILES [FILES ...]
                        Paths to files to format.
  -i, --in-place        Format the files in place.
  --query-names QUERY_NAMES [QUERY_NAMES ...]
                        Strings contained by variable names of queries to format. Default to 'query'.
  --python-style PYTHON_STYLE
                        Style for Python formatting, interface to https://github.com/google/yapf.
  --sparksql-config SPARKSQL_CONFIG
                        Configurations for the query language, interface to https://github.com/largecats/sparksql-formatter.
```
E.g.,
```
$ pysqlformatter --python-style='pep8' --sparksql-config="{'reservedKeywordUppercase': False}" -f <path_to_file>
```
Or using config files:
```
$ pysqlformatter --python-style="<path_to_python_style_config_file>" --sparksql-config="<path_to_sparksql_config_file>" -f <path_to_file>
```

## Use as Python library
The module can also be used as a Python library.

Call `pysqlformatter.api.format_script()` to format script passed as string:
```
>>> from pysqlformatter import api
>>> script = '''query = 'select * from t0'\nspark.sql(query)'''
>>> api.format_script(script)
"query = '''\nSELECT\n    *\nFROM\n    t0\n'''\nspark.sql(query)\n"
```
Call `pysqlformatter.api.format_file()` to format script in file:
```
>>> from pysqlformatter import api
>>> api.format_file(<path_to_file>, inPlace=False)
...
```