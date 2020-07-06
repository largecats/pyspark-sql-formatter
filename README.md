# pyspark-sql-formatter
A formatter for Pyspark code with SQL queries.

- [pyspark-sql-formatter](#pyspark-sql-formatter)

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
usage: pysqlformatter [-h] [-files FILES [FILES ...]] [-i] [--python-style PYTHON_STYLE] [--hiveql-config HIVEQL_CONFIG]

Formatter for Pyspark code and HiveQL queries.

optional arguments:
  -h, --help            show this help message and exit
  -files FILES [FILES ...]
                        Paths to files to format.
  -i, --inplace         Format the files in place.
  --python-style PYTHON_STYLE
                        Style for Python formatting, interface to https://github.com/google/yapf.
  --hiveql-config HIVEQL_CONFIG
                        Configurations for the query language, interface to https://github.com/largecats/hiveql-formatter.
```
E.g.,
```
$ pysqlformatter --python-style='pep8' --hiveql-config="{'reservedKeywordUppercase': False}" -files <path_to_file>
```
Or using config files:
```
$ pysqlformatter --python-style="<path_to_python_style_config_file>" --hiveql-config="<path_to_hiveql_config_file>" -files <path_to_file>
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
>>> api.format_file(<path_to_file>, inplace=False)
...
```