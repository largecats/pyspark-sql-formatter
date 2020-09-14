# -*- coding: utf-8 -*-
# MIT License

# Copyright (c) 2020-present largecats

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# from __future__ import print_function  # for print() in Python 2
import os
import sys
import argparse
import logging
import codecs
import json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pysqlformatter.src import api
from pysqlformatter.src.formatter import Formatter

logger = logging.getLogger(__name__)
log_formatter = '[%(asctime)s] %(levelname)s [%(filename)s:%(lineno)s:%(funcName)s] %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=log_formatter)


def main(argv):
    args = get_arguments(argv)
    pythonStyle = args['python_style']
    sparksqlStyle = args['sparksql_style']
    filePaths = args['files']
    queryNames = args['query_names']
    if filePaths:
        if sparksqlStyle:
            if pythonStyle:
                for filePath in filePaths:
                    api.format_file(filePath=filePath,
                                    pythonStyle=pythonStyle,
                                    sparksqlStyle=sparksqlStyle,
                                    queryNames=queryNames,
                                    inPlace=args.get('in_place'))
            else:
                for filePath in filePaths:
                    api.format_file(filePath=filePath,
                                    sparksqlStyle=sparksqlStyle,
                                    queryNames=queryNames,
                                    inPlace=args.get('in_place'))
        else:
            if pythonStyle:
                for filePath in filePaths:
                    api.format_file(filePath=filePath,
                                    pythonStyle=pythonStyle,
                                    queryNames=queryNames,
                                    inPlace=args.get('in_place'))
            else:
                for filePath in filePaths:
                    api.format_file(filePath=filePath, queryNames=queryNames, inPlace=args.get('in_place'))


def get_arguments(argv):
    '''
    Return arguments passed via command-line.

    Parameters:
    argv: list
        sys.argv
    
    Returns: dict
        A dictionary containing arguments for the formatter.
    '''
    parser = argparse.ArgumentParser(
        description=
        "Formatter for Pyspark code and SparkSQL queries. The queries should be in the form spark.sql(query) or spark.sql('xxx'). Cases like spark.sql('xxx'.format()), spark.sql('xxx'.replace()) may raise Exceptions."
    )

    parser.add_argument('-f', '--files', type=str, nargs='+', help='Paths to files to format.')

    parser.add_argument('-i', '--in-place', action='store_true', help='Format the files in place.')

    parser.add_argument('--python-style',
                        type=str,
                        default=None,
                        help='Style for Python formatting, interface to https://github.com/google/yapf.')

    parser.add_argument(
        '--sparksql-style',
        type=str,
        default=None,
        help="Style for SparkSQL formatting, interface to https://github.com/largecats/sparksql-formatter.")

    parser.add_argument(
        '--query-names',
        type=str,
        default=['query'],
        nargs='+',
        help="String variables with names containing these strings will be formatted as SQL queries. Default to 'query'."
    )

    args = vars(parser.parse_args(argv[1:]))

    return args


def run_main():
    main(sys.argv)


if __name__ == '__main__':
    run_main()
