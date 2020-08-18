# from __future__ import print_function  # for print() in Python 2
from io import open
import sys
import re
import logging

from pysqlformatter.src.formatter import Formatter
from sparksqlformatter import Style as sparksqlStyle
from sparksqlformatter import api as sparksqlAPI

logger = logging.getLogger(__name__)
log_formatter = '[%(asctime)s] %(levelname)s [%(filePath)s:%(lineno)s:%(funcName)s] %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=log_formatter)


def format_file(filePath, pythonStyle='pep8', sparksqlStyle=sparksqlStyle(), queryNames=['query'], inPlace=False):
    '''
    Format file with given settings for python style and sparksql configurations.

    Parameters
    filePath: string
        Path to the file to format.
    pythonStyle: string
        A style name or path to a style config file; interface to https://github.com/google/yapf.
    sparksqlStyle: string, dict, or sparksqlformatter.src.style.Style() object
        Configurations for the query language; interface to https://github.com/largecats/sparksql-formatter.
    queryNames: list

    inPlace: bool
        If True, will format the file in place.
        Else, will write the formatted file to stdout.

    Return: None
    '''
    if type(sparksqlStyle) == type(sparksqlStyle):
        formatter = Formatter(pythonStyle=pythonStyle, sparksqlStyle=sparksqlStyle)
    else:
        if type(sparksqlStyle) == str:
            if sparksqlStyle.startswith('{'):
                sparksqlStyle = eval(sparksqlStyle)
                formatter = Formatter(pythonStyle=pythonStyle,
                                      sparksqlStyle=sparksqlAPI._create_style_from_dict(sparksqlStyle),
                                      queryNames=queryNames)
            else:
                formatter = Formatter(pythonStyle=pythonStyle,
                                      sparksqlStyle=sparksqlAPI._create_style_from_file(sparksqlStyle))
        elif type(sparksqlStyle) == dict:
            formatter = Formatter(pythonStyle=pythonStyle,
                                  sparksqlStyle=sparksqlAPI._create_style_from_dict(sparksqlStyle),
                                  queryNames=queryNames)
        else:
            raise Exception('Unsupported config type')
    _format_file(filePath, formatter, inPlace)


def format_script(script, pythonStyle='pep8', sparksqlStyle=sparksqlStyle(), queryNames=['query']):
    '''
    Format script using given settings for python style and sparksql configurations.

    Parameters
    script: string
        The script to be formatted.
    pythonStyle: string
        A style name or path to a style config file; interface to https://github.com/google/yapf.
    sparksqlStyle: string, dict, or sparksqlformatter.src.style.Style() object
        Configurations for the query language; interface to https://github.com/largecats/sparksql-formatter.
    
    Return: string
        The formatted script.
    '''
    if type(sparksqlStyle) == type(sparksqlStyle):
        formatter = Formatter(pythonStyle=pythonStyle, sparksqlStyle=sparksqlStyle, queryNames=queryNames)
    else:
        if type(sparksqlStyle) == str:
            if sparksqlStyle.startswith('{'):
                sparksqlStyle = eval(sparksqlStyle)
                formatter = Formatter(pythonStyle=pythonStyle,
                                      sparksqlStyle=sparksqlAPI._create_style_from_dict(sparksqlStyle),
                                      queryNames=queryNames)
            else:
                formatter = Formatter(pythonStyle=pythonStyle,
                                      sparksqlStyle=sparksqlAPI._create_style_from_file(sparksqlStyle),
                                      queryNames=queryNames)
        elif type(sparksqlStyle) == dict:
            formatter = Formatter(pythonStyle=pythonStyle,
                                  sparksqlStyle=sparksqlAPI._create_style_from_dict(sparksqlStyle),
                                  queryNames=queryNames)
        else:
            raise Exception('Unsupported config type')
    return _format_script(script, formatter)


def _format_file(filePath, formatter, inPlace=False):
    '''
    The I/O helper function for format_file(). Read from given file, format it, and write to specified output.

    Parameters
    filePath: string
        Path to the file to format.
    formatter: pysqlformatter.src.formatter.Formatter() object
        Formatter.
    inPlace: bool
        If True, will format the file in place.
        Else, will write the formatted file to stdout.
    
    Return: None
    '''
    script = _read_from_file(filePath)
    formattedScript = _format_script(script, formatter)
    if inPlace:  # overwrite file
        logger.info('Writing to ' + filePath + '...')
        _write_to_file(formattedScript, filePath)
    else:  # write to stdout
        sys.stdout.write(formattedScript)


def _read_from_file(filePath):
    '''
    The input helper function for _format_file(). Read from given file and return its content.

    Parameters
    filePath: string
        Path to the file to format.
    
    Return: string
        The file content.
    '''
    # see https://docs.python.org/3.5/library/functions.html#open
    with open(file=filePath, mode='r', newline=None, encoding='utf-8') as f:
        text = f.read()
    return text


def _write_to_file(formattedQuery, filePath):
    '''
    The output helper function for _format_file(). Write formatted query to given file.

    Parameters
    formattedQuery: string
        The formatted query.
    filePath: string
        Path to the file to write to.
    '''
    # see https://docs.python.org/3.5/library/functions.html#open
    with open(file=filePath, mode='w', newline='\n', encoding='utf-8') as f:
        f.write(formattedQuery)


def _format_script(script, formatter):
    '''
    The wrapper function for format_script(). Format a given script using given formatter.

    Parameters
    string: string
        The script to format.
    formatter: sparksqlformatter.src.formatter.Formatter() object
        Formatter.
    
    Return: string
        The formatted script.
    '''
    return formatter.format(script)
