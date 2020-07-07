from __future__ import print_function  # for print() in Python 2
import sys
import re
import codecs
import logging

from pysqlformatter.src.formatter import Formatter
from hiveqlformatter import Config as hiveqlConfig
from hiveqlformatter import api as hiveqlAPI

logger = logging.getLogger(__name__)
log_formatter = '[%(asctime)s] %(levelname)s [%(filePath)s:%(lineno)s:%(funcName)s] %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=log_formatter)


def format_file(filePath, pythonStyle='pep8', hiveqlConfig=hiveqlConfig(), inplace=False):
    '''
    Format file with given settings for python style and hiveql configurations.

    Parameters
    filePath: string
        Path to the file to format.
    pythonStyle: string
        A style name or path to a style config file; interface to https://github.com/google/yapf.
    hiveqlConfig: string, dict, or hiveqlformatter.src.config.Config() object
        Configurations for the query language; interface to https://github.com/largecats/hiveql-formatter.
    inplace: bool
        If True, will format the file in place.
        Else, will write the formatted file to stdout.

    Return: None
    '''
    if type(hiveqlConfig) == type(hiveqlConfig):
        formatter = Formatter(pythonStyle=pythonStyle, hiveqlConfig=hiveqlConfig)
    else:
        if type(hiveqlConfig) == str:
            if hiveqlConfig.startswith('{'):
                hiveqlConfig = eval(hiveqlConfig)
                formatter = Formatter(pythonStyle=pythonStyle,
                                      hiveqlConfig=hiveqlAPI._create_config_from_dict(hiveqlConfig))
            else:
                formatter = Formatter(pythonStyle=pythonStyle,
                                      hiveqlConfig=hiveqlAPI._create_config_from_file(hiveqlConfig))
        elif type(hiveqlConfig) == dict:
            formatter = Formatter(pythonStyle=pythonStyle,
                                  hiveqlConfig=hiveqlAPI._create_config_from_dict(hiveqlConfig))
        else:
            raise Exception('Unsupported config type')
    _format_file(filePath, formatter, inplace)


def format_script(script, pythonStyle='pep8', hiveqlConfig=hiveqlConfig()):
    '''
    Format script using given settings for python style and hiveql configurations.

    Parameters
    script: string
        The script to be formatted.
    pythonStyle: string
        A style name or path to a style config file; interface to https://github.com/google/yapf.
    hiveqlConfig: string, dict, or hiveqlformatter.src.config.Config() object
        Configurations for the query language; interface to https://github.com/largecats/hiveql-formatter.
    
    Return: string
        The formatted script.
    '''
    if type(hiveqlConfig) == type(hiveqlConfig):
        formatter = Formatter(pythonStyle=pythonStyle, hiveqlConfig=hiveqlConfig)
    else:
        if type(hiveqlConfig) == str:
            if hiveqlConfig.startswith('{'):
                hiveqlConfig = eval(hiveqlConfig)
                formatter = Formatter(pythonStyle=pythonStyle,
                                      hiveqlConfig=hiveqlAPI._create_config_from_dict(hiveqlConfig))
            else:
                formatter = Formatter(pythonStyle=pythonStyle,
                                      hiveqlConfig=hiveqlAPI._create_config_from_file(hiveqlConfig))
        elif type(hiveqlConfig) == dict:
            formatter = Formatter(pythonStyle=pythonStyle,
                                  hiveqlConfig=hiveqlAPI._create_config_from_dict(hiveqlConfig))
        else:
            raise Exception('Unsupported config type')
    return _format_script(script, formatter)


def _format_file(filePath, formatter, inplace=False):
    '''
    The I/O helper function for format_file(). Read from given file, format it, and write to specified output.

    Parameters
    filePath: string
        Path to the file to format.
    formatter: pysqlformatter.src.formatter.Formatter() object
        Formatter.
    inplace: bool
        If True, will format the file in place.
        Else, will write the formatted file to stdout.
    
    Return: None
    '''
    script = _read_from_file(filePath)
    reformattedScript = _format_script(script, formatter)
    if inplace:  # overwrite file
        logger.info('Writing to ' + filePath + '...')
        _write_to_file(reformattedScript, filePath)
    else:  # write to stdout
        sys.stdout.write(reformattedScript)


def _read_from_file(filePath):
    '''
    The input helper function for _format_file(). Read from given file and return its content.

    Parameters
    filePath: string
        Path to the file to format.
    
    Return: string
        The file content.
    '''
    with open(filename=filePath, mode='r', newline='', encoding='utf-8') as f:
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
    with open(filename=filePath, mode='w', newline='', encoding='utf-8') as f:
        f.write(formattedQuery)


def _format_script(script, formatter):
    '''
    The wrapper function for format_script(). Format a given script using given formatter.

    Parameters
    string: string
        The script to format.
    formatter: hiveqlformatter.src.formatter.Formatter() object
        Formatter.
    
    Return: string
        The formatted script.
    '''
    return formatter.format(script)
