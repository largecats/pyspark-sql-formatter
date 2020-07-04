from __future__ import print_function # for print() in Python 2
import sys
import re
import codecs
import logging

from pysqlformatter.src.formatter import Formatter
from hiveqlformatter import Config, api

logger = logging.getLogger(__name__)
log_formatter = '[%(asctime)s] %(levelname)s [%(filename)s:%(lineno)s:%(funcName)s] %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=log_formatter)

def format_file(filename, pythonStyle='pep8', hiveqlConfig=Config(), inplace=False):
    if type(hiveqlConfig) == type(hiveqlConfig):
        formatter = Formatter(pythonStyle=pythonStyle, hiveqlConfig=hiveqlConfig)
    else:
        if type(hiveqlConfig) == str:
            if hiveqlConfig.startswith('{'):
                hiveqlConfig = eval(hiveqlConfig)
                formatter = Formatter(pythonStyle=pythonStyle, hiveqlConfig=api._create_config_from_dict(hiveqlConfig))
            else:
                formatter = Formatter(pythonStyle=pythonStyle, hiveqlConfig=api._create_config_from_file(hiveqlConfig))
        elif type(hiveqlConfig) == dict:
            formatter = Formatter(pythonStyle=pythonStyle, hiveqlConfig=api._create_config_from_dict(hiveqlConfig))
        else:
            raise Exception('Unsupported config type')
    _format_file(filename, formatter, inplace)

def format_script(script, pythonStyle='pep8', hiveqlConfig=Config()):
    if type(hiveqlConfig) == type(hiveqlConfig):
        formatter = Formatter(pythonStyle=pythonStyle, hiveqlConfig=hiveqlConfig)
    else:
        if type(hiveqlConfig) == str:
            if hiveqlConfig.startswith('{'):
                hiveqlConfig = eval(hiveqlConfig)
                formatter = Formatter(pythonStyle=pythonStyle, hiveqlConfig=api._create_config_from_dict(hiveqlConfig))
            else:
                formatter = Formatter(pythonStyle=pythonStyle, hiveqlConfig=api._create_config_from_file(hiveqlConfig))
        elif type(hiveqlConfig) == dict:
            formatter = Formatter(pythonStyle=pythonStyle, hiveqlConfig=api._create_config_from_dict(hiveqlConfig))
        else:
            raise Exception('Unsupported config type')
    return _format_script(script, formatter)

def _format_file(filename, formatter, inplace=False):
    script = api._read_from_file(filename)
    reformattedScript = _format_script(script, formatter)
    if inplace: # overwrite file
        logger.info('Writing to ' + filename + '...')
        api._write_to_file(reformattedScript, filename)
    else: # write to stdout
        sys.stdout.write(reformattedScript)

def _format_script(script, formatter):
    return formatter.format(script)