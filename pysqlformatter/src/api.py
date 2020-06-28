from __future__ import print_function # for print() in Python 2
import sys
import re
import codecs
import logging

logger = logging.getLogger(__name__)
log_formatter = '[%(asctime)s] %(levelname)s [%(filename)s:%(lineno)s:%(funcName)s] %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=log_formatter)

def format_file(filename, formatter, inplace=False):
    script = read_from_file(filename)
    reformattedScript = format_script(script, formatter)
    if inplace: # overwrite file
        logger.info('Writing to ' + filename + '...')
        write_to_file(reformattedScript, filename)
    else: # write to stdout
        sys.stdout.write(reformattedScript)

def read_from_file(filename):
    with codecs.open(filename=filename, mode='r', encoding='utf-8') as f:
        text = f.read()
    return text

def write_to_file(reformattedScript, filename):
    with codecs.open(filename=filename, mode='w', encoding='utf-8') as f:
        f.write(reformattedScript)

def format_script(script, formatter):
    return formatter.format(script)

