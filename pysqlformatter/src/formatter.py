from __future__ import print_function  # for print() in Python 2
import re
from yapf.yapflib import yapf_api
from hiveqlformatter import Config, api
from pysqlformatter.src.tokenizer import Token, TokenType, Tokenizer


class Formatter:
    '''
    Format a script with Python code and Spark SQL, HiveQL queries.
    '''
    def __init__(self, pythonStyle='pep8', hiveqlConfig=Config()):
        '''
        Parameters
        pythonStyle: string
            A style name or path to a style config file; interface to https://github.com/google/yapf.
        hiveqlConfig: string, dict, or hiveqlformatter.src.config.Config() object
            Configurations for the query language; interface to https://github.com/largecats/hiveql-formatter.
        '''
        self.pythonStyle = pythonStyle
        self.hiveqlConfig = hiveqlConfig
        self.pointer = 0  # next position to read
        self.tokenizer = Tokenizer()

    def format(self, script):
        pythonReformatted = yapf_api.FormatCode(script, style_config=self.pythonStyle)[0]
        tokens = self.tokenizer.tokenize(pythonReformatted)  # get all strings passed to spark.sql() in the .py script
        return self.get_formatted_script_from_tokens(pythonReformatted, tokens)

    def get_formatted_script_from_tokens(self, script, tokens):
        '''
        Format the given script.

        Parameters
        scirpt: string
            The script to format.
        
        Return: string
            The formatted script.
        '''
        reformattedScript = ''
        for token in tokens:
            reformattedScript += script[self.pointer:token.start]
            reformattedQuery = api.format_query(token.value,
                                                self.hiveqlConfig)  # will get rid of starting/trailling blank spaces

            reformattedQuery = Formatter.indent_query(reformattedQuery, token.indent)
            if not script[(token.start - 3):token.start] in [
                    "'''", '"""'
            ]:  # handle queries quoted by '' or "" that are formatted to multiline
                if '\n' in reformattedQuery:
                    reformattedScript = reformattedScript[:-1] + "'''\n"  # remove starting ' or "
                    reformattedScript += reformattedQuery
                    reformattedScript += '\n' + token.indent + "'''"
                    self.pointer = token.end + 1  # skip ending ' or "
                else:
                    reformattedScript += reformattedQuery
                    self.pointer = token.end
            else:
                reformattedScript += '\n' + reformattedQuery + '\n' + token.indent  # properly format bewteen triple quotes
                self.pointer = token.end
        reformattedScript += script[self.pointer:]
        self.reset()
        return yapf_api.FormatCode(reformattedScript, style_config=self.pythonStyle)[0]

    @staticmethod
    def indent_query(query, indent):
        '''
        Indent given query when formatting.

        Parameters
        query: string
            The query to indent.
        indent: string
            The indentation.
        
        Return: string
            The indented query.
        '''
        lines = query.split('\n')
        lines = [indent + line for line in lines]
        indentedQuery = '\n'.join(lines)
        return indentedQuery

    def reset(self):
        '''
        Reset the formatter after finishing formatting a script.
        '''
        self.pointer = 0
