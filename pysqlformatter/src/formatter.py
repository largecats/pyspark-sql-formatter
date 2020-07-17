from __future__ import print_function  # for print() in Python 2
import re
from yapf.yapflib import yapf_api
from sparksqlformatter import Config, api
from pysqlformatter.src.tokenizer import Token, Tokenizer


class Formatter:
    '''
    Format a script with Python code and Spark SQL, HiveQL queries.
    '''
    def __init__(self, pythonStyle='pep8', sparksqlConfig=Config(), queryNames=['query']):
        '''
        Parameters
        pythonStyle: string
            A style name or path to a style config file; interface to https://github.com/google/yapf.
        sparksqlConfig: string, dict, or sparksqlformatter.src.config.Config() object
            Configurations for the query language; interface to https://github.com/largecats/sparksql-formatter.
        '''
        self.pythonStyle = pythonStyle
        self.sparksqlConfig = sparksqlConfig
        self.pointer = 0  # next position to read
        self.tokenizer = Tokenizer(queryNames=queryNames)

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
        formattedScript = ''
        for token in tokens:
            print('token.type = ' + token.type)
            print('token.value = ' + token.value)
            formattedScript += script[self.pointer:token.start]
            formattedQuery = api.format_query(token.value,
                                              self.sparksqlConfig)  # will get rid of starting/trailling blank spaces

            formattedQuery = Formatter.indent_query(formattedQuery, token.indent)
            if not script[(token.start - 3):token.start] in [
                    "'''",
                    '"""'
            ]:  # handle queries quoted by '' or "" that are formatted to multiline
                if '\n' in formattedQuery:
                    formattedScript = formattedScript[:-1] + "'''\n"  # remove starting ' or "
                    formattedScript += formattedQuery
                    formattedScript += '\n' + token.indent + "'''"
                    self.pointer = token.end + 1  # skip ending ' or "
                else:
                    formattedScript += formattedQuery
                    self.pointer = token.end
            else:
                formattedScript += '\n' + formattedQuery + '\n' + token.indent  # properly format between triple quotes
                self.pointer = token.end
        formattedScript += script[self.pointer:]
        self.reset()
        return yapf_api.FormatCode(formattedScript, style_config=self.pythonStyle)[0]

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
