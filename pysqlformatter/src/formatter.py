from __future__ import print_function # for print() in Python 2
import re
from yapf.yapflib import yapf_api
from hiveqlformatter import HiveqlFormatter, Config, api
from pysqlformatter.src.token import Token

class Formatter:
    '''
    Format a script with Python code and Spark SQL, HiveQL queries.
    '''

    def __init__(self, pythonStyle='pep8', hiveqlFormatterConfig=Config()):
        self.pythonStyle = pythonStyle
        self.hiveqlFormatter = HiveqlFormatter(hiveqlFormatterConfig)
        self.pointer = 0 # next position to read

    def format(self, script):
        pythonReformatted = yapf_api.FormatCode(script, style_config=self.pythonStyle)[0]
        queryMatchTokens = self.get_query_strings(pythonReformatted) # get all strings passed to spark.sql() in the .py script
        
        reformattedScript = ''
        for token in queryMatchTokens:
            # print('self.pointer = ' + str(self.pointer))
            # print('token = ' + token.value)
            reformattedScript += pythonReformatted[self.pointer: token.start]
            reformattedQuery = api.format_query(token.value, self.hiveqlFormatter)
            if not pythonReformatted[(token.start-2):token.start] in ["'''", '"""']: # handle queries quoted by '' or "" that are formatted to multiline
                if '\n' in reformattedQuery:
                    reformattedScript = reformattedScript[:-1] + "'''\n" # remove starting ' or "
                    reformattedScript += reformattedQuery
                    reformattedScript += "\n'''"
                    self.pointer = token.end + 1 # skip ending ' or "
                else:
                    reformattedScript += reformattedQuery
                    self.pointer = token.end + 1
            else:
                reformattedScript += reformattedQuery
                self.pointer = token.end + 1
        reformattedScript += pythonReformatted[self.pointer:]
        self.reset()
        return reformattedScript
    
    def get_query_strings(self, pythonReformatted):
        sparkSqlMatchObjs = Formatter.get_spark_sql_args(pythonReformatted)
        queryMatchTokens = []
        for match in sparkSqlMatchObjs:
            if Formatter.is_query(match.groups()[0]): # e.g., spark.sql('select * from t0')
                if match.groups()[0].startswith("'''") or match.groups()[0].startswith('"""'):
                    matchTokenWithoutQuotes = Token(
                                            value=match.groups()[0].strip('"').strip("'"),
                                            start=match.start(1)+3,
                                            end=match.end(1)-3
                                            )
                else:
                    matchTokenWithoutQuotes = Token(
                                            value=match.groups()[0].strip('"').strip("'"),
                                            start=match.start(1)+1,
                                            end=match.end(1)-1
                                            )
                queryMatchTokens.append(matchTokenWithoutQuotes)
            else: # e.g., spark.sql(query1)
                queryMatchObj = Formatter.get_query_from_variable_name(match.groups()[0], pythonReformatted[:match.start(1)])
                queryMatchToken = Formatter.create_token_from_match(queryMatchObj)
                queryMatchTokens.append(queryMatchToken)
        return queryMatchTokens

    @staticmethod
    def get_spark_sql_args(pythonReformatted):
        sparkSqlRegex = 'spark\.sql\((.*?)\)'
        return re.finditer(sparkSqlRegex, pythonReformatted, flags=re.DOTALL)
    
    @staticmethod
    def is_query(text):
        return text.startswith("'") or text.startswith('"')

    @staticmethod
    def clean_up(query):
        '''
        Remove starting and ending quotes.
        '''
        return query.strip('"').strip("'")
    
    @staticmethod
    def get_query_from_variable_name(queryVariableName, script):
        queryRegexSingleQuotes = "{queryVariableName}\s*=\s*'(.*?)'".format(queryVariableName=queryVariableName)
        queryRegexDoubleQuotes = '{queryVariableName}\s*=\s*"(.*?)"'.format(queryVariableName=queryVariableName)
        queryRegexTipleSingleQuotes = "{queryVariableName}\s*=\s*'''(.*?)'''".format(queryVariableName=queryVariableName)
        queryRegexTripleDoubleQuotes = '{queryVariableName}\s*=\s*"""(.*?)"""'.format(queryVariableName=queryVariableName)
        queryRegex = '|'.join([queryRegexSingleQuotes, queryRegexDoubleQuotes, queryRegexTipleSingleQuotes, queryRegexTripleDoubleQuotes])
        queryMatchTokens = re.finditer(queryRegex, script, flags=re.DOTALL)
        return [q for q in queryMatchTokens][-1]

    @staticmethod
    def create_token_from_match(matchObj):
        return Token(
            value=matchObj.groups()[0],
            start=matchObj.start(1),
            end=matchObj.end(1)
        )
    
    def reset(self):
        self.pointer = 0