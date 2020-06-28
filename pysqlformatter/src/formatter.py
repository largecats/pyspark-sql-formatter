from __future__ import print_function # for print() in Python 2
import re
from yapf.yapflib import yapf_api
from hiveqlformatter import HiveqlFormatter, Config, api

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
        queryMatchObjs = self.get_query_strings(pythonReformatted) # get all strings passed to spark.sql() in the .py script
        
        reformattedScript = ''
        for match in queryMatchObjs:
            reformattedScript += pythonReformatted[self.pointer: match.start(1)]
            reformattedQuery = api.format_query(match.groups()[0], self.hiveqlFormatter)
            if not pythonReformatted[(match.start(1)-2):match.start(1)] in ["'''", '"""']: # handle queries quoted by '' or "" that are formatted to multiline
                if '\n' in reformattedQuery:
                    reformattedScript = reformattedScript[:-1] + "'''\n" # remove starting ' or "
                    reformattedScript += reformattedQuery
                    reformattedScript += "\n'''"
                    self.pointer = match.end(1) + 1 # skip ending ' or "
                else:
                    reformattedScript += reformattedQuery
                    self.pointer = match.end(1) + 1
            else:
                reformattedScript += reformattedQuery
                self.pointer = match.end(1) + 1
        reformattedScript += pythonReformatted[self.pointer:]
        return reformattedScript
    
    def get_query_strings(self, pythonReformatted):
        sparkSqlMatchObjs = self.get_spark_sql_args(pythonReformatted)
        queryMatchObjs = []
        for match in sparkSqlMatchObjs:
            if Formatter.is_query(match.groups()[0]): # e.g., spark.sql('select * from t0')
                queryMatchObjs.append(match)
            else: # e.g., spark.sql(query1)
                queryMatchObj = Formatter.get_query_from_variable_name(match.groups()[0], pythonReformatted[:match.start(1)])
                queryMatchObjs.append(queryMatchObj)
        return queryMatchObjs

    def get_spark_sql_args(self, pythonReformatted):
        sparkSqlRegex = 'spark\.sql\((.*?)\)'
        return re.finditer(sparkSqlRegex, pythonReformatted, flags=re.DOTALL)
    
    @staticmethod
    def is_query(text):
        textUpper = text.upper()
        return textUpper.startswith('WITH') or textUpper.startswith('SELECT')
    
    @staticmethod
    def get_query_from_variable_name(queryVariableName, script):
        queryRegexSingleQuotes = "{queryVariableName}\s*=\s*'(.*?)'".format(queryVariableName=queryVariableName)
        queryRegexDoubleQuotes = '{queryVariableName}\s*=\s*"(.*?)"'.format(queryVariableName=queryVariableName)
        queryRegexTipleSingleQuotes = "{queryVariableName}\s*=\s*'''(.*?)'''".format(queryVariableName=queryVariableName)
        queryRegexTripleDoubleQuotes = '{queryVariableName}\s*=\s*"""(.*?)"""'.format(queryVariableName=queryVariableName)
        queryRegex = '|'.join([queryRegexSingleQuotes, queryRegexDoubleQuotes, queryRegexTipleSingleQuotes, queryRegexTripleDoubleQuotes])
        queryMatchObjs = re.finditer(queryRegex, script, flags=re.DOTALL)
        return [q for q in queryMatchObjs][-1]