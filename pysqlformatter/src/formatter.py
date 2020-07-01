from __future__ import print_function # for print() in Python 2
import re
from yapf.yapflib import yapf_api
from hiveqlformatter import Config, api
from pysqlformatter.src.token import Token

class Formatter:
    '''
    Format a script with Python code and Spark SQL, HiveQL queries.
    '''

    def __init__(self, pythonStyle='pep8', hiveqlFormatterConfig=Config()):
        self.pythonStyle = pythonStyle
        self.hiveqlConfig = hiveqlFormatterConfig
        self.pointer = 0 # next position to read

    def format(self, script):
        pythonReformatted = yapf_api.FormatCode(script, style_config=self.pythonStyle)[0]
        queryMatchTokens = self.get_query_strings(pythonReformatted) # get all strings passed to spark.sql() in the .py script
        
        reformattedScript = ''
        for token in queryMatchTokens:
            # print('self.pointer = ' + str(self.pointer))
            # print('token.value = ' + repr(token.value))
            # print('token.start = {}, token.end = {}'.format(token.start, token.end))
            reformattedScript += pythonReformatted[self.pointer: token.start]
            reformattedQuery = api.format_query(token.value, self.hiveqlConfig)
            indentCurrLine = Formatter.get_indent(token.start, pythonReformatted)
            indentPrevLine = Formatter.get_indent(Formatter.get_prev_line_end(token.start, pythonReformatted), pythonReformatted)
            indent = indentCurrLine if len(indentCurrLine) > len(indentPrevLine) else indentPrevLine
            # print('indent = ' + repr(indent))
            reformattedQuery = Formatter.indent_query(reformattedQuery, indent)
            if not pythonReformatted[(token.start-3):token.start] in ["'''", '"""']: # handle queries quoted by '' or "" that are formatted to multiline
                if '\n' in reformattedQuery:
                    reformattedScript = reformattedScript[:-1] + "'''\n" # remove starting ' or "
                    reformattedScript += reformattedQuery
                    reformattedScript += '\n' + indent + "'''"
                    self.pointer = token.end + 1 # skip ending ' or "
                else:
                    reformattedScript += reformattedQuery
                    self.pointer = token.end
            else:
                reformattedScript += '\n' + reformattedQuery + '\n' + indent # properly format bewteen triple quotes
                self.pointer = token.end
        reformattedScript += pythonReformatted[self.pointer:]
        self.reset()
        return reformattedScript
    
    def get_query_strings(self, pythonReformatted):
        sparkSqlMatchObjs = Formatter.get_spark_sql_args(pythonReformatted)
        queryMatchTokens = []
        for match in sparkSqlMatchObjs:
            matchGroupIndex = 1
            for m in match.groups():
                if m:
                    matchGroup = m # first match group that is not None
                    break
                else:
                    matchGroupIndex += 1
            if Formatter.is_query(matchGroup): # e.g., spark.sql('select * from t0')
                if matchGroup.startswith("'''") or matchGroup.startswith('"""'):
                    matchTokenWithoutQuotes = Token(
                                            value=matchGroup.strip('"').strip("'"),
                                            start=match.start(matchGroupIndex)+3,
                                            end=match.end(matchGroupIndex)-3,
                                            indent=Formatter.get_indent(match.start(matchGroupIndex)+3, pythonReformatted)
                                            )
                else:
                    matchTokenWithoutQuotes = Token(
                                            value=matchGroup.strip('"').strip("'"),
                                            start=match.start(matchGroupIndex)+1,
                                            end=match.end(matchGroupIndex)-1,
                                            indent=Formatter.get_indent(match.start(matchGroupIndex)+1, pythonReformatted)
                                            )
                queryMatchTokens.append(matchTokenWithoutQuotes)
            else: # e.g., spark.sql(query)
                queryMatchObj = Formatter.get_query_from_variable_name(matchGroup, pythonReformatted[:match.start(1)])
                queryMatchToken = Formatter.create_token_from_match(queryMatchObj, pythonReformatted)
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
    def remove_quotes(query):
        '''
        Remove starting and ending quotes.
        '''
        return query.strip('"').strip("'")
    
    @staticmethod
    def get_query_from_variable_name(queryVariableName, script):
        queryRegexSingleQuotes = '{queryVariableName}\s*=\s*\'(.*?)\''.format(queryVariableName=queryVariableName)
        queryRegexDoubleQuotes = '{queryVariableName}\s*=\s*"(.*?)"'.format(queryVariableName=queryVariableName)
        queryRegexTipleSingleQuotes = '{queryVariableName}\s*=\s*\'\'\'(.*?)\'\'\''.format(queryVariableName=queryVariableName)
        queryRegexTripleDoubleQuotes = '{queryVariableName}\s*=\s*"""(.*?)"""'.format(queryVariableName=queryVariableName)
        queryRegex = '|'.join([queryRegexTipleSingleQuotes, queryRegexTripleDoubleQuotes, queryRegexSingleQuotes, queryRegexDoubleQuotes])
        # print(queryRegex)
        queryMatchObjs = re.finditer(queryRegex, script, flags=re.DOTALL)
        queryMatchObjsList = [m for m in queryMatchObjs]
        return queryMatchObjsList[-1]

    @staticmethod
    def create_token_from_match(matchObj, script):
        matchGroupIndex = 0
        for m in matchObj.groups():
            if m:
                return Token(
                    value=matchObj.groups()[matchGroupIndex],
                    start=matchObj.start(matchGroupIndex+1), # start(0) is the start of the whole match, see https://docs.python.org/3/library/re.html re.Match.start([group])
                    end=matchObj.end(matchGroupIndex+1), # end(0) is the end of the whole match
                    indent=Formatter.get_indent(matchObj.start(matchGroupIndex), script)
                )
            else:
                matchGroupIndex += 1
    
    @staticmethod
    def get_indent(pos, script):
        '''
        Get indentation of the line of given position in script.
        '''
        startOfLine = pos
        while startOfLine > 0 and script[startOfLine] != '\n': # find start of line, i.e., position of the last \n before pos + 1
            startOfLine -= 1
        startOfLine += 1
        indent = 0
        while script[startOfLine+indent].isspace(): # find position of the first non-space character in the line
            indent += 1
        return script[startOfLine:startOfLine+indent]
    
    @staticmethod
    def get_prev_line_end(pos, script):
        '''
        Get position of the end of the previous line.
        '''
        endOfPrevLine = pos
        while endOfPrevLine > 0 and script[endOfPrevLine] != '\n':
            endOfPrevLine -= 1
        endOfPrevLine -= 1
        return endOfPrevLine
    
    @staticmethod
    def indent_query(query, indent):
        lines = query.split('\n')
        lines = [indent + line for line in lines]
        indentedQuery = '\n'.join(lines)
        return indentedQuery
    
    def reset(self):
        self.pointer = 0