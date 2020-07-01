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
        # print('pythonReformatted = ' + pythonReformatted)
        queryMatchTokens = self.get_query_strings(pythonReformatted) # get all strings passed to spark.sql() in the .py script
        
        reformattedScript = ''
        for token in queryMatchTokens:
            # print('self.pointer = ' + str(self.pointer))
            print('token.value = ' + repr(token.value))
            # print('token.start = {}, token.end = {}'.format(token.start, token.end))
            reformattedScript += pythonReformatted[self.pointer: token.start]
            reformattedQuery = api.format_query(token.value, self.hiveqlFormatter)
            totalIndent = Formatter.get_indent(token.start, pythonReformatted)
            # print('totalIndent = ' + repr(totalIndent))
            reformattedQuery = Formatter.indent_query(reformattedQuery, totalIndent)
            if not pythonReformatted[(token.start-3):token.start] in ["'''", '"""']: # handle queries quoted by '' or "" that are formatted to multiline
                if '\n' in reformattedQuery:
                    reformattedScript = reformattedScript[:-1] + "'''\n" # remove starting ' or "
                    reformattedScript += reformattedQuery
                    reformattedScript += '\n' + totalIndent + "'''"
                    self.pointer = token.end + 1 # skip ending ' or "
                else:
                    reformattedScript += reformattedQuery
                    self.pointer = token.end
            else:
                reformattedScript += '\n' + reformattedQuery + '\n' + totalIndent
                self.pointer = token.end
            # print('reformattedScript in loop: ' + reformattedScript)
        reformattedScript += pythonReformatted[self.pointer:]
        self.reset()
        return yapf_api.FormatCode(reformattedScript, style_config=self.pythonStyle)[0]
    
    def get_query_strings(self, pythonReformatted):
        sparkSqlMatchObjs = Formatter.get_spark_sql_args(pythonReformatted)
        queryMatchTokens = []
        for match in sparkSqlMatchObjs:
            matchGroupIndex = 1
            # print(match)
            # print('match.groups() = ')
            # print(match.groups())
            for m in match.groups():
                if m:
                    matchGroup = m # first match group that is not None
                    break
                else:
                    matchGroupIndex += 1
            # print('matchGroup = ' + matchGroup)
            # print(pythonReformatted[match.start(matchGroupIndex):match.end(matchGroupIndex)])
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
                # print(queryMatchObj)
                # print(queryMatchObj.groups())
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
        # print('queryVariableName = ' + queryVariableName)
        # print('script = ' + script)
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
                    start=matchObj.start(matchGroupIndex+1),
                    end=matchObj.end(matchGroupIndex+1),
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
    
    # @staticmethod
    # def get_blanks_before_query(start, script):
    #     print('start = ' + str(start))
    #     print('script = ' + script[:start])
    #     while script[start] == '\n':
    #         start += 1
    #     firstNonBlank = start
    #     while script[firstNonBlank].isspace():
    #         firstNonBlank += 1
    #     print('firstNonBlank = ' + str(firstNonBlank))
    #     return script[start:firstNonBlank]
    
    @staticmethod
    def indent_query(query, indent):
        lines = query.split('\n')
        lines = [indent + line for line in lines]
        indentedQuery = '\n'.join(lines)
        return indentedQuery
    
    def reset(self):
        self.pointer = 0