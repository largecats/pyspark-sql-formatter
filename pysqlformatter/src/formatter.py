from __future__ import print_function # for print() in Python 2
import re
from yapf.yapflib import yapf_api
from hiveqlformatter import Config, api
from pysqlformatter.src.token import Token, TokenType

class Formatter:
    '''
    Format a script with Python code and Spark SQL, HiveQL queries.
    '''

    def __init__(self, pythonStyle='pep8', hiveqlConfig=Config()):
        self.pythonStyle = pythonStyle
        self.hiveqlConfig = hiveqlConfig
        self.pointer = 0 # next position to read

    def format(self, script):
        pythonReformatted = yapf_api.FormatCode(script, style_config=self.pythonStyle)[0]
        queryMatchTokens = self.get_query_strings(pythonReformatted) # get all strings passed to spark.sql() in the .py script
        
        reformattedScript = ''
        for token in queryMatchTokens:
            reformattedScript += pythonReformatted[self.pointer: token.start]
            reformattedQuery = api.format_query(token.value, self.hiveqlConfig) # will get rid of starting/trailling blank spaces
            queryStart = Formatter.get_query_start(token.start, pythonReformatted)
            # print('pythonReformatted[:queryStart] = ' + pythonReformatted[:queryStart])
            currLineIndent = Formatter.get_indent(queryStart, pythonReformatted)
            prevLineEnd = Formatter.get_prev_line_end(queryStart, pythonReformatted)
            prevLineIndent = Formatter.get_indent(prevLineEnd, pythonReformatted)
            if token.type == TokenType.QUERY_IN_VARIABLE:
                indent = prevLineIndent # align with previous line
            else:
                if queryStart == token.start: # if there are no starting \n, \r, align with current line
                    indent = currLineIndent
                else:
                    indent = prevLineIndent
            # indent = currLineIndent if len(currLineIndent) > len(prevLineIndent) else prevLineIndent
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
        return yapf_api.FormatCode(reformattedScript, style_config=self.pythonStyle)[0]
    
    def get_query_strings(self, script):
        sparkSqlMatchObjs = Formatter.get_spark_sql_args(script)
        queryMatchTokens = []
        for matchObj in sparkSqlMatchObjs:
            matchGroupIndex = 1 # we want the start of the matched group, so need to skip m.start(0), the entire matched string
            for m in matchObj.groups():
                if m:
                    matchGroup = m # first match group that is not None
                    break
                else:
                    matchGroupIndex += 1
            if Formatter.is_query(matchGroup): # e.g., spark.sql('select * from t0')
                if matchGroup.startswith("'''") or matchGroup.startswith('"""'):
                    matchTokenWithoutQuotes = Token(
                                            type=TokenType.QUERY,
                                            value=matchGroup.strip('"').strip("'"),
                                            start=matchObj.start(matchGroupIndex)+3, # skip opening triple quotes
                                            end=matchObj.end(matchGroupIndex)-3, # skip ending triple quotes
                                            indent=Formatter.get_indent(Formatter.get_query_start(matchObj.start(matchGroupIndex)+3, script), script) # get indent of the start of the query, excluding """/'''
                                            )
                else:
                    matchTokenWithoutQuotes = Token(
                                            type=TokenType.QUERY,
                                            value=matchGroup.strip('"').strip("'"),
                                            start=matchObj.start(matchGroupIndex)+1,
                                            end=matchObj.end(matchGroupIndex)-1,
                                            indent=Formatter.get_indent(Formatter.get_query_start(matchObj.start(matchGroupIndex)+1, script), script)
                                            )
                queryMatchTokens.append(matchTokenWithoutQuotes)
            else: # e.g., spark.sql(query)
                queryMatchObj = Formatter.get_query_from_variable_name(matchGroup, script[:matchObj.start(matchGroupIndex)]) # may contaiin starting/trailing blank spaces
                queryMatchToken = Formatter.create_token_from_match(queryMatchObj, script, TokenType.QUERY_IN_VARIABLE)
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
        '''
        Result may contain \n, \r after opening quotes and before ending quotes as adding [\n\r]* in the regex somehow triggers yapf syntax error.
        Instead, the \n, \r will be handled by get_indent() and hiveqlformatter.
        '''
        queryRegexSingleQuotes = '{queryVariableName}\s*=\s*\'(.*?)\''.format(queryVariableName=queryVariableName)
        queryRegexDoubleQuotes = '{queryVariableName}\s*=\s*"(.*?)"'.format(queryVariableName=queryVariableName)
        queryRegexTipleSingleQuotes = '{queryVariableName}\s*=\s*\'\'\'(.*?)\'\'\''.format(queryVariableName=queryVariableName)
        queryRegexTripleDoubleQuotes = '{queryVariableName}\s*=\s*"""(.*?)"""'.format(queryVariableName=queryVariableName)
        queryRegex = '|'.join([queryRegexTipleSingleQuotes, queryRegexTripleDoubleQuotes, queryRegexSingleQuotes, queryRegexDoubleQuotes])
        queryMatchObjs = re.finditer(queryRegex, script, flags=re.DOTALL)
        queryMatchObjsList = [m for m in queryMatchObjs]
        return queryMatchObjsList[-1]

    @staticmethod
    def create_token_from_match(matchObj, script, tokenType=TokenType.QUERY_IN_VARIABLE):
        matchGroupIndex = 1
        for m in matchObj.groups():
            if m:
                return Token(
                    type=tokenType,
                    value=m,
                    start=matchObj.start(matchGroupIndex), # start(0) is the start of the whole match, see https://docs.python.org/3/library/re.html re.Match.start([group])
                    end=matchObj.end(matchGroupIndex), # end(0) is the end of the whole match
                    indent=Formatter.get_indent(Formatter.get_query_start(matchObj.start(matchGroupIndex), script), script)
                )
            else:
                matchGroupIndex += 1
    
    @staticmethod
    def get_indent(pos, script):
        '''
        Get indentation of the line of given position in script.
        '''
        startOfLine = pos
        # while script[startOfLine] in ['\n', '\r']: # skip \n, \r after the opening '''/"""; these will be actually removed by hiveqlformatter
        #     startOfLine += 1
        while startOfLine > 0 and (script[startOfLine] not in ['\n', '\r']): # find start of line
            startOfLine -= 1
        startOfLine += 1
        indent = 0
        while script[startOfLine+indent] not in ['\n', '\r'] and script[startOfLine+indent].isspace(): # find position of the first non-space character in the line
            indent += 1
        indentString = script[startOfLine:startOfLine+indent]
        return indentString
    
    @staticmethod
    def get_query_start(pos, script):
        '''
        Get position of the start of the actual query in a query match, excluding blank space characters.
        '''
        startOfQuery = pos
        while script[startOfQuery].isspace(): # skip \n, \r after the opening '''/"""
            startOfQuery += 1
        return startOfQuery
    
    @staticmethod
    def get_prev_line_end(pos, script):
        '''
        Get position of the end of the previous line.
        '''
        endOfPrevLine = pos
        while endOfPrevLine > 0 and script[endOfPrevLine] not in ['\n', '\r']:
            endOfPrevLine -= 1
        while script[endOfPrevLine] in ['\n', '\r']:
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