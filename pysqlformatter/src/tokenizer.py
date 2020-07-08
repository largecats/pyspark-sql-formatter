import re


class TokenType:
    QUERY_IN_VARIABLE = 'QUERY_IN_VARIABLE'  # e.g., query = 'select * from t0'
    QUERY = 'QUERY'  # e.g., spark.sql('select * from t0)


class Token:
    def __init__(self, type, value, start, end, script):
        '''
        Parameters:
        type: string
            TokenType
        value: string
        start: int
            Position of the token's first character.
        end: int
            Position of the token's last character.
        script: string
            The script containing the token.
        '''
        __slots__ = 'type', 'value', 'start', 'end'  # saves space since there would be many instances of Token
        self.type = type
        self.value = value
        self.start = start
        self.end = end
        self.indent = self.get_token_indent(start, type, script)

    def get_token_indent(self, start, type, script):
        '''
        Determine the token's indentation when formatting the script.

        Parameters
        start: int
            Start of the token.
        type: string
            Type of the token.
        script: string
            The script containing the token.
        
        Return: string
            The token's indentation.
        '''
        queryStart = Token.get_query_start(start, script)
        currLineIndent = Token.get_line_indent(queryStart, script)
        prevLineEnd = Token.get_prev_line_end(queryStart, script)
        prevLineIndent = Token.get_line_indent(prevLineEnd, script)
        if type == TokenType.QUERY_IN_VARIABLE:
            indent = prevLineIndent  # align with previous line
        else:
            if queryStart == start:  # if there are no starting \n, align with current line
                indent = currLineIndent
            else:  # align with previous line
                indent = prevLineIndent
        # indent = currLineIndent if len(currLineIndent) > len(prevLineIndent) else prevLineIndent
        return indent

    @staticmethod
    def get_line_indent(pos, script):
        '''
        Get indentation of the line of given position in script.

        Parameters
        pos: int
            Position to get indentation for.
        script: string
            Script to format.
        
        Return: string
            Indentation of the line containing given position.
        '''
        startOfLine = pos
        while startOfLine > 0 and (script[startOfLine] != '\n'):  # find start of line
            startOfLine -= 1
        startOfLine += 1
        indent = 0
        # find position of the first non-space character in the line
        while (script[startOfLine + indent] != '\n') and script[startOfLine + indent].isspace():
            indent += 1
        indentString = script[startOfLine:startOfLine + indent]
        return indentString

    @staticmethod
    def get_query_start(pos, script):
        '''
        Get position of the start of the first line of the actual query in a query match.

        Parameters
        pos: int
            Position to get indentation for.
        script: string
            Script to format.
        
        Return: int
            Position of the start of the first line of the query.
        '''
        startOfQuery = pos
        while script[startOfQuery] == '\n':  # skip \n after the opening '''/"""
            startOfQuery += 1
        return startOfQuery

    @staticmethod
    def get_prev_line_end(pos, script):
        '''
        Get position of the end of the previous non-empty line.

        Parameters
        pos: int
            Position to get indentation for.
        script: string
            Script to format.

        Return: int
            Position of the last character in the previous line.
        '''
        endOfPrevLine = pos
        while endOfPrevLine > 0 and script[endOfPrevLine] != '\n':
            endOfPrevLine -= 1
        while script[endOfPrevLine] == '\n':
            endOfPrevLine -= 1
        return endOfPrevLine


class Tokenizer:
    def __init__(self):
        pass

    def tokenize(self, script):
        '''
        Extract all queries from script.

        Parameters
        script: string
            The script to format.
        
        Return: list
            Queries found in the script.
        '''
        sparkSqlMatchObjs = Tokenizer.get_spark_sql_args(script)
        queryMatchTokens = []
        for matchObj in sparkSqlMatchObjs:
            matchGroupIndex = 1  # we want the start of the matched group, so need to skip m.start(0), the entire matched string
            for m in matchObj.groups():
                if m:
                    matchGroup = m  # first match group that is not None
                    break
                else:
                    matchGroupIndex += 1
            if Tokenizer.is_query(matchGroup):  # e.g., spark.sql('select * from t0')
                if matchGroup.startswith("'''") or matchGroup.startswith('"""'):
                    matchTokenWithoutQuotes = Token(
                        type=TokenType.QUERY,
                        value=matchGroup.strip('"').strip("'"),
                        start=matchObj.start(matchGroupIndex) + 3,  # skip opening triple quotes
                        end=matchObj.end(matchGroupIndex) - 3,  # skip ending triple quotes
                        script=script)
                else:
                    matchTokenWithoutQuotes = Token(
                        type=TokenType.QUERY,
                        value=matchGroup.strip('"').strip("'"),
                        start=matchObj.start(matchGroupIndex) + 1,  # skip opening quotes
                        end=matchObj.end(matchGroupIndex) - 1,  # skip ending quotes
                        script=script)
                queryMatchTokens.append(matchTokenWithoutQuotes)
            else:  # e.g., spark.sql(query)
                queryMatchObj = Tokenizer.get_query_from_variable_name(
                    matchGroup, script[:matchObj.start(matchGroupIndex)])  # may contaiin starting/trailing blank spaces
                queryMatchToken = Tokenizer.create_token_from_match(queryMatchObj, script, TokenType.QUERY_IN_VARIABLE)
                queryMatchTokens.append(queryMatchToken)
        return queryMatchTokens

    @staticmethod
    def get_spark_sql_args(script):
        '''
        Match spark.sql() blocks in the script.

        Parameters
        script: string
            The script to format.
        
        Return: re.finditer object
            Iterator for all matched content in spark.sql()
        '''
        sparkSqlRegex = 'spark\.sql\((.*?)\)'
        return re.finditer(sparkSqlRegex, script, flags=re.DOTALL)

    @staticmethod
    def is_query(text):
        '''
        Determine whether the argument in spark.sql() is a query.

        Parameters
        text: string
            The content in spark.sql()
        
        Return: bool
            True if the text is a query. E.g., spark.sql('select * from t0').
            False if the text is a variable that stores the query. E.g., spark.sql(query)
        '''
        return text.startswith("'") or text.startswith('"')

    @staticmethod
    def remove_quotes(query):
        '''
        Remove starting and ending quotes from query.

        Parameters
        query: string
            The query to format.
        
        Return: string
            Query without starting and ending quotation marks.
        '''
        return query.strip('"').strip("'")

    @staticmethod
    def get_query_from_variable_name(queryVariableName, script):
        '''
        Retrieve the actual query string from its variable name.
        Result may contain \n after opening quotes and before ending quotes as adding [\n]* in the regex somehow triggers yapf syntax error.
        Instead, the \n will be handled by get_token_indent() and hiveqlformatter.

        Parameters
        queryVariableName: string
            Name of the variable that contains the query string.
        script: string
            The script to format.
        
        Return: re.match object
            The matched query.
        '''
        queryRegexSingleQuotes = '{queryVariableName}\s*=\s*\'(.*?)\''.format(queryVariableName=queryVariableName)
        queryRegexDoubleQuotes = '{queryVariableName}\s*=\s*"(.*?)"'.format(queryVariableName=queryVariableName)
        queryRegexTipleSingleQuotes = '{queryVariableName}\s*=\s*\'\'\'(.*?)\'\'\''.format(
            queryVariableName=queryVariableName)
        queryRegexTripleDoubleQuotes = '{queryVariableName}\s*=\s*"""(.*?)"""'.format(
            queryVariableName=queryVariableName)
        queryRegex = '|'.join(
            [queryRegexTipleSingleQuotes, queryRegexTripleDoubleQuotes, queryRegexSingleQuotes, queryRegexDoubleQuotes])
        queryMatchObjs = re.finditer(queryRegex, script, flags=re.DOTALL)
        queryMatchObjsList = [m for m in queryMatchObjs]
        return queryMatchObjsList[-1]

    @staticmethod
    def create_token_from_match(matchObj, script, tokenType=TokenType.QUERY_IN_VARIABLE):
        '''
        Create token from re.match object.

        Parameters
        matchObj: re.match object
        script: string
            The script to format.
        tokenType: string
            Type of the token.
        
        Return: Token() object
        '''
        matchGroupIndex = 1
        for m in matchObj.groups():
            if m:
                return Token(
                    type=tokenType,
                    value=m,
                    start=matchObj.start(
                        matchGroupIndex
                    ),  # start(0) is the start of the whole match, see https://docs.python.org/3/library/re.html re.Match.start([group])
                    end=matchObj.end(matchGroupIndex),  # end(0) is the end of the whole match
                    script=script)
            else:
                matchGroupIndex += 1
