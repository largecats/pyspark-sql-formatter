class TokenType:
    QUERY_VARIABLE = 'QUERY_VARIABLE'
    QUERY_ARGUMENT = 'QUERY_ARGUMENT'


class Token:
    def __init__(self, type, value, start, end, script):
        '''
        Parameters:
        type: string
            TokenType.
        value: string
        start: int
            Position of the token's first character.
        end: int
            Position of the token's last character.
        script: string
            The script containing the token.
        '''
        __slots__ = 'value', 'start', 'end', 'type'  # saves space since there would be many instances of Token
        self.value = value
        self.start = start
        self.end = end
        self.type = type
        self.indent = self.get_token_indent(start, script)

    def get_token_indent(self, start, script):
        '''
        Determine the token's indentation when formatting the script.

        Parameters
        start: int
            Start of the token.
        script: string
            The script containing the token.
        
        Return: string
            The token's indentation.
        '''
        queryStart = Token.get_query_start(start, script)
        currLineIndent = Token.get_line_indent(queryStart, script)
        prevLineEnd = Token.get_prev_line_end(queryStart, script)
        prevLineIndent = Token.get_line_indent(prevLineEnd, script)
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