class TokenType:
    QUERY_IN_VARIABLE = 'QUERY_IN_VARIABLE'
    QUERY = 'QUERY'

class Token:

    def __init__(self, type, value, start, end, indent):
        '''
        Parameters:
        type: string
            TokenType
        value: string
        start: int
            Position of the token's first character.
        end: int
            Position of the token's last character
        indent: string
            Indentation for the token.
        '''
        self.type = type
        self.value = value
        self.start = start
        self.end = end
        self.indent = indent