class Token:

    def __init__(self, value, start, end, indent):
        '''
        Parameters:
        value: string
        start: int
            Position of the token's first character.
        end: int
            Position of the token's last character
        indent: string
            Indentation for the token.
        '''
        self.value = value
        self.start = start
        self.end = end
        self.indent = indent