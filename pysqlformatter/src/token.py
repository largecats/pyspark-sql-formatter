class Token:

    def __init__(self, value, start, end):
        '''
        Parameters:
        value: string
        start: int
            Position of the token's first character.
        end: int
            Position of the token's last character
        '''
        self.value = value
        self.start = start
        self.end = end