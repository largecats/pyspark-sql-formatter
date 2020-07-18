import re

from pysqlformatter.src.token import TokenType, Token


class Tokenizer:
    def __init__(self, queryNames):
        '''
        Class for tokenizing the script to be formatted.

        Parameters
        queryNames: list
            Strings used to identify variables that contain the SparkSQL queries. 
            All string variables whose name contain these strings will be formatted.
        '''
        self.queryNames = queryNames

    def tokenize(self, script):
        '''
        Extract all queries from script.

        Parameters
        script: string
            The script to format.
        
        Return: list
            Query tokens found in the script.
        '''
        queryMatchTokens = self.get_queries(script=script)
        return queryMatchTokens

    def get_queries(self, script):
        queryVariableTokens = self.get_query_variable(script=script)
        queryArgumentTokens = self.get_query_argument(script=script)
        queryTokens = queryArgumentTokens + queryVariableTokens
        queryTokens.sort(key=lambda token: token.start)
        return queryTokens

    def get_query_variable(self, script):
        '''
        Find queries stored in variables with designated set of variable names in self.queryNames.

        Paramters
        script: string
            The script to format.
        
        Return: list
            Tokens of matched queries.
        '''
        queryVariableNames = '|'.join(self.queryNames)
        queryRegexSingleQuotes = '{queryVariableNames}\s*=\s*\'(.*?)\''.format(queryVariableNames=queryVariableNames)
        queryRegexDoubleQuotes = '{queryVariableNames}\s*=\s*"(.*?)"'.format(queryVariableNames=queryVariableNames)
        queryRegexTipleSingleQuotes = '{queryVariableNames}\s*=\s*\'\'\'(.*?)\'\'\''.format(
            queryVariableNames=queryVariableNames)
        queryRegexTripleDoubleQuotes = '{queryVariableNames}\s*=\s*"""(.*?)"""'.format(
            queryVariableNames=queryVariableNames)
        queryRegex = '|'.join(
            [queryRegexTipleSingleQuotes, queryRegexTripleDoubleQuotes, queryRegexSingleQuotes, queryRegexDoubleQuotes])
        queryMatchObjs = re.finditer(pattern=queryRegex, string=script, flags=re.IGNORECASE | re.DOTALL)
        queryMatchList = [
            Tokenizer.create_token_from_match(matchObj=m, script=script, type=TokenType.QUERY_VARIABLE)
            for m in queryMatchObjs
        ]
        return queryMatchList

    def get_query_argument(self, script):
        '''
        Find queries in spark.sql() in the script.

        Paramters
        script: string
            The script to format.
        
        Return: list
            Tokens of matched queries.
        '''
        sparkSqlRegex = 'spark\.sql\((.*?)\)'
        sparkSqlMatchObjs = re.finditer(sparkSqlRegex, script, flags=re.DOTALL)
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
                if matchGroup.startswith("'''") or matchGroup.startswith('"""'):  # enclosed by triple quotes
                    matchTokenWithoutQuotes = Token(
                        type=TokenType.QUERY_ARGUMENT,
                        value=matchGroup.strip('"').strip("'"),
                        start=matchObj.start(matchGroupIndex) + 3,  # skip opening triple quotes
                        end=matchObj.end(matchGroupIndex) - 3,  # skip ending triple quotes
                        script=script)
                else:  # enclosed by single quotes
                    matchTokenWithoutQuotes = Token(
                        type=TokenType.QUERY_ARGUMENT,
                        value=matchGroup.strip('"').strip("'"),
                        start=matchObj.start(matchGroupIndex) + 1,  # skip opening quotes
                        end=matchObj.end(matchGroupIndex) - 1,  # skip ending quotes
                        script=script)
                queryMatchTokens.append(matchTokenWithoutQuotes)
            else:  # e.g., spark.sql(query)
                continue  # ignore, because it is already handled by self.get_query_argument()
        return queryMatchTokens

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
    def create_token_from_match(matchObj, script, type):
        '''
        Create token from re.match object.

        Parameters
        matchObj: re.match object
        script: string
            The script to format.
        type: string
            TokenType.
        
        Return: Token() object
        '''
        matchGroupIndex = 1
        for m in matchObj.groups():
            if m:
                return Token(
                    type=type,
                    value=m,
                    start=matchObj.start(
                        matchGroupIndex
                    ),  # start(0) is the start of the whole match, see https://docs.python.org/3/library/re.html re.Match.start([group])
                    end=matchObj.end(matchGroupIndex),  # end(0) is the end of the whole match
                    script=script)
            else:
                matchGroupIndex += 1
