import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import logging

from pysqlformatter.src.api import api

logger = logging.getLogger(__name__)
log_formatter = '[%(asctime)s] %(levelname)s [%(filename)s:%(lineno)s:%(funcName)s] %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=log_formatter)

class Test:

    def __init__(self):
        pass
    
    def test_script_with_simple_query_as_variable(self):
        msg = 'Testing script with simple query as variable passed to spark.sql()'
        testScript = """
query = 'select * from t0'
df = spark.sql(query)
        """
        key = """
query = '''
SELECT
    *
FROM
    t0
'''
df = spark.sql(query)
        """.strip() + '\n' # pep8
        self.run(msg, testScript, key)
    
    def test_script_with_simple_query_as_argument(self):
        msg = 'Testing script with simple query in spark.sql()'
        testScript = """
df = spark.sql('select * from t0')
        """
        key = """
df = spark.sql('''
SELECT
    *
FROM
    t0
''')
        """.strip() + '\n' # pep8
        self.run(msg, testScript, key)
    
    def test_script_with_complex_query_as_variable1(self):
        msg = 'Testing script with complex query in spark.sql()'
        testScript = """
import re

def get_base(date):
    query = '''
    select * from
    t0
    left join t1 on t0.id = t1.id
    where t1.date = '{date}'
    '''
    query = query.format(date=date)
    df = spark.sql(query)
    df.cache()
    return df

def add_columns():
    df = spark.sql('select * from base left join t2 on base.id = t2.id')
    return df

def final():
    columns = ['id', 'date', 'c1', 'c2']
    df = df.select(columns)
    return df
        """
        key = """
import re


def get_base(date):
    query = '''
    SELECT
        *
    FROM
        t0
        LEFT JOIN t1 ON t0.id = t1.id
    WHERE
        t1.date = '{date}'
    '''
    query = query.format(date=date)
    df = spark.sql(query)
    df.cache()
    return df


def add_columns():
    df = spark.sql('''
    SELECT
        *
    FROM
        base
        LEFT JOIN t2 ON base.id = t2.id
    ''')
    return df


def final():
    columns = ['id', 'date', 'c1', 'c2']
    df = df.select(columns)
    return df
        """.strip() + '\n' # pep8
        self.run(msg, testScript, key)

    def test_script_with_complex_query_as_variable_currLineIndent_less_than_prevLineIndent(self):
        msg = 'Testing script with complex query in spark.sql(), with currLineIndent < prevLineIndent'
        testScript = """
import re

def get_base(date):
    query = '''
select * from
t0
left join t1 on t0.id = t1.id
where t1.date = '{date}'
    '''
    query = query.format(date=date)
    df = spark.sql(query)
    df.cache()
    return df

def add_columns():
    df = spark.sql('select * from base left join t2 on base.id = t2.id')
    return df

def final():
    columns = ['id', 'date', 'c1', 'c2']
    df = df.select(columns)
    return df
        """
        key = """
import re


def get_base(date):
    query = '''
    SELECT
        *
    FROM
        t0
        LEFT JOIN t1 ON t0.id = t1.id
    WHERE
        t1.date = '{date}'
    '''
    query = query.format(date=date)
    df = spark.sql(query)
    df.cache()
    return df


def add_columns():
    df = spark.sql('''
    SELECT
        *
    FROM
        base
        LEFT JOIN t2 ON base.id = t2.id
    ''')
    return df


def final():
    columns = ['id', 'date', 'c1', 'c2']
    df = df.select(columns)
    return df
        """.strip() + '\n' # pep8
        self.run(msg, testScript, key)

    def test_script_with_complex_query_as_variable_currLineIndent_greater_than_prevLineIndent(self):
        msg = 'Testing script with complex query in spark.sql(), with currLineIndent > prevLineIndent'
        testScript = """
import re

def get_base(date):
    query = '''
        select * from
        t0
        left join t1 on t0.id = t1.id
        where t1.date = '{date}'
    '''
    query = query.format(date=date)
    df = spark.sql(query)
    df.cache()
    return df

def add_columns():
    df = spark.sql('select * from base left join t2 on base.id = t2.id')
    return df

def final():
    columns = ['id', 'date', 'c1', 'c2']
    df = df.select(columns)
    return df
        """
        key = """
import re


def get_base(date):
    query = '''
    SELECT
        *
    FROM
        t0
        LEFT JOIN t1 ON t0.id = t1.id
    WHERE
        t1.date = '{date}'
    '''
    query = query.format(date=date)
    df = spark.sql(query)
    df.cache()
    return df


def add_columns():
    df = spark.sql('''
    SELECT
        *
    FROM
        base
        LEFT JOIN t2 ON base.id = t2.id
    ''')
    return df


def final():
    columns = ['id', 'date', 'c1', 'c2']
    df = df.select(columns)
    return df
        """.strip() + '\n' # pep8
        self.run(msg, testScript, key)
        
    def run(self, msg, testScript, key, formatter=Formatter()):
        logger.info(msg)
        logger.info('testScript =')
        logger.info(testScript)
        logger.info(repr(testScript))
        formattedScript = api.format_script(testScript)
        logger.info('formattedScript =')
        logger.info(formattedScript)
        logger.info(repr(formattedScript))
        logger.info('key =')
        logger.info(key)
        logger.info(repr(key))
        assert formattedScript == key
        return True

    def run_all(self):
        tests = list(filter(lambda m: m.startswith('test_'), dir(self)))
        for test in tests:
            getattr(self, test)()

if __name__ == "__main__":
    Test().run_all()