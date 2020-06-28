import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import logging

from pysqlformatter.src.formatter import Formatter

logger = logging.getLogger(__name__)
log_formatter = '[%(asctime)s] %(levelname)s [%(filename)s:%(lineno)s:%(funcName)s] %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=log_formatter)

class Test:

    def __init__(self):
        pass
    
    def test_script_with_one_query(self):
        msg = 'Testing script with one query'
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
        

    def run(self, msg, testScript, key, formatter=Formatter()):
        logger.info(msg)
        logger.info('testScript =')
        logger.info(testScript)
        logger.info(repr(testScript))
        formattedScript = formatter.format(testScript)
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