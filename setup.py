import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(name='pysqlformatter',
                 version='0.0.7',
                 author='largecats',
                 author_email='linfanxiaolinda@outlook.com',
                 description='A formatter for Python code and SparkSQL queries.',
                 long_description=long_description,
                 long_description_content_type='text/markdown',
                 url='https://github.com/largecats/pyspark-sql-formatter',
                 packages=setuptools.find_packages(),
                 install_requires=['yapf', 'sparksqlformatter>=0.1.12', 'configparser'],
                 classifiers=[
                     'Programming Language :: Python',
                     'Programming Language :: Python :: 2',
                     'Programming Language :: Python :: 2.7',
                     'Programming Language :: Python :: 3',
                     'Programming Language :: Python :: 3.6',
                     'License :: OSI Approved :: MIT License',
                     'Operating System :: OS Independent',
                 ],
                 entry_points={
                     'console_scripts': ['pysqlformatter=pysqlformatter:run_main'],
                 })
