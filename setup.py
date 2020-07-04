import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='pysqlformatter-largecats', # Replace with your own username
    version='0.0.0',
    author='largecats',
    author_email='linfanxiaolinda@outlook.com',
    description='A formatter for Pyspark code with SQL queries.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/largecats/pyspark-sql-formatter',
    packages=setuptools.find_packages(),
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
    }
)