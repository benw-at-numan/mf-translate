from setuptools import setup, find_packages

setup(
    name='mf-translate',
    version='0.1',
    py_modules=['mf_compare_query'],
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'mf-compare-query=mf_compare_query:main'
        ],
    },
)