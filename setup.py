from setuptools import setup

setup(
    name='mf-translate',
    version='0.1',
    py_modules=['mf_compare_query'],
    entry_points={
        'console_scripts': [
            'mf-compare-query=mf_compare_query:main'
        ],
    },
)