from setuptools import setup, find_packages

setup(
    name='mf-translate',
    version='0.1',
    py_modules=['mf_translate', 'mf_compare_query'],
    packages=find_packages(),
    install_requires=[
        'pandas>=1.5.0,<2.3.0',
        'tabulate>=0.9.0,<1.0.0',
        'looker-sdk>=23.0.0,<25.0.0',
        'lkml>=1.3.0,<1.4.0',
        'ruamel.yaml>=0.18.0,<0.19.0'
    ],
    entry_points={
        'console_scripts': [
            'mf-translate=mf_translate:main',
            'mf-compare-query=mf_compare_query:main'
        ],
    },
)