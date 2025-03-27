from setuptools import setup, find_packages

setup(
    name="flight_data_pipeline",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pandas',
        'paramiko',
        'loguru',
    ],
) 