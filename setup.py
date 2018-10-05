import os

from setuptools import setup, find_packages

VERSION_FILE = 'VERSION'


def read_version():
    with open(VERSION_FILE, 'r') as fp:
        return tuple(map(int, fp.read().split('.')))


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="cgp-dss-data-loader",
    description="Simple data loader for CGP HCA Data Store",
    packages=find_packages(exclude=('datasets', 'tests', 'transformer')),  # include all packages
    url="https://github.com/DataBiosphere/cgp-dss-data-loader",
    entry_points={
        'console_scripts': [
            'dssload=scripts.cgp_data_loader:main'
        ]
    },
    long_description=read('README.md'),
    long_description_content_type="text/markdown",
    install_requires=['boto3 >= 1.7.0, < 1.8',
                      'cloud-blobstore >= 2.1.1, < 3',
                      'CommonMark == 0.7.5',
                      'crcmod >= 1.7, < 2',
                      'dcplib >= 1.3.2, < 2',
                      'google-cloud-storage >= 1.9.0, < 2',
                      'hca == 4.1.4',
                      'iso8601 == 0.1.12',
                      'requests >= 2.18.4, < 3'],
    license='Apache License 2.0',
    include_package_data=True,
    zip_safe=True,
    author="Jesse Brennan",
    author_email="brennan@ucsc.edu",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
    ],
    version='{}.{}.{}'.format(*read_version()),
    keywords=['genomics', 'metadata', 'loading', 'NIHDataCommons'],
)
