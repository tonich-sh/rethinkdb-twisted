# Copyright 2014 Anton Schur

from setuptools import setup

setup(
    name="rethinkdb-twisted",
    zip_safe=True,
    version="0.0.1",
    description="This package provides the Twisted connector for the Python\
driver library for the RethinkDB database server.",
    url="https://github.com/",
    maintainer="Anton Schur",
    maintainer_email="tonich.sh@gmail.com",
    license='Apache-2.0',
    packages=['rethinkdb.twisted'],
    install_requires=['rethinkdb>=1.13.0', 'Twisted>=14.0.0'],
)
