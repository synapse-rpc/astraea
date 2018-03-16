#!/usr/bin/env python
# coding=utf-8

from setuptools import setup, find_packages

setup(
    name='rpc.synpase.astraea',
    version='1.4.3',
    description=(
        'A rpc framework base RabbitMQ'
    ),
    long_description=open('README.md').read(),
    author='xRain',
    author_email='xrain@simcu.com',
    maintainer='xrain0610',
    maintainer_email='xrain@simcu.com',
    license='BSD License',
    packages=find_packages(),
    platforms=["all"],
    url='https://github.com/synapse-rpc',
    classifiers=[
        'Operating System :: OS Independent',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: Implementation',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries'
    ],
    install_requires=[
        'kombu'
    ],

)
