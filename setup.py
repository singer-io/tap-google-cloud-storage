#!/usr/bin/env python

from setuptools import setup

setup(name='tap-google-cloud-storage',
      version='0.0.2',
      description='Singer tap for extracting files from Google Cloud Storage',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      install_requires=[
          'backoff==2.2.1',
          'urllib3==2.6.3',
          'singer-encodings==0.4.0',
          'singer-python==6.8.0',
          'google-cloud-storage==3.7.0',
          'gcsfs==2024.10.0',
          'voluptuous==0.15.2'
      ],
      extras_require={
          'dev': [
              'ipdb'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-google-cloud-storage=tap_google_cloud_storage:main
      ''',
      packages=['tap_google_cloud_storage'])
