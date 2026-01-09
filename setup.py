#!/usr/bin/env python

from setuptools import setup

setup(name='tap-google-cloud-storage',
      version='1.0.0',
      description='Singer tap for extracting files from Google Cloud Storage',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      install_requires=[
          'backoff==2.2.1',
          'urllib3==2.5.0',
          'singer-encodings==0.1.2',
          'singer-python==6.1.1',
          'google-cloud-storage==3.7.0',
          'pyarrow==16.1.0',
          'fastavro==1.9.7'
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
