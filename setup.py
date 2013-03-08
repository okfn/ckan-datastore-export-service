from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(name='ckan-exporter-service',
      version=version,
      description="Service that allows exporting resources from the CKAN DataStore.",
      long_description="""\
""",
      classifiers=[],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Dominik Moritz',
      author_email='dominik.moritz@okfn.org',
      url='',
      license='AGPL',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=['''
            ckan-service-provider
            Requests >= 1.0.0'''
      ],
      entry_points={
            'console_scripts':
                  ['ckan-exporter-service = ckanexporterservice.main:main'],
            },
      )
