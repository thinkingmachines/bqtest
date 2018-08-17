from setuptools import setup
from setuptools import find_packages

setup(name='bqtest',
      version='0.0.1',
      description='Unit Testing for BigQuery',
      url='https://github.com/thinkingmachines/',
      author='Thinking Machines',
      author_email='iman@thinkingmachin.es',
      license='Apache License 2.0',
      install_requires=[
          'docopt',
          'google-cloud-bigquery'
      ],
      dependency_links=[],
      packages=find_packages('bqtest'),
      zip_safe=False,
      entry_points={
          'console_scripts': [
              'bqtest=bqtest:main',
          ],
      },
      )
