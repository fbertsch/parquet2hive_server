from setuptools import setup, find_packages

setup(name='parquet2hive_server',
      version='0.0.1',
      author='Frank Bertsch',
      author_email='fbertsch@mozilla.com',
      description='API Endpoint for Parquet2hive',
      url='https://github.com/fbertsch/parquet2hive_server',
      scripts=['start_parquet2hive_server', 'load_dataset'],
      packages=find_packages(),
      install_requires=['parquet2hive', 'Flask', 'flask_restful', 'boto3', 'requests'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest'])
