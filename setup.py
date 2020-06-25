from setuptools import setup, find_namespace_packages

setup(name='coalmine',
      version='0.1.0',
      description='A library for handling with Dataset ETL for pytorch',
      license="BSD3",
      author='Bernardo Lourenço',
      author_email='bernardo.lourenco@ua.pt',
      python_requires=">3.6.0",
      packages=find_namespace_packages(where='.'),
      classifiers=[
          "License :: OSI Approved :: Apache Software License",
          "Programming Language :: Python :: 3",
          "Programming Language :: Python :: 3.6",
          "Programming Language :: Python :: 3.7",
          "Topic :: Scientific/Engineering :: Artificial Intelligence",
      ],
      )
