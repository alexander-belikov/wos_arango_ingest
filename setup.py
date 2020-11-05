import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="wos_db_studies",
    version="0.1",
    author="Alexander Belikov",
    author_email="abelikov@gmail.com",
    description="tools to ingest csvs and jsons into arango DB",
    license="BSD",
    keywords="arangodb",
    url="git@github.com:alexander-belikov/wos_db_studies.git",
    packages=['wos_db_studies'],
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 0 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
    ],
    install_requires=['python-arango==5.2.1']
)
