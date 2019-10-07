import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="datahelpers",
    version="0.1",
    author="Alexander Belikov",
    author_email="abelikov@gmail.com",
    description="tools to ingest csvs and jsons into arango DB",
    license="BSD",
    keywords="arangodb",
    url="git@github.com:alexander-belikov/wos_arango_ingest.git",
    packages=['wos_arango_ingest'],
    long_description=read('README.rst'),
    classifiers=[
        "Development Status :: 0 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
    ],
    install_requires=['python-arango']
)
