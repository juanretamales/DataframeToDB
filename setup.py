import setuptools
from setuptools import setup, find_packages

long_description=""
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name = 'dataframetodb',
    packages = ['dataframetodb'], 
    version = '1.0',
    description = 'A conector for save dataframe to db and others utils with sqlalchemy',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author = 'Juan Retamales',
    author_email = 'jua.retamales@gmail.com',
    url = 'https://github.com/juanretamales/DataframeToDB', 
    project_urls={
        "Bug Tracker": "https://github.com/juanretamales/DataframeToDB/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    download_url = 'https://github.com/juanretamales/DataframeToDB/tarball/1.0',
    keywords = ['sqlalchemy', 'dataframe', 'to_sql', 'to_db', 'pandas'],
    install_requires=['pandas', 'numpy', 'python-dateutil', 'SQLAlchemy']
)