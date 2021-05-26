import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name = 'dataframetodb',
    packages = ['dataframetodb'], 
    version = '0.1',
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
        "License :: GNU GENERAL PUBLIC LICENSE",
        "Operating System :: OS Independent",
    ],
    download_url = 'https://github.com/juanretamales/DataframeToDB/tarball/0.1',
    keywords = ['sqlalchemy', 'dataframe', 'to_sql', 'to_db', 'pandas'],
    install_requires=['pandas', 'numpy', 'python-dateutil', 'SQLAlchemy']
)