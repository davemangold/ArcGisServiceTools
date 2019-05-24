import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cws-agstools",
    url="",
    version="1.0.0",
    author="David Mangold",
    author_email="mangoldd@cleanwaterservices.org",
    description="A collection of classes for working with ArcGIS services.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    install_requires=[
        'python-dateutil',
        'requests'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: Microsoft :: Windows"
    ],
)
