from setuptools import find_packages, setup

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="pyspark-exercise",
    version="0.0.1",
    description="The program joins two specified CSV files using PySpark and applies some minor transformations and filtering",
    package_dir={"": ""},
    packages=find_packages(),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/martijnwesselius/pyspark-exercise",
    author="martijnwesselius",
    author_email="",
    license="",
    classifiers=[
        "Programming Language :: Python :: 3.8.5",
        "Operating System :: OS Independent",
    ],
    install_requires=["pandas>=1.1.5", "pyspark>=3.4.1"],
    extras_require={},
    python_requires=">=3.8.5",
)