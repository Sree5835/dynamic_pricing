# This module converts my dynamic_pricing directory into a package

import setuptools

setuptools.setup(
    name="dynamic_pricing",
    version="0.0.1",
    packages=setuptools.find_namespace_packages(where="src"),
    package_dir={"": "src"},
)
