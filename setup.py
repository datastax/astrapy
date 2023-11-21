# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup
from os import path

from astrapy import __version__

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, "README.MD"), encoding="utf-8") as f:
    long_description = f.read()

install_requires = [
    req_line.strip()
    for req_line in open("requirements.txt").readlines()
    if req_line.strip() != ""
    if req_line.strip()[0] != "#"
    if "-e ." not in req_line
]

setup(
    name="astrapy",
    packages=[
        "astrapy",
    ],
    package_data={"astrapy": ["py.typed"]},
    version=__version__,
    license="Apache license 2.0",
    description="AstraPy is a Pythonic SDK for DataStax Astra",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Kirsten Hunter",
    author_email="kirsten.hunter@datastax.com",
    url="https://github.com/datastax/astrapy",
    keywords=["DataStax", "Astra"],
    python_requires=">=3.8",
    install_requires=install_requires,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
