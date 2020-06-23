from setuptools import setup, find_packages
from os import path

__version__ = "0.0.1"

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

# get the dependencies and installs
with open(path.join(here, "requirements.txt"), encoding="utf-8") as f:
    all_reqs = f.read().split("\n")

install_requires = [x.strip() for x in all_reqs if not x.startswith("git+")]
dependency_links = [
    x.strip().replace("git+", "") for x in all_reqs if x.startswith("git+")
]

setup(
    name="Kafka Examples",
    version=__version__,
    description="Kafka producer and consumer examples",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/basvdl/api",
    download_url="https://github.com/basvdl/api/tarball/" + __version__,
    license="BSD",
    keywords="",
    packages=find_packages(exclude=["tests*"]),
    include_package_data=True,
    author="Bas van de Lustgraaf",
    install_requires=install_requires,
    extras_require={"dev": ["pytest", "flake8", "black"]},
    dependency_links=dependency_links,
    author_email="bas@lustgraaf.nl",
)
