import os
from setuptools import setup, find_packages
import re

HERE = os.path.abspath(os.path.dirname(__file__))


with open(os.path.join(HERE, "../inference_tools/version.py"), encoding="utf-8") as f2:
    version_content = f2.read()
    version_template = "__version__ = '(.*)'\n"
    m = re.match(version_template, version_content)
    fallback_version = m.group(1)


setup(
    name="kg-inference-docs",
    author="Blue Brain Project, EPFL",
    use_scm_version={
        "relative_to": __file__,
        "write_to": "version.py",
        "write_to_template": "__version__ = '{version}'\n",
        "fallback_version": fallback_version
    },
    description="KG Inference Docs",
    keywords="ontology knowledge graph data science inference api",
    packages=find_packages(),
    python_requires=">=3.8",
    setup_requires=[
        "setuptools_scm",
    ],
    install_requires=[
        "sphinx==7.0.1",
        "sphinx-bluebrain-theme==0.4.1"
    ],
    classifiers=[
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering",
        "Programming Language :: Python :: 3 :: Only",
        "Natural Language :: English",
    ]
)
