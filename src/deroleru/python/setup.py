# Guillaume Valadon <guillaume.valadon@ssi.gouv.fr>

from setuptools import setup
from setuptools_rust import RustExtension

setup(name="deroleru",
      version='0.1.0',
      rust_extensions=[RustExtension("deroleru", "./Cargo.toml")],
      zip_safe=False)
