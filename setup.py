from setuptools import setup, find_packages

setup(
    name="c3python",
    version="0.3",
    packages=find_packages(),
    # Metadata
    author="Matthew Krafczyk, Darren Adams",
    description="Provides helper functions for interacting with c3",
    install_requires=["requests", "pycryptodome","azure-mgmt-storage","azure-storage-blob"],
    extras_require={"DF": ["pandas"]},
    entry_points={"console_scripts": ["c3py=c3python.c3py:main"]},
)
