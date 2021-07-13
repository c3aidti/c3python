from setuptools import setup, find_packages
setup(
    name="c3python",
    version="0.2",
    packages=find_packages(),

    # Metadata
    author="Matthew Krafczyk, Darren Adams",
    description="Provides helper functions for interacting with c3",
    install_requires=['requests', 'pycryptodome'],
    extras_require = {
        'DF':  ['pandas']
    }
)
