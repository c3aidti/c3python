# c3python

This repository contains useful python functions to interact with c3
deployments.

## Installation
```
pip install git+https://github.com/c3aidti/c3python
```

## get_c3
Retrieve the remote python type system from a C3 tag.
The primary use is as follows:

```
from c3python import get_c3
c3 = get_c3('<vanity_url>', '<tenant>', '<tag>')
```

Authentication will use c3 key if present and otherwise prompt for username password.

Use the python `help` function for more info about the c3 object and particular types within it.

## Jupyter Seed Data

The `C3JupyterNotebook` class is provided to facilitate the generation of seed data for 
deploying Jupyter notebooks developed using the C3 Jupyter service.  

Example script for generating seed data in the current working directory (e.g. `dti-jupyter/seed`) from a new notebook named: `MyNb.ipynb` on the `tc01` tag of the `dti-jupyter` application:  
```
from c3python import get_c3
from c3python import C3JupyterNotebook
c3=get_c3(url="https://tc01-dti-jupyter.c3dti.ai",tenant="dti-jupyter", tag="tc01")
c3nb = C3JupyterNotebook(c3=c3,seed_path=".",name="MyNb.ipynb")
c3nb.write_jupyter_directory_json()
c3nb.write_jupyter_notebook_json()
```
The above script will generate 2 directories if not already present and 2 files:
```
$ ls -1 | grep Jupyter
JupyterDirectory
JupyterNotebook
```
Each directory will contain the appropriate json seed data that can then be provisioned.

## Command line utility: `c3py`
The `c3py` command line utility will be installed to your environment after running `pip install`.  To shorten the command options when working with the same tag, url, tag and tenant setting can be set in the following environment variables:
```
C3_URL
C3_TAG
C3_TENANT
```
Note: command line option override environment variables.  

Example of above Jupyter seed operation from command line with environment variables set for `C3_URL`, `C3_TAG` and `C3_TENANT`:
```
c3py seed-jupyter --name Untitled.ipynb --seed-dir .
```

Usage info for `c3py`:
```
$ c3py -h
usage: c3py [-h] [-e URL] [-t TAG] [-g TENANT] [-d] {seed-jupyter} ...

c3py: Command line utility for helper functions related to C3 development. A URL, TAG and TENANT must be provided either through command line or the
following environment variables: C3_URL, C3_TAG, C3_TENANT. Command line options for URL, TAG and TENANT take precedence over environment variables.
Authentication will use the ~/.c3/c3-rsa and corresponding user file key if they exists, otherwise username and password will be prompted for.

optional arguments:
  -h, --help            show this help message and exit
  -e URL, --url URL     C3 Vanity URL for desired tag.
  -t TAG, --tag TAG     C3 tag to be used.
  -g TENANT, --tenant TENANT
                        C3 tenant to be used.
  -d, --debug           Enable debug mode.

subcommands:
  seed-jupyter: Generate json seed data from Jupyter notebook that has been saved to a C3 tag. ONE OF THE FOLLOWING OPTIONS MUST BE PROVIDED: --name,
  --path or --id.
  
$ c3py seed-jupyter -h
usage: c3py seed-jupyter [-h] [-n NAME] [-p PATH] [-i ID] [-s SEED_DIR] [-w]

Generate json seed data from Jupyter notebook that has been saved to a C3 tag. ONE OF THE FOLLOWING OPTIONS MUST BE PROVIDED: --name, --path or --id.

optional arguments:
  -h, --help            show this help message and exit
  -n NAME, --name NAME  Name from JupyterNotebook type saved to C3 tag.
  -p PATH, --path PATH  Path from JupyterNotebook type saved to C3 tag.
  -i ID, --id ID        Id from JupyterNotebook type saved to C3 tag.
  -s SEED_DIR, --seed-dir SEED_DIR
                        Location of local seed directory in which to write Jupyter seed data.
  -w, --writeable       Store notebooks as writeable.
```
