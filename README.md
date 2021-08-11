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
from c3python get_c3
c3 = get_c3('<vanity_url>', '<tenant>', '<tag>')
```

Authentication will use c3 key if present and otherwise prompt for username password.

Use the python `help` function for more info about the c3 object and particular types within it.

## Jupyter Seed Data

The `C3JupyterNotebook` class is provided to facilitate the generation of seed data for 
deploying jupyter notebooks developed using the C3 Jupyter service.  

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
The `c3py` command line utility will be installed to your environment after running `pip install`.  TYo shorten the command options when working wit hthe smae tag, url, tag and tenent setting can be set in the following environment variables:
```
C3_URL
C3_TAG
C3_TENANT
```
Note: command line option override environment variables.  

Example of above jupyter seed operation from command line with environment variables set for `C3_URL`, `C3_TAG` and `C3_TENANT`:
```
c3py seed-jupyter --name Untitled.ipynb --seed-dir .
```
