import argparse
import os
import json

from c3python import get_c3, C3Python
from c3python import C3JupyterNotebook

def c3py(args):
    c3 = c3py_obj.get_c3()
    info = c3.SystemInformation.about()
    print(f"Successfully imported types from {args.url} running: C3 version: {info.serverVersion}\n")

def seed_jupyter(args):
    c3 = c3py_obj.get_c3()
    info = c3.SystemInformation.about()
    print("seed-jupyter")
    c3nb = C3JupyterNotebook(c3=c3, seed_path=args.seed_dir, name=args.name, path=args.path, id=args.id, writeable=args.writeable)

    print(f"Seed Data for Notebook: {c3nb.name}")
    print(json.dumps(c3nb.jupyter_notebook_json, indent=4))
    print(f"Seed Data for Notebook Directory: {c3nb.name}")
    print(json.dumps(c3nb.jupyter_directory_json, indent=4))

    if args.seed_dir and os.path.exists(args.seed_dir):
        c3nb.write_jupyter_directory_json(verbose=True)
        c3nb.write_jupyter_notebook_json(verbose=True)

def get_c3cli(args,path='.'):
    print("get-c3cli")
    ldr = c3py_obj.get_loader()
    cli = ldr.download_c3_cli_gzip()
    with open(path+'/'+'cli.tar.gz','wb') as f:
        f.write(cli)


def main():
    parser = argparse.ArgumentParser(description=
    """
    c3py: Command line utility for helper functions related to C3 development.\n
    A URL, TAG and TENANT must be provided either through command line or the following environment variables:
    C3_URL, C3_TAG, C3_TENANT. Command line options for URL, TAG and TENANT take precedence over environment variables.
    Authentication will use the ~/.c3/c3-rsa and corresponding user file key if they exists, otherwise username and password
    will be prompted for.
    """)
    parser.add_argument('-e', '--url', help='C3 Vanity URL for desired tag.', default=os.environ.get("C3_URL"))
    parser.add_argument('-t', '--tag', help='C3 tag to be used.', default=os.environ.get("C3_TAG"))
    parser.add_argument('-g', '--tenant', help='C3 tenant to be used.', default=os.environ.get("C3_TENANT"))
    parser.add_argument('-d', '--debug', help='Enable debug mode.', action='store_true', required=False)
    parser.add_argument('-a', '--auth', help='C3 Auth token.', default=None, required=False)
    parser.set_defaults(func=c3py)

    sub_parsers = parser.add_subparsers(description='')
    seed_jupyter_parser = sub_parsers.add_parser('seed-jupyter', parents=[parser], add_help=False, description="""
     Generate json seed data from Jupyter notebook that has been saved to a C3 tag.
    ONE OF THE FOLLOWING OPTIONS MUST BE PROVIDED: --name, --path or --id."""
    )
    seed_jupyter_parser.add_argument('-n', '--name', help='Name from JupyterNotebook type saved to C3 tag.', required=False)
    seed_jupyter_parser.add_argument('-p', '--path', help='Path from JupyterNotebook type saved to C3 tag.', required=False)
    seed_jupyter_parser.add_argument('-i', '--id', help='Id from JupyterNotebook type saved to C3 tag.', required=False)
    seed_jupyter_parser.add_argument('-tp', '--target-path', help='Target path to save notebook to.', required=False)
    seed_jupyter_parser.add_argument('-s', '--seed-dir', help='Location of local seed directory in which to write Jupyter seed data.', required=False)
    seed_jupyter_parser.add_argument('-w', '--writeable', help='Store notebooks as writeable.', default=False,action='store_true', required=False)
    seed_jupyter_parser.set_defaults(func=seed_jupyter)

    get_c3cli_parser = sub_parsers.add_parser('get-c3cli', parents=[parser], add_help=False, description="""
    Download the C3 CLI tool.
    """)
    get_c3cli_parser.set_defaults(func=get_c3cli)

    # Parse the args
    args = parser.parse_args()
    if not args.url:
        parser.error("C3 URL is required.")
    if not args.tag:
        parser.error("C3 tag is required.")
    if not args.tenant:
        parser.error("C3 tenant is required.")
    # Import C3 type system
    print(f"\nImporting C3 typesys from:\nurl: {args.url}\ntenant: {args.tenant}\ntag: {args.tag}")
    global c3py_obj
    c3py_obj = C3Python(url=args.url, tag=args.tag, tenant=args.tenant,auth_token=args.auth, debug=args.debug)
    #c3 = get_c3(url=args.url, tag=args.tag, tenant=args.tenant)
    

    # call function
    args.func(args)

    

