import argparse
import os

from c3python import get_c3

def c3py(args):
    print("c3py")


def seed_jupyter(args):
    print("seed-jupyter")

def main():
    parser = argparse.ArgumentParser(description='Process commands for c3py.')
    parser.add_argument('-e', '--url', help='C3 Vanity URL for desired tag.', default=os.environ.get("C3_URL") , required=True)
    parser.add_argument('-t', '--tag', help='C3 tag to be used.', default=os.environ.get("C3_TAG") , required=True)
    parser.add_argument('-g', '--tenant', help='C3 tenant to be used.', default=os.environ.get("C3_TENANT") , required=True)
    parser.set_defaults(func=c3py)

    sub_parsers = parser.add_subparsers(help='commands')
    seed_jupyter_parser = sub_parsers.add_parser('seed-jupyter', help='Seed Jupyter')
    seed_jupyter_parser.add_argument('-n', '--name', help='Name of Jupyter Notebook saved to C3 tag.', required=False)
    seed_jupyter_parser.add_argument('-p', '--path', help='Path to Jupyter Notebook saved to C3 tag.', required=False)
    seed_jupyter_parser.add_argument('-i', '--id', help='Id from JupyterNotebook type of Jupyter Notebook saved to C3 tag.', required=False)
    seed_jupyter_parser.set_defaults(func=seed_jupyter)

    # Parse the args
    args = parser.parse_args()
    # Import C3 type system
    print(f"\nc3py: Importing C3 typesys from: {args.url}\ntenant: {args.tenant}\ntag: {args.tag}")
    global c3
    c3 = get_c3(args.url, args.tag, args.tenant)
    info = c3.SystemInformation.about()
    print(f"Successfully imported types from {args.url} running: C3 version: {info.serverVersion}")

    # call function
    args.func(args)

    

