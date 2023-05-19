import os
import requests
import sys
from time import time
import base64
from c3python.applogger import getmylogger
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5
from Crypto.Hash import SHA512

log = getmylogger(__name__)

class C3Python(object):
    def __init__(self, url, tenant, tag, auth=None, keyfile=None, keystring=None,username=None, userfile=None,debug=False, auth_token=None):
        # Ensuring required parameters are not None
        if not all([url, tenant, tag]):
            raise ValueError("url, tenant, and tag cannot be None")
        
        # Check if both keystring and keyfile are specified
        if keystring and keyfile:
            raise ValueError("keyfile and keystring cannot both be specified")
        
        # Check if both username and userfile are specified
        if userfile and username:
            raise ValueError("userfile and username cannot both be specified")
        
        # Import necessary modules
        try:
            from urllib.request import urlopen
        except ImportError:
            from urllib2 import urlopen

        from types import ModuleType

        # Initialize instance variables
        self.url = url
        self.tenant = tenant
        self.tag = tag
        self.auth = auth
        self.auth_token = auth_token
        self.keyfile = keyfile
        self.keystring = keystring
        self.username = username
        self.userfile = userfile
        self.password = None
        self.c3 = None

        # Initialize C3IoT module
        self.c3iot = ModuleType("c3IoT")
        self.c3iot.__loader__ = self.c3iot
        src = urlopen(url + "/public/python/c3remote_bootstrap.py").read()
        exec(src, self.c3iot.__dict__)

        # Handle authentication
        if self.auth or self.auth_token or self.keyfile or self.keystring:
            if keyfile or keystring:
                self._set_auth()

            # If no auth_token exists after above checks, try to get a new token
            if not self.auth_token:
                self.auth_token = self._get_C3AuthToken()

            # Set auth as auth_token if it exists after all checks
            if self.auth_token:
                self.auth = self.auth_token
        else:
            self._set_auth()

        # Trade the auth token (which could be a c3key based one) for a new longer lasting one
        if self.auth:
            self.auth_token = self._get_C3AuthToken()

    def get_conn(self):
        return self.c3iot.C3RemoteLoader.connect(self.url, self.tenant, self.tag, self.auth)
    
    def get_loader(self, define_types=False, action_id=None,mode='thick'):
        loader = self.c3iot.C3RemoteLoader(url=self.url, tenant=self.tenant, tag=self.tag, auth=self.auth, mode=mode,
                                action_id=action_id, define_types=define_types)
        def _download_c3_cli_gzip(self):
            return self._request('/static/nodejs-apps/cli/cli.tar.gz', 'application/gzip')
        self.c3iot.C3RemoteLoader.download_c3_cli_gzip = _download_c3_cli_gzip

        return loader
    
    def _get_C3AuthToken(self):
        #print(self.auth)
        print("Getting C3AuthToken...")
        headers = {'content-type': 'application/json', 'accept': 'application/json', 'Authorization': self.auth}
        c3_type = "Authenticator"
        action = "generateC3AuthToken"
        request_url = self.url + "/" + "api/1" + "/" + self.tenant + "/" + self.tag + "/" + c3_type + "?" + "action=" + action
        result = requests.get(request_url, headers=headers)
        if result.status_code == 200:
            return result.json()
        else:
            print("Error: " + str(result.status_code))
            raise Exception("Failed to get C3AuthToken")

    def _set_auth(self):
        if not self.keystring and not self.keyfile:
            default_keyfile = os.path.join(os.getenv("HOME"), ".c3", "c3-rsa")
            if os.path.isfile(default_keyfile):
                self.keyfile = default_keyfile

        if self.keystring:
            if self.username:
                log.info("Getting token for keystring + user")
                self.auth = _get_c3_key_token(keystring=self.keystring, username=self.username)
            else:
                raise ValueError("username cannot be None with specified keystring.")
        elif self.keyfile:
            # If no username was provided, get user from tag-associated file
            if not self.username:
                self.username = _get_rsa_user(self.url)
            log.info(f"Getting token from keyfile: {self.keyfile} for user: {self.username}")
            self.auth = _get_c3_key_token(keyfile=self.keyfile, username=self.username)
        else:
            # If neither a keystring nor a keyfile was found or provided, leave auth as None
            self.auth = None

    def get_c3(self,mode="thick",define_types=True):
        # If auth is not None, retry with auth None if it fails
        # Note that auth=None implies username password auth
        if self.auth_token:
            # If we have an auth token, try it first.  this only happens if BOTH private key
            # and auth are specified to the constructor
            #log.info(f"Getting C3 client with auth token for {self.url}...")
            print(f"Getting C3 client with auth token for {self.url}...")

            # print(self.auth_token)
            c3 = self.c3iot.C3RemoteLoader.typeSys(
                url=self.url,
                tenant=self.tenant,
                tag=self.tag,
                mode=mode,
                auth=self.auth_token,
                define_types=define_types,
            )
        else:

            while True:
                try:
                    log.info(f"Getting C3 client for {self.url}...")
                    c3 = self.c3iot.C3RemoteLoader.typeSys(ls 
                        url=self.url,
                        tenant=self.tenant,
                        tag=self.tag,
                        mode=mode,
                        auth=self.auth,
                        define_types=define_types,
                    )
                    break
                except Exception as e:
                    #raise e
                    if self.auth:
                        self.auth = None
                    else:
                        raise e

        return c3


def get_c3(
    url,
    tenant,
    tag,
    mode="thick",
    define_types=True,
    auth=None,
    auth_token=None,
    keyfile=None,
    keystring=None,
    username=None,
):
    """
    Returns c3remote type system for python from a particular tenant+tag.

    todo:
      - add support for keystring
    
    """
    c3py = C3Python(url,tenant,tag,auth=auth,auth_token=auth_token,keyfile=keyfile,keystring=keystring,username=username)
    return c3py.get_c3(mode=mode,define_types=define_types)

def _get_key(PEM_LOCATION):
    with open(PEM_LOCATION, "rb") as secret_file:
        secret = secret_file.read()
        if secret.startswith(b"-----BEGIN RSA PRIVATE KEY-----") or secret.startswith(
            b"-----BEGIN PRIVATE KEY-----"
        ):
            # string with PEM encoded key data
            k = secret
            rsa_key = RSA.importKey(k)
            return PKCS1_v1_5.new(rsa_key)
        else:
            return None

def _get_rsa_user(vanity_url):
    """
    Return the username associated with the user's private key.
    """

    filename = (
        os.environ.get("HOME")
        + "/.c3/"
        + "c3-rsa."
        + vanity_url.split("://")[1]
        + ".user"
    )
    print (filename)
    if os.path.isfile(filename):
        with open(filename, "r") as f:
            user = f.read().rstrip("\n")
    else:
        raise ValueError("c3-rsa.{}.user not found".format(vanity_url.split("://")[1]))
    return user


def _get_c3_key_token(keyfile=None, keystring=None, signature_text=None, username=None):
    """
    Return the key token for the c3 keyfile.
    No support for keystring yet.
    """

    if not keyfile and not keystring:
        keyfile = os.getenv("HOME") + "/.c3/c3-rsa"
    if keyfile and keystring:
        raise ValueError("keyfile and keystring cannot both be specified")
    
    if keystring and not keyfile:    
        rsa_key = RSA.importKey(keystring.encode())
        key = PKCS1_v1_5.new(rsa_key)
    if keyfile and not keystring:
        key = _get_key(keyfile)

    if not signature_text:
        signature_text = str(int(time() * 1000))

    h = SHA512.new()
    h.update(signature_text.encode("utf-8"))
    sig = key.sign(h)
    plainsig = base64.b64encode(sig).decode("utf-8")
    tokenstr = f"{username}:{base64.b64encode(signature_text.encode('utf-8')).decode('utf-8')}:{plainsig}"
    authtoken = f"c3key {base64.b64encode(tokenstr.encode('utf-8')).decode('utf-8')}"
    return authtoken

def send_file(file, vanity_url, tenant, tag, api_endpoint, auth_token, verbose=True):
    """
    Send a file to the c3 server.
    """

    # Definition of Argument parser
    # parser = argparse.ArgumentParser('helper function to post data to a c3 tag')
    # parser.add_argument('--file', help='The file to send', type=str, required=True)
    # parser.add_argument('--vanity-url', help='The vanity url to use', type=str, required=True)
    # parser.add_argument('--tenant', help='The tenant to use', type=str, required=True)
    # parser.add_argument('--tag', help='The tag to use', type=str, required=True)
    # parser.add_argument('--api-endpoint', help='The API endpoint to upload data to', type=str, required=True)
    # parser.add_argument('--auth-token', help='The authorization token to use. Generated with Authenticator.generateC3AuthToken()', type=str, required=True)

    # Parse Argument
    #args = parser.parse_args()

    file_path = file

    # User input validation
    if not os.path.exists(file_path):
        raise RuntimeError(f"File to send {file_path} doesn't exist!")

    # Build full URL
    full_url = '/'.join([
        vanity_url,
        'import',
        '1',
        tenant,
        tag,
        api_endpoint])

    print(f"Sending data to {full_url}")

    headers = {}

    # Auth token
    headers['Content-Type'] = 'text/csv'
    headers['Authorization'] = auth_token

    # From https://stackoverflow.com/questions/13909900/progress-of-python-requests-post
    class upload_in_chunks(object):
        def __init__(self, filename, chunksize=1 << 13):
            self.filename = filename
            self.chunksize = chunksize
            self.totalsize = os.path.getsize(filename)
            self.readsofar = 0

        def __iter__(self):
            with open(self.filename, 'rb') as file:
                while True:
                    data = file.read(self.chunksize)
                    if not data:
                        sys.stderr.write("\n")
                        break
                    self.readsofar += len(data)
                    percent = self.readsofar * 1e2 / self.totalsize
                    if verbose:
                        sys.stderr.write("\r{percent:3.0f}%".format(percent=percent))
                    yield data

        def __len__(self):
            return self.totalsize

    # Also from https://stackoverflow.com/questions/13909900/progress-of-python-requests-post
    class IterableToFileAdapter(object):
        def __init__(self, iterable):
            self.iterator = iter(iterable)
            self.length = len(iterable)

        def read(self, size=-1): # TBD: add buffer for `len(data) > size` case
            return next(self.iterator, b'')

        def __len__(self):
            return self.length

    file_it = upload_in_chunks(file_path, 125)
    r = requests.put(full_url,
                    data=IterableToFileAdapter(file_it),
                    headers=headers)

    print("{} - {}".format(r.status_code, requests.status_codes._codes[r.status_code][0]))

def EvaluateResultToPandas(result=None, eval_spec=None):
    """
    Take an EvaluateResult type and build a usable Pandas DataFrame from it.
    If you pass the EvaluateSpec you used to eval_spec (either c3.EvaluateSpec
    or dict type), this function will name the columns correctly as well.

    Arguments:
      result: A c3.EvaluateResult type which contains the results you want to
        turn into a Pandas DataFrame. [Required]
      eval_spec: Either a dict or c3.EvaluateSpec containing the spec you used
        to get the results. If no eval_spec is passed, by default the columns
        will be named 'c0', 'c1', ...

    Returns:
      A Pandas DataFrame containing the EvaluateResult data.
    """

    # Check whether the input is None
    if result is None:
        raise ValueError("You must pass a non-None value.")

    # Check whether the input is the right type
    # For now, we do a string comparison because the 'c3' object
    # Is not in scope in this function.
    if str(type(result)) != "c3.EvaluateResult":
        raise ValueError("You must pass a c3.EvaluateResult type.")

    # Find number of columns in the result
    num_columns = len(result.tuples[0].cells)

    # Get column names
    if eval_spec is None:
        column_names = ["c{}".format(i) for i in range(num_columns)]
    else:
        if type(eval_spec) is dict:
            column_names = eval_spec["projection"].split(",")
        elif str(type(eval_spec)) == "c3.EvaluateSpec":
            # For now, we do a string comparison because the 'c3' object
            # Is not in scope in this function.
            column_names = eval_spec.projection.split(",")
        else:
            raise RuntimeError(
                "eval_spec should be either a dict or c3.EvaluateSpec, Type {} not supported".format(
                    type(eval_spec)
                )
            )

        if len(column_names) != num_columns:
            raise RuntimeError(
                "Eval Spec doesn't have correct number of columns! Has {} expected {}. Did you use the right Spec?".format(
                    len(column_names), num_columns
                )
            )

    # Initialize the results dict
    results = {}
    for col in column_names:
        results[col] = []

    # Fill the results dict
    for row in result.tuples:
        for i in range(num_columns):
            results[column_names[i]].append(row.cells[i].value())

    # Build and return the final Pandas DataFrame
    try:
        import pandas as pd

        return pd.DataFrame(results)
    except ImportError as e:
        # Trick from https://stackoverflow.com/questions/6062576/adding-information-to-an-exception/6062799
        raise type(e)(e.message + " EvaluateResultToPandas needs pandas to work!")
    except Exception as e:
        raise e
