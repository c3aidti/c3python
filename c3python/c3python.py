import os
from time import time
import base64
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5
from Crypto.Hash import SHA512


def get_c3(
    url,
    tenant,
    tag,
    mode="thick",
    define_types=True,
    auth=None,
    keyfile=None,
    keystring=None,
):
    """
    Returns c3remote type system for python.

    todo:
      - add support for keystring
    
    """
    keyauth = False
    if url is None:
        raise ValueError("url cannot be None")
    if tenant is None:
        raise ValueError("tenant cannot be None")
    if tag is None:
        raise ValueError("tag cannot be None")
    try:
        from urllib.request import urlopen
    except ImportError:
        from urllib2 import urlopen

    from types import ModuleType

    c3iot = ModuleType("c3IoT")
    c3iot.__loader__ = c3iot
    src = urlopen(url + "/public/python/c3remote_bootstrap.py").read()

    # use the c3-rsa keyfile, if it exists
    if keyfile is None:
        keyfile = os.environ.get("HOME") + "/.c3/c3-rsa"
    if os.path.isfile(keyfile):
        user = _get_rsa_user(url)
        if user:
            auth = _get_c3_key_token(keyfile, username=user)

    # It might be good to have a try except here...
    exec(src, c3iot.__dict__)

    # If auth is not None, retry with auth None if it fails
    # Note that auth=None implies username password auth
    while True:
        try:
            c3 = c3iot.C3RemoteLoader.typeSys(
                url=url,
                tenant=tenant,
                tag=tag,
                mode=mode,
                auth=auth,
                define_types=define_types,
            )
            break
        except Exception as e:
            if auth:
                auth = None
            else:
                raise e

    return c3


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


def _get_sig(signer, sigtext):
    """
    Return the signature.
    """
    pass


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
    if os.path.isfile(filename):
        with open(filename, "r") as f:
            user = f.read().rstrip("\n")
    else:
        user = None
    return user


def _get_c3_key_token(keyfile=None, keystring=None, signature_text=None, username=None):
    """
    Return the key token for the c3 keyfile.
    No support for keystring yet.
    """
    if not keyfile and not keystring:
        keyfile = os.getenv("HOME") + "/.c3/c3-rsa"

    if not signature_text:
        signature_text = str(int(time() * 1000))

    key = _get_key(keyfile)
    h = SHA512.new()
    h.update(signature_text.encode("utf-8"))
    sig = key.sign(h)
    plainsig = base64.b64encode(sig).decode("utf-8")
    tokenstr = f"{username}:{base64.b64encode(signature_text.encode('utf-8')).decode('utf-8')}:{plainsig}"
    authtoken = f"c3key {base64.b64encode(tokenstr.encode('utf-8')).decode('utf-8')}"
    return authtoken


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
