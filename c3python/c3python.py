import os
from time import time
import json

import base64
from Crypto.Hash import SHA512
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5
def _get_key(PEM_LOCATION):
    with open(PEM_LOCATION, 'rb') as secret_file:
        secret=secret_file.read()
        if (secret.startswith(b'-----BEGIN RSA PRIVATE KEY-----') or
            secret.startswith(b'-----BEGIN PRIVATE KEY-----')):
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

def _get_c3_key_token(keyfile=None, keystring=None, signature_text=None, username=None):
    """
    Return the key token for the c3 keyfile.
    """
    if not keyfile and not keystring:
        keyfile = os.getenv('HOME') + '/.c3/c3-rsa'

    if not signature_text:
      signature_text = str(int(time()*1000))
    
    key = _get_key(keyfile)
    print (signature_text)
    #data=json.dumps(signature_text)
    h=SHA512.new()
    h.update(signature_text.encode('utf-8'))
    sig = key.sign(h)
    plainsig = base64.b64encode(sig).decode('utf-8')
    print(f"pysig:{plainsig}")
    #tokenString = adminUser + ":" + Buffer.from(signatureText).toString('base64') + ":" + signature;
    tokenstr = f"{username}:{base64.b64encode(signature_text.encode('utf-8')).decode('utf-8')}:{plainsig}"
    print(f"pytokenstr:{tokenstr}")
    authtoken = f"c3key {base64.b64encode(tokenstr.encode('utf-8')).decode('utf-8')}"
    print(f"pyauthtoken:{authtoken}")
    token = sig
    return token

def getc3():
    """
    Return the c3 object.
    """
    pass