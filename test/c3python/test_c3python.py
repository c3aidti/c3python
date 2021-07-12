import pytest
import os
from c3python.c3python import _get_c3_key_token
from c3python import get_c3


def test__get_c3_key_token():
   tok =  _get_c3_key_token(signature_text='1626099164960',username='auser')
   assert tok is not None


# def test_getc3_nodetoken_signature():
#     import sys
#     from Naked.toolshed.shell import muterun_js

#     here=os.path.dirname(os.path.abspath(__file__))
#     print(here)
#     response = muterun_js(here+'/nodetoken/bin/index.js')
#     if response.exitcode == 0:
#       response_string = response.stdout.decode('utf-8')
#       spl = response_string.split("\n")
#       sig = spl[0].split("::")[1]
#       print(f"sig:{sig}")
#       tokstr=spl[1].split("::")[1]
#       print(f"tokstr:{tokstr}")
#       auth_token=spl[2].split("::")[1]
#       print(f"auth_token:{auth_token}")
#       #print(spl[0])
#     else:
#       sys.stderr.write(response.stderr.decode('utf-8'))
    
