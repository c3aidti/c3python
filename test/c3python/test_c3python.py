import pytest
import os
from urllib.error import URLError
from c3python.c3python import _get_c3_key_token
from c3python import get_c3


def test_get_c3_user_pass(monkeypatch):
    """
    Will currently fail is url ,tennat or tag are invalid or none 
    """
    c3 = get_c3(
        os.environ.get("C3_URL"), os.environ.get("C3_TENANT"), os.environ.get("C3_TAG")
    )
    assert c3 is not None


# def test_get_c3_user_badurl(monkeypatch):
#     """
#     Will currently fail is url ,tennat or tag are invalid or none
#     """
#     monkeypatch.setenv('C3_URL', 'http://craphost:8080')
#     monkeypatch.setenv('C3_TENANT', 'auser')
#     monkeypatch.setenv('C3_TAG', 'atag')
#     pytest.raises(URLError, get_c3(
#         os.environ.get('C3_URL'),os.environ.get('C3_TENANT'),os.environ.get('C3_TAG')
#     )
#     )


def test_get_c3_user_badargs(monkeypatch):
    """
    Will currently fail is url ,tennat or tag are invalid or none 
    """
    monkeypatch.delenv("C3_URL", raising=False)
    monkeypatch.setenv("C3_TENANT", "auser")
    monkeypatch.setenv("C3_TAG", "atag")
    pytest.raises(
        ValueError,
        get_c3,
        os.environ.get("C3_URL"),
        os.environ.get("C3_TENANT"),
        os.environ.get("C3_TAG"),
    )
    monkeypatch.setenv("C3_URL", "http://craphost:8080")
    monkeypatch.delenv("C3_TENANT", raising=False)
    monkeypatch.setenv("C3_TAG", "atag")
    pytest.raises(
        ValueError,
        get_c3,
        os.environ.get("C3_URL"),
        os.environ.get("C3_TENANT"),
        os.environ.get("C3_TAG"),
    )
    monkeypatch.setenv("C3_URL", "http://craphost:8080")
    monkeypatch.setenv("C3_TENANT", "auser")
    monkeypatch.delenv("C3_TAG", raising=False)
    pytest.raises(
        ValueError,
        get_c3,
        os.environ.get("C3_URL"),
        os.environ.get("C3_TENANT"),
        os.environ.get("C3_TAG"),
    )


def test__get_c3_key_token():
    tok = _get_c3_key_token(signature_text="1626099164960", username="auser")
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
