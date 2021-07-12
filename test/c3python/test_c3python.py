import pytest
from c3python import getc3

def test_getc3():
    c3 = getc3()
    assert c3 is not None

