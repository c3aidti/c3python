import pytest
import os
from c3python import get_c3, C3Python

C3TAG = os.environ.get('C3_TAG')
C3URL = os.environ.get('C3_URL')
C3TENANT = os.environ.get('C3_TENANT')

@pytest.fixture(scope='session')
def c3env():
    return {
        'C3_TAG': C3TAG,
        'C3_URL': C3URL,
        'C3_TENANT': C3TENANT,
    }

@pytest.fixture(scope="session")
def c3py():
    return C3Python(C3URL, C3TENANT, C3TAG)

@pytest.fixture(scope="session")
def c3(c3py):
    return c3py.get_c3()