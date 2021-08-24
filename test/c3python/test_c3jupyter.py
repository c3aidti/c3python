import pytest
import os
import json

from c3python import C3JupyterNotebook

# class MockNotebookRow(object):
#     @staticmethod
#     def get_jupyter_notebook_row(filter):

# class MockNotebook(C3JupyterNotebook):
#     def __init__(self, filename):
#         self.filename = filename
#         self.data = None
#         self.load_data()

#     def load_data(self):
#         with open(self.filename, 'r') as f:
#             self.data = json.load(f)

#     def save_data(self):
#         with open(self.filename, 'w') as f:
#             json.dump(self.data, f)

# class C3(object):
#     def __init__(self):
#         pass
#     class Fetch(object):
#         def __init__(self, filter):
#             pass
#         def objs(self):
#             return [{'id': '1', 'name': 'test'}]
#     class JupyterNotebook(object):
#         def __init__(self):
#             self.fetch = C3.Fetch()
#         def fetchCount(self):
#             return 1
#         def fetch(self):
#             return self.Fetch(self)

@pytest.fixture
def c3mock():
    return C3()

class TestC3Jupyter:
    def test_init(self,c3):
        notebook = C3JupyterNotebook(c3=c3,name="Untitled.ipynb")
        print(notebook.jupyter_notebook_row)
        #print(json.dumps(notebook,indent=4))
        

        