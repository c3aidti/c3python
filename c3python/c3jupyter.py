import json
import os

class C3JupyterNotebook(object):
    def __init__(self, c3=None, seed_path=None, name = None, path = None, id = None):
        """
        Initialize a C3JupyterNotebook object using a name, if or path.

        Args:
            c3: A C3 object.
            seed_path: The path to the seed directory.
            name: The name of the jupyter notebook.
            path: The path to the jupyter notebook + notebook name (path/to/notebook.ipynb).
            id: The id of the jupyter notebook record

        """
        self.c3 = c3
        self.seed_path = seed_path
        self.name = name
        self.path = path
        self.id = id
        self.jupyter_notebook_row = None
        self.jupyter_seed_json = None
        self.jupyter_directory_json = None

        if self.c3 is None:
            print("C3JupyterNotebook: No c3 object provided.")
            return None
        
        if self.name is None and self.path is None and self.id is None:
            print("Error: must specify name, path, or id")
            return None

        if self.id and self.name:
            print("Error: cannot specify both id and name")
            return None

        if self.name and self.path:
            print("Error: cannot specify both name and path")
            return None

        if self.path and self.id:
            print("Error: cannot specify both path and id")
            return None

        if self.name:
            filter = "name=='" + self.name + "'"
        if self.id:
            filter = "id=='" + self.id + "'"
        if self.path:
            filter = "path=='" + self.path + "'"
        
        self.jupyter_notebook_row = self.get_jupyter_notebook_row(filter)

        if self.id is None:
            self.id = self.jupyter_notebook_row.id
        if self.name is None:
            self.name = self.jupyter_notebook_row.name
        if self.path is None:
            self.path = self.jupyter_notebook_row.path

        self.jupyter_seed_json = self.get_jupyter_seed_json()
        self.jupyter_directory_json = self.get_jupyter_directory_json()


    def get_jupyter_notebook_row(self, filter):
        """
        Get the row for a jupyter notebook saved to a C3 tag.
        """
        # get count and bail if not exactly 1
        cnt = self.c3.JupyterNotebook.fetchCount({'filter': filter})
        if cnt != 1:
            print("Error: expected 1, got {}".format(cnt))
            return None
        
        return self.c3.JupyterNotebook.fetch({"filter": filter}).objs[0]

    def get_jupyter_seed_json(self):
        """
        Get the seed data for a jupyter notebook saved to a C3 tag.
        """
        nb = self.c3.JupyterNotebook.get(this={'id': self.id})
        return nb.jsonForSeedData()

    def get_jupyter_directory_json(self, writeable=True):
        """
        Return a dictionary of the jupyter directory json.
        """
        return {
            "id": self.path,
            "name": self.path,
            "path": self.path,
            "jupyterContentType": "directory",
            "format": "json",
            "writable": writeable,
        }    
    
    def write_jupyter_directory_json(self):
        if self.seed_path is None:
            print("Error: must specify seed_path to write seed files")
            return None
        if os.path.isdir(self.seed_path):
            if not os.path.isdir(self.seed_path + "/" + "JupyterDirectory"):
                os.mkdir(self.seed_path + "/" + "JupyterDirectory")
            with open(self.seed_path + "/" + "JupyterDirectory" + "/" + self.name + ".json", "w") as f:
                json.dump(self.jupyter_directory_json, f, indent=4)


    def write_jupyter_notebook_json(self):
        if self.seed_path is None:
            print("Error: must specify seed_path to write seed files")
            return None
        if os.path.isdir(self.seed_path):
            if not os.path.isdir(self.seed_path + "/" + "JupyterNotebook"):
                os.mkdir(self.seed_path + "/" + "JupyterNotebook")
            with open(self.seed_path + "/" + "JupyterNotebook" + "/" + self.name + ".json", "w") as f:
                json.dump(self.jupyter_seed_json, f, indent=4)