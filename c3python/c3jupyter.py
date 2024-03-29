import json
import os
import re

class C3JupyterNotebook(object):
    def __init__(self, c3=None, seed_path=None, name = None, path = None, id = None, target_path=None, writeable=False):
        """
        Initialize a C3JupyterNotebook object using a name, if or path.

        Args:
            c3: A C3 object.
            seed_path: The path to the seed directory.
            name: The name of the jupyter notebook.
            path: The path to the jupyter notebook + notebook name (path/to/notebook.ipynb).
            target_path: The path to the jupyter notebook on the target tag to be seeded.
            id: The id of the jupyter notebook record

        """
        self.c3 = c3
        self.seed_path = seed_path
        self.name = name
        self.path = path
        self.target_path = target_path
        self.target_name = None
        self.id = id
        self.jupyter_notebook_row = None
        self.jupyter_notebook_json = None
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

        # If target path includes a filename,ipynb, then use it to rename the notebook
        if self.target_path:
            if os.path.basename(self.target_path) != self.name:
                if self.target_path.endswith(".ipynb"):
                    self.target_name = os.path.basename(self.target_path)

        # Search filters based on name, path, or id
        if self.name:
            filter = "name=='" + self.name + "'"
        if self.id:
            filter = "id=='" + self.id + "'"
        if self.path:
            filter = "path=='" + self.path + "'"
        
        # Get the jupyter notebook row
        self.jupyter_notebook_row = self.get_jupyter_notebook_row(filter)

        # Update unspecified values to those from the jupyter notebook row
        if self.id is None:
            self.id = self.jupyter_notebook_row.id
        if self.name is None:
            self.name = self.jupyter_notebook_row.name
        if self.path is None:
            self.path = self.jupyter_notebook_row.path

        # Get json for seed data
        self.jupyter_notebook_json = self.get_jupyter_notebook_json()
        self.jupyter_directory_json = self.get_jupyter_directory_json(writeable=writeable)


    def get_jupyter_notebook_row(self, filter):
        """
        Get the row for a jupyter notebook saved to a C3 tag.
        """
        # get count and bail if not exactly 1
        cnt = self.c3.JupyterNotebook.fetchCount({'filter': filter})
        if cnt != 1:
            print("Error: expected 1, got {}".format(cnt))
            raise Exception(f"Error unable to retrive Notebook with filter: {filter}")
        
        return self.c3.JupyterNotebook.fetch({"filter": filter}).objs[0]

    def get_jupyter_notebook_json(self):
        """
        Get the seed data for a jupyter notebook saved to a C3 tag.
        """
        nb = self.c3.JupyterNotebook.get(this={'id': self.id})
        seed = nb.jsonForSeedData()
        # Update the json to use the target_path/target_name if provided
        name = self.name
        if self.target_name:
            name = self.target_name
        if self.target_path:
            seed['path'] = self.target_path + "/" + name
        #print(f"TTTT: {os.path.split(seed['path'])[0]}")
        return seed

    def get_jupyter_directory_json(self, writeable=False):
        """
        Return a dictionary of the jupyter directory json.
        """
        # remove file name from path
        pathname = os.path.dirname(self.path)

        # Set id to a unique string
        # Set name to the last part of the path
        # Set path to the full path
        return {
            "id": re.sub(r"\/","_",pathname),
            "name": os.path.basename(pathname),
            "path": pathname,
            "jupyterContentType": "directory",
            "format": "json",
            "writable": writeable,
        }    
    
    def write_jupyter_directory_json(self, verbose=False):
        if self.seed_path is None:
            print("Error: must specify seed_path to write seed files")
            return None
        if os.path.isdir(self.seed_path):
            if not os.path.isdir(self.seed_path + "/" + "JupyterDirectory"):
                os.mkdir(self.seed_path + "/" + "JupyterDirectory")
            seedfile = self.seed_path + "/" + "JupyterDirectory" + "/" + os.path.splitext(self.name)[0] + ".json"
            if verbose:
                print(f"Seeding Jupyter Directory for {self.name} to {seedfile}")
            with open(seedfile, "w") as f:
                json.dump(self.jupyter_directory_json, f, indent=4)


    def write_jupyter_notebook_json(self, verbose=False):
        if self.seed_path is None:
            print("Error: must specify seed_path to write seed files")
            return None
        if os.path.isdir(self.seed_path):
            if not os.path.isdir(self.seed_path + "/" + "JupyterNotebook"):
                os.mkdir(self.seed_path + "/" + "JupyterNotebook")
            seedfile = self.seed_path + "/" + "JupyterNotebook" + "/" + os.path.splitext(self.name)[0] + ".json"
            if verbose:
                print(f"Seeding Jupyter Notebook for {self.name} to {seedfile}")
            with open(self.seed_path + "/" + "JupyterNotebook" + "/" + self.name + ".json", "w") as f:
                json.dump(self.jupyter_notebook_json, f, indent=4)