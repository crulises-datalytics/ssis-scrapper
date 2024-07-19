#%%
import os
import json
import os
from utils import dependencies, process_map_dict, clean_dep_dict, collect_keys_values
from SSISModule import SSISDiscovery


path = os.getcwd()

dir_path = path+"\\"+"bing"
target_dir = path+"\\"+"dtsx"
valid_dirs = ['DataLakeHRISToBase']

discovery = SSISDiscovery(dir_path, valid_dirs=valid_dirs, file_extension=".dtsx")
files_path = discovery.get_files()

map_dict = {}

for file_path in files_path:
    map_dict.update({file_path: dependencies(file_path)})

new_dep_dict, iterated_keys = process_map_dict(map_dict)
new_dep_dict = clean_dep_dict(new_dep_dict, iterated_keys)

with open(path+"\\analysis\\"+"tree_deps.json", "w") as f:
    f.write(json.dumps(new_dep_dict, indent=4))