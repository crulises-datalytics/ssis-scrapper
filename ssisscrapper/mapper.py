#%%
import os
import json
import os
from utils import dependencies, process_map_dict, clean_dep_dict, collect_keys_values, build_dependencies
from SSISModule import SSISDiscovery
#site to generate grapphs of dependencies from json 
#https://jsoncrack.com/editor

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


#%%
# i want to recursively go through a json that has more jsons inside it, and put together all values and keys

# Example usage
# with open(path+"\\analysis\\"+"map_dict.json", "r") as f:
#     data = json.load(f)

# values = collect_keys_values(data)
# print("Values:", len(values))

with open(path+"\\analysis\\"+"tree_deps.json", "r") as f:
    tree_deps = json.load(f)

total_deps = []

for k in tree_deps.keys():
    if isinstance(tree_deps[k], dict):
        total_deps.append(build_dependencies(k))

with open(path+"\\analysis\\"+"total_dependencies.json", "w") as f:
    f.write(json.dumps(total_deps, indent=4))