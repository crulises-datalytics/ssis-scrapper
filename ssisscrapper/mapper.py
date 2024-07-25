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
valid_dirs = ['StagingToEDW', 'DataLakeHRISToBase', 'DWMartIncrementalLoad', 'DataLakeBaseToMart']

discovery = SSISDiscovery(dir_path, valid_dirs=valid_dirs, file_extension=".dtsx")
files_path = discovery.get_files()

map_dict = {}

for file_path in files_path:
    map_dict.update({file_path: dependencies(file_path)})

new_dep_dict, iterated_keys = process_map_dict(map_dict)
new_dep_dict = clean_dep_dict(new_dep_dict, iterated_keys)

with open(path+"\\analysis\\"+"tree_deps.json", "w") as f:
    f.write(json.dumps(new_dep_dict, indent=4))


with open(path+"\\analysis\\"+"HR_Jams.json", "r") as f:
    HR_Jams = json.load(f)

# total_deps = []

# for k in tree_deps.keys():
#     if isinstance(tree_deps[k], dict):
#         total_deps.append(build_dependencies(k))

# with open(path+"\\analysis\\"+"total_dependencies.json", "w") as f:
#     f.write(json.dumps(total_deps, indent=4))

dtsx_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'dtsx'))

total_deps = {}

for k in HR_Jams.keys():
    file_path = os.path.join(dtsx_path, HR_Jams[k]['package_name'])
    
    total_deps.update({
        k : {
            'package_name': HR_Jams[k]['package_name'],
            'depends_on' : HR_Jams[k]['depends_on'],
            'package_content' : build_dependencies(file_path)
        }
    })

with open(path+"\\analysis\\"+"HR_total_dependencies.json", "w") as f:
    f.write(json.dumps(total_deps, indent=4))