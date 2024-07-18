#%%

import pandas as pd
from SSISModule import SSISAnalyzer

disc = SSISAnalyzer(root_directory=r"C:\Users\luciano.argolo\ssis-scrapper\csv", valid_dirs=['csv'], file_extension=".csv")
df = disc.read_all_files()
df = df[['File_path']].drop_duplicates()
df.to_csv(r"C:\Users\luciano.argolo\ssis-scrapper\analysis\all_files.csv", index=False)

#Nakash_packages_ssis.csv
df['File_path'] = df['File_path'].str.lower()

df_nakash = pd.read_csv(r"C:\Users\luciano.argolo\ssis-scrapper\analysis\Nakash_packages_SSIS.csv")

df['is_contained'] = df['File_path'].apply(lambda x: any(df_nakash['path'].str.contains(x)))
df = df[df['is_contained']]

#%%
csv_main_files = df_nakash['path'].tolist() 
# %%
csv_main_files

# %%
#%%
#i need to open a .dtsx file
from SSISModule import SSISDiscovery
import json
from more_itertools import flatten
import pandas as pd
import os
import re 

def mapping_out(file_path, map_dict, visited=None):
    if visited is None:
        visited = set()

    # Avoid revisiting files
    if file_path in visited:
        return None
    visited.add(file_path)

    with open(file_path, 'r') as file:
        content = file.read()

    # Extract with this pattern <PackageName>(.*?)<\/PackageName> all the matches
    matches = re.findall('<PackageName>(.*?)<\/PackageName>', content)
    if len(matches) > 0:
        dtsxs_path = os.path.dirname(file_path)
        for match in matches:
            match_path = os.path.join(dtsxs_path, match)  # Assuming '.dtsx' needs to be appended
            # Check if the file exists before attempting to open it
                # Update map_dict with the match and its path
            if file_path in map_dict:
                map_dict[file_path].update({match_path : mapping_out(match_path, map_dict)})
                # Recursively call mapping_out with the new match_path
            else:
                map_dict[file_path] = {match_path : mapping_out(match_path, map_dict)}
                # Recursively call mapping_out with the new match_path
    else:
        return None

    return map_dict


def dependencies(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        

    # Extract with this pattern <PackageName>(.*?)<\/PackageName> all the matches
    list_of_match_and_time = []
    matches = re.findall('<PackageName>(.*?)<\/PackageName>', content)

    dir_name = os.path.dirname(file_path)
    matches = [os.path.join(dir_name, match) for match in matches]  # Assuming '.dtsx' needs to be appended
    
    if len(matches) > 0:
        return matches
    else:
        match = file_path
        return ""


dir_path = r"C:\Users\luciano.argolo\ssis-scrapper\SSIS"
target_dir = r"C:\Users\luciano.argolo\ssis-scrapper\dtsx"
valid_dirs = ['StagingToEDW', 'DWBaseIncrementalLoad']

discovery = SSISDiscovery(dir_path, valid_dirs=valid_dirs, file_extension=".dtsx")
files_path = discovery.get_files()

map_dict = {}

df = pd.read_csv(r"C:\Users\luciano.argolo\ssis-scrapper\analysis\files_estimated.csv")

for file_path in files_path:
    map_dict.update({file_path: dependencies(file_path)})

map_dict = {k: v for k, v in sorted(map_dict.items(), key=lambda item: len(item[1]) if item[1] is not None else 0, reverse=True)}


iterated_keys = []
new_dep_dict = {}
for key, value in map_dict.items():
    print(f"{key} : {value}")
    if key not in iterated_keys:
        if value:
            for v in value:
                print(f"    {v}")
                if key not in new_dep_dict:
                    new_dep_dict[key] = {v: map_dict[v]}
                else:
                    new_dep_dict[key].update({v: map_dict[v]})
                iterated_keys.append(v)
        else:
            new_dep_dict[key] = ""
    else:
        new_dep_dict[key] = ""

# i want to pop all the keys that are in the iterated_keys list
for key in list(set(iterated_keys)):
    new_dep_dict.pop(key)

with open(r"C:\Users\luciano.argolo\ssis-scrapper\analysis\tree_deps.json", "w") as f:
    f.write(json.dumps(new_dep_dict, indent=4))

pass