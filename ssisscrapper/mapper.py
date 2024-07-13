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
import os
import re 

def mapping_out(file_path, map_dict):
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




dir_path = r"C:\Users\luciano.argolo\ssis-scrapper\SSIS"
target_dir = r"C:\Users\luciano.argolo\ssis-scrapper\dtsx"
valid_dirs = ['StagingToEDW', 'DWBaseIncrementalLoad']

discovery = SSISDiscovery(dir_path, valid_dirs=valid_dirs, file_extension=".dtsx")
files_path = discovery.get_files()

map_dict = {}

for file_path in files_path:
    print(f"File: {file_path}")
    print(f"File position: {files_path.index(file_path) + 1}/{len(files_path)}")
    #file_path = r'C:\Users\luciano.argolo\ssis-scrapper\SSIS\ADPtoStagingIncrementalLoad\ADPtoStagingIncrementalLoad\ADPToStagingIncrementalParentPackage.dtsx'  # Update this to your file's path
    if file_path != r'C:\Users\luciano.argolo\ssis-scrapper\SSIS\StagingToEDW\StagingToEDW\StagingToEDWParentPackage.dtsx':
        mapping_out(file_path, map_dict)



map_dict

pass
pass

