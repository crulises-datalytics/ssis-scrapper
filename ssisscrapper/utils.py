#%%
import os
import re

import xml.etree.ElementTree as ET

def create_directories(dirs:list, path:str) -> None: 
    for directory in dirs:
        dir_to_create = os.path.join(path, directory)
        if not os.path.exists(dir_to_create):  # Check if the directory exists
            os.makedirs(dir_to_create)  # Create the directory if it does not exist
            
def dependencies(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
    matches = re.findall('<PackageName>(.*?)<\/PackageName>', content)
    # matches = [os.path.join(match_path, match) for match in matches]  # Assuming '.dtsx' needs to be appended
    
    if len(matches) > 0:
        return matches
    else:
        return None