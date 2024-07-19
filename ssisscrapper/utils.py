#%%
import os
import re

def create_directories(dirs:list, path:str) -> None: 
    for directory in dirs:
        dir_to_create = os.path.join(path, directory)
        if not os.path.exists(dir_to_create):  # Check if the directory exists
            os.makedirs(dir_to_create)  # Create the directory if it does not exist

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
    """
    Extracts the package names from an SSIS file and returns a list of file paths for the dependent packages.

    Args:
        file_path (str): The path to the SSIS file.

    Returns:
        list: A list of file paths for the dependent packages.

    """
    with open(file_path, 'r') as file:
        content = file.read()

    matches = re.findall('<PackageName>(.*?)<\/PackageName>', content)

    dir_name = os.path.dirname(file_path)
    matches = [os.path.join(dir_name, match) for match in matches]  # Assuming '.dtsx' needs to be appended

    if len(matches) > 0:
        return matches
    else:
        return ""
    

def collect_keys_values(json_obj, keys=set(), values=set()):
    """
    Recursively collects all keys and values from a JSON object.

    Args:
        json_obj (dict or list): The JSON object to collect keys and values from.
        keys (set, optional): A set to store the collected keys. Defaults to an empty set.
        values (set, optional): A set to store the collected values. Defaults to an empty set.

    Returns:
        set: A set containing all the collected keys and values.

    """
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            keys.add(key)
            if isinstance(value, dict):
                collect_keys_values(value, keys, values)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        collect_keys_values(item, keys, values)
                    else:
                        values.add(item)
            else:
                values.add(value)
    elif isinstance(json_obj, list):
        for item in json_obj:
            if isinstance(item, dict):
                collect_keys_values(item, keys, values)
            else:
                values.add(item)
    return keys | values


def process_map_dict(map_dict):
    """
    Process a dictionary of mappings and returns a new dictionary with dependencies.

    Args:
        map_dict (dict): A dictionary containing mappings.

    Returns:
        tuple: A tuple containing the new dependency dictionary and a list of iterated keys.

    Example:
        >>> map_dict = {'A': ['B', 'C'], 'B': ['D'], 'C': ['E']}
        >>> process_map_dict(map_dict)
        ({'A': {'B': ['D'], 'C': ['E']}, 'B': {'D': []}, 'C': {'E': []}}, ['B', 'C', 'D', 'E'])
    """
    # Function implementation goes here
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
    return new_dep_dict, iterated_keys

def clean_dep_dict(new_dep_dict, iterated_keys):
    """
    Removes specified keys from the dependency dictionary and returns a new dictionary.

    Args:
        new_dep_dict (dict): The dependency dictionary to clean.
        iterated_keys (iterable): The keys to remove from the dictionary.

    Returns:
        dict: A new dictionary with the specified keys removed.

    Example:
        >>> dep_dict = {'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [7, 8, 9]}
        >>> keys_to_remove = ['A', 'C']
        >>> clean_dep_dict(dep_dict, keys_to_remove)
        {'B': [4, 5, 6]}
    """
    for key in set(iterated_keys):
        new_dep_dict.pop(key, None)  # Use pop with None as default to avoid KeyError
    return {k: v for k, v in sorted(new_dep_dict.items(), key=lambda item: len(item[1]) if item[1] is not None else 0, reverse=True)}