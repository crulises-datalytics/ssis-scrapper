#%%
import os
import re
import bs4 as bs
import lxml
import lxml.etree
import pandas as pd
from lxml import etree

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
    # matches = ["|".join([dir_name.split("\\")[-1], match]) for match in matches]

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
        # print(f"{key} : {value}")
        if key not in iterated_keys:
            if value:
                for v in value:
                    # print(f"    {v}")
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

def get_package_inner_execution_order(file_path):
    '''
    Returns the execution order of the activities within a certain SSIS package.
    
    Args:
        file_path (string): local path to the SSIS package.
        
    Returns:
        inner_dependencies (dict): dictionary containing the precedence order of the activities.
    '''
    with open(file_path, 'r') as file:
        content = file.read()
    
    soup = bs.BeautifulSoup(content, 'lxml')

    inner_dependencies = {}

    for i in soup.find_all('DTS:PrecedenceConstraints'.lower()):
            for x in i.findChildren():
                    # print(f"from: {x.attrs.get('dts:from')} to {x.attrs.get('dts:to')}")
                    inner_dependencies[x.attrs.get('dts:to')] = x.attrs.get('dts:from')

    return inner_dependencies

def extract_activities(container, inner_dependencies):
    '''
    Recursively extracts the activities' information from within a SSIS package container (STOCK:SEQUENCE) activity .
    
    Args:
        container (lxml.etree._Element): a STOCK:SEQUENCE SSIS activity that contains other activities within it, be it package execution tasks or more container activities.
        inner_dependencies (dict): the precedence order of the activities within the original container.
        
    Returns:
        pack_dict (dict): a dictionary containing the activity name, what it depends on to run, and the elements contained within the container. Elements may be the packages invoked, or inner containers.
    '''
    prefix='{www.microsoft.com/SqlServer/Dts}'
    
    activities = container.findall(f'{prefix}Executables')[0].getchildren()
    pack_dict = {
        'activity_name' : container.attrib[f'{prefix}refId'],
        'depends_on' : inner_dependencies.get(container.attrib[f'{prefix}refId'], None),
        'elements' : []}
    for act in activities:
        if act.attrib[f'{prefix}ExecutableType'] == 'Microsoft.ExecutePackageTask':
            pack_dict['elements'].append(act.attrib[f'{prefix}ObjectName'])
        elif act.attrib[f'{prefix}ExecutableType'] == 'STOCK:SEQUENCE':
            pack_dict['elements'].append(extract_activities(act, inner_dependencies))
    
    return pack_dict

def build_dependencies(file_path):
    '''
    Given the file path pointing to a parent SSIS package, it builds the dependencies and relationships of the packages contained within the parent one.
    
    Args:
        file_path (string): local path to the parent SSIS package containing other packages.
        
    Returns:
        pack_dependencies (dict): dictionary in which the key is the parent package name, and it's value is the composition of the activities' dependencies and relations.
    '''
    tree = lxml.etree.parse(file_path)
    prefix='{www.microsoft.com/SqlServer/Dts}'
    root = tree.getroot()
    
    pack_dependencies = {root.attrib[f'{prefix}ObjectName'] : []}
    inner_order = get_package_inner_execution_order(file_path)
    
    cajas = root.findall(f'{prefix}Executables')[0].getchildren()
    
    for caja in cajas:
        pack_dependencies[root.attrib[f'{prefix}ObjectName']].append(extract_activities(caja, inner_order))
    
    return pack_dependencies

def extract_sql_data(input_df, columns_to_keep:list=['File_path', 'Extracted', 'db']):
    regex = "FROM\\s+[ _@A-Za-z0-9.\\[\\]]+|join[ _A-Za-z0-9.\\[\\]]+|insert\\s+into\\s+[ _@A-Za-z0-9.\\[\\]]+|declare[ _@A-Za-z0-9.\\[\\]]+|update\\s+[ _@A-Za-z0-9.\[\]]+"
    # Filter rows based on the presence of SQL keywords and patterns
    filtered_df = input_df[input_df['SqlTaskData'].str.contains(regex, case=False, na=False)]
    
    # Extract all matches of SQL keywords and patterns
    matches_df = filtered_df['SqlTaskData'].str.extractall(f'({regex})', flags=re.IGNORECASE)
    matches_df = matches_df.astype(str)
    
    # Aggregate matches and split; then explode to separate rows
    filtered_df['Extracted'] = matches_df.groupby(level=0).agg('; '.join)
    filtered_df['Extracted'] = filtered_df['Extracted'].str.split('; ')
    exploded_df = filtered_df.explode('Extracted')
    
    # Extract database names, fill missing values with "No db found"
    exploded_df['db'] = exploded_df['Extracted'].str.extract('([a-zA-Z0-9_\\[\\]]+)\\.', flags=re.IGNORECASE).fillna("No db found")
    
    # Select specific columns and remove duplicates
    final_df = exploded_df[columns_to_keep].drop_duplicates()
    
    return final_df

def extract_values(all_files_path, pattern, split_values=False, add_prefix:bool=False) -> pd.DataFrame:
    """
    Extracts values from XML files based on a given pattern. Can optionally split values and save them to a CSV file.

    Parameters:
    - all_files_path: List of paths to the XML files.
    - pattern: XPath pattern to extract values.
    - split_values: Boolean indicating whether to split the extracted values by ';'.

    Returns:
    - A pandas DataFrame with the extracted values.
    """
    results = []
    max_split_values = 0

    for file_path in all_files_path:
        tree = etree.parse(file_path)
        values = tree.xpath(pattern)

        for value in values:
            file_path = '_'.join(file_path.split('\\')[-2:]) if add_prefix else file_path.split('\\')[-1]
            result_dict = {"File_path": file_path.replace('.dtsx', '')}
            if split_values and len(value) > 0:
                split_values_list = value.split(';')
                # Update max_split_values if the current list is longer
                max_split_values = max(max_split_values, len(split_values_list))
                for i, split_value in enumerate(split_values_list):
                    result_dict[f"value_{i+1}"] = split_value
            else:
                result_dict["SqlTaskData"] = value
            results.append(result_dict)

    df = pd.DataFrame(results)

    # If split_values was used, ensure all expected columns are present
    if split_values:
        for i in range(1, max_split_values + 1):
            if f"value_{i}" not in df.columns:
                df[f"value_{i}"] = None

    return df

def get_package_inner_execution_order(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
    
    soup = bs.BeautifulSoup(content, 'lxml')

    inner_dependencies = {}

    for i in soup.find_all('DTS:PrecedenceConstraints'.lower()):
            for x in i.findChildren():
                    # print(f"from: {x.attrs.get('dts:from')} to {x.attrs.get('dts:to')}")
                    inner_dependencies[x.attrs.get('dts:to')] = x.attrs.get('dts:from')

    return inner_dependencies

def extract_activities(container, inner_dependencies):
    prefix='{www.microsoft.com/SqlServer/Dts}'
    
    activities = container.findall(f'{prefix}Executables')[0].getchildren()
    pack_dict = {
        'activity_name' : container.attrib[f'{prefix}refId'],
        'depends_on' : inner_dependencies.get(container.attrib[f'{prefix}refId'], None),
        'elements' : []}
    for act in activities:
        if act.attrib[f'{prefix}ExecutableType'] == 'Microsoft.ExecutePackageTask':
            pack_dict['elements'].append(act.attrib[f'{prefix}ObjectName'])
        elif act.attrib[f'{prefix}ExecutableType'] == 'STOCK:SEQUENCE':
            pack_dict['elements'].append(extract_activities(act, inner_dependencies))
    
    return pack_dict

def build_dependencies(file_path):
    tree = lxml.etree.parse(file_path)
    prefix='{www.microsoft.com/SqlServer/Dts}'
    root = tree.getroot()
    
    pack_dependencies = {root.attrib[f'{prefix}ObjectName'] : []}
    inner_order = get_package_inner_execution_order(file_path)
    
    cajas = root.findall(f'{prefix}Executables')[0].getchildren()
    
    for caja in cajas:
        pack_dependencies[root.attrib[f'{prefix}ObjectName']].append(extract_activities(caja, inner_order))
    
    return pack_dependencies
def extract_sql_data(input_df, columns_to_keep:list=['File_path', 'Extracted', 'db']):
    regex = "FROM\\s+[ _@A-Za-z0-9.\\[\\]]+|join[ _A-Za-z0-9.\\[\\]]+|insert\\s+into\\s+[ _@A-Za-z0-9.\\[\\]]+|declare[ _@A-Za-z0-9.\\[\\]]+|update\\s+[ _@A-Za-z0-9.\[\]]+"
    # Filter rows based on the presence of SQL keywords and patterns
    filtered_df = input_df[input_df['SqlTaskData'].str.contains(regex, case=False, na=False)]
    
    # Extract all matches of SQL keywords and patterns
    matches_df = filtered_df['SqlTaskData'].str.extractall(f'({regex})', flags=re.IGNORECASE)
    matches_df = matches_df.astype(str)
    
    # Aggregate matches and split; then explode to separate rows
    filtered_df['Extracted'] = matches_df.groupby(level=0).agg('; '.join)
    filtered_df['Extracted'] = filtered_df['Extracted'].str.split('; ')
    exploded_df = filtered_df.explode('Extracted')
    
    # Extract database names, fill missing values with "No db found"
    exploded_df['db'] = exploded_df['Extracted'].str.extract('([a-zA-Z0-9_\\[\\]]+)\\.', flags=re.IGNORECASE).fillna("No db found")
    
    # Select specific columns and remove duplicates
    final_df = exploded_df[columns_to_keep].drop_duplicates()
    
    return final_df

def extract_values(all_files_path, pattern, split_values=False, add_prefix:bool=False) -> pd.DataFrame:
    """
    Extracts values from XML files based on a given pattern. Can optionally split values and save them to a CSV file.

    Parameters:
    - all_files_path: List of paths to the XML files.
    - pattern: XPath pattern to extract values.
    - split_values: Boolean indicating whether to split the extracted values by ';'.

    Returns:
    - A pandas DataFrame with the extracted values.
    """
    results = []
    max_split_values = 0

    for file_path in all_files_path:
        tree = etree.parse(file_path)
        values = tree.xpath(pattern)

        for value in values:
            file_path = '_'.join(file_path.split('\\')[-2:]) if add_prefix else file_path.split('\\')[-1]
            result_dict = {"File_path": file_path.replace('.dtsx', '')}
            if split_values and len(value) > 0:
                split_values_list = value.split(';')
                # Update max_split_values if the current list is longer
                max_split_values = max(max_split_values, len(split_values_list))
                for i, split_value in enumerate(split_values_list):
                    result_dict[f"value_{i+1}"] = split_value
            else:
                result_dict["SqlTaskData"] = value
            results.append(result_dict)

    df = pd.DataFrame(results)

    # If split_values was used, ensure all expected columns are present
    if split_values:
        for i in range(1, max_split_values + 1):
            if f"value_{i}" not in df.columns:
                df[f"value_{i}"] = None

    return df