import xml.etree.ElementTree as ET
import shutil
import pandas as pd
import os
import re
import json

import xml.etree.ElementTree as ET

class SSISMigrator:
    """
    Parses SSIS package XML files to extract and manipulate package data.
    
    Methods:
        parse_node: Recursively parses an XML node and its children into a nested dictionary.
        parse_xml_file: Parses an entire XML file into a nested dictionary structure.
        get_parent_path: Retrieves the path to a node's parent based on a specified number of levels up.
        get_node_by_path: Retrieves a node from the parsed XML data based on a specified path.
        get_nodes_by_key: Searches for and retrieves nodes by key, optionally looking a specified number of levels up.
        extract_executable_type: Extracts information about executable types from the parsed XML data.
        get_df: Converts the extracted executable type information into a pandas DataFrame.
    """
    def parse_node(self, node):
        """
        Recursively parses an XML node and its children into a nested dictionary.
        """
        parsed_data = {}

        # Parse attributes
        if node.attrib:
            parsed_data['Attributes'] = {key: value for key, value in node.attrib.items()}

        # Parse sub-nodes
        for sub_node in node:
            sub_node_data = self.parse_node(sub_node)
            sub_node_tag = sub_node.tag

            if sub_node_tag in parsed_data:
                if isinstance(parsed_data[sub_node_tag], list):
                    parsed_data[sub_node_tag].append(sub_node_data)
                else:
                    parsed_data[sub_node_tag] = [parsed_data[sub_node_tag], sub_node_data]
            else:
                parsed_data[sub_node_tag] = sub_node_data

        return parsed_data

    def parse_xml_file(self, file_path):
        """
        Parses an entire XML file into a nested dictionary structure.
        """
        tree = ET.parse(file_path)
        root = tree.getroot()
        parsed_data = {root.tag: self.parse_node(root)}
        return parsed_data

    def get_parent_path(self, path, levels):
        """
        Retrieves the path to a node's parent based on a specified number of levels up.
        """
        return path[:-levels] if levels <= len(path) else []

    def get_node_by_path(self, parsed_data, path):
        """
        Retrieves a node from the parsed XML data based on a specified path.
        """
        node = parsed_data
        for step in path:
            if isinstance(node, dict):
                node = node.get(step)
                if isinstance(node, list):
                    node = node[0]
            elif isinstance(node, str):
                node = node
            else:
                node = 'None'
        return node

    def get_nodes_by_key(self, parsed_data, key, levels_up):
        """
        Searches for and retrieves nodes by key, optionally looking a specified number of levels up.
        """
        def recursive_search(data, key, path=None):
            if path is None:
                path = []
            result = []
            if isinstance(data, dict):
                for k, v in data.items():
                    new_path = path + [k]
                    if k == key:
                        parent_path = self.get_parent_path(new_path, levels_up)
                        parent_node = self.get_node_by_path(parsed_data, parent_path)
                        result.append((parent_node, v))
                    result.extend(recursive_search(v, key, new_path))
            elif isinstance(data, list):
                for item in data:
                    result.extend(recursive_search(item, key, path))
            return result

        return recursive_search(parsed_data, key)

    def extract_executable_type(self, obj, result=None):
        """
        Extracts information about executable types from the parsed XML data.
        """
        if result is None:
            result = []
        if isinstance(obj, dict):
            if 'Attributes' in obj and '{www.microsoft.com/SqlServer/Dts}ExecutableType' in obj['Attributes'] and ".EventHandlers" not in obj['Attributes']['{www.microsoft.com/SqlServer/Dts}refId']:
                if obj['Attributes']['{www.microsoft.com/SqlServer/Dts}ExecutableType'].lower() == 'microsoft.pipeline' and '{www.microsoft.com/SqlServer/Dts}ObjectData' in obj:
                    pipeline_data = obj['{www.microsoft.com/SqlServer/Dts}ObjectData'].get('pipeline', {})
                    components = pipeline_data.get('components', {})
                    for component in components.get('component', []):
                        result.append({
                            'RefId': obj['Attributes'].get('{www.microsoft.com/SqlServer/Dts}refId', ''),
                            'ExecutableType': obj['Attributes']['{www.microsoft.com/SqlServer/Dts}ExecutableType'],
                            'ObjectName': obj['Attributes'].get('{www.microsoft.com/SqlServer/Dts}ObjectName', ''),
                            'componentClassID': component["Attributes"].get('componentClassID', ''),
                            'contactInfo': component["Attributes"].get('contactInfo', ''),
                            'description': component["Attributes"].get('description', ''),
                            'name': component["Attributes"].get('name', '') 
                        })
                else:
                    result.append({
                        'RefId': obj['Attributes'].get('{www.microsoft.com/SqlServer/Dts}refId', ''),
                        'ExecutableType': obj['Attributes']['{www.microsoft.com/SqlServer/Dts}ExecutableType'],
                        'ObjectName': obj['Attributes'].get('{www.microsoft.com/SqlServer/Dts}ObjectName', ''),
                        'componentClassID': '',
                        'contactInfo': '',
                        'description': '',
                        'name': '' 
                    })
            for value in obj.values():
                self.extract_executable_type(value, result)
        elif isinstance(obj, list):
            for item in obj:
                self.extract_executable_type(item, result)
        return result
    
    def get_df(self, data:dict) -> pd.DataFrame:
        """
        Converts the extracted executable type information into a pandas DataFrame.
        """
        executable_types = self.extract_executable_type(data)
        df = pd.DataFrame(executable_types)
        return df


class SSISDiscovery:
    """
    Discovers SSIS package files (.dtsx) within a specified directory.
    
    Methods:
        get_files: Retrieves a list of file paths for files with a specified extension.
        extract_dtsx_files: Copies discovered .dtsx files to a target directory, renaming them for uniqueness.
    """
    def __init__(self, root_directory):
        """
        Initializes the SSISDiscovery with a root directory to search within.
        """
        self.root_directory = root_directory

    def get_files(self, file_extension=".dtsx"):
        """
        Retrieves a list of file paths for files with a specified extension.
        """
        final_files = []
        for root, dirs, files in os.walk(self.root_directory):
            for file in files:
                if file.endswith(file_extension):
                    final_files.append(os.path.join(root, file))
        return final_files
    
    def extract_dtsx_files(self, target_dir, files):
        """
        Copies discovered .dtsx files to a target directory, renaming them for uniqueness.
        """
        for file_path in files:
            parent_dir_name = os.path.basename(os.path.dirname(file_path))
            new_file_name = f"{parent_dir_name}_{os.path.basename(file_path)}"
            target_path = os.path.join(target_dir, new_file_name)
            shutil.copy(file_path, target_path)
    
class SSISAnalyzer(SSISDiscovery):
    """
    Extends SSISDiscovery to analyze and extract information from SSIS package files.
    
    Methods:
        read_all_files: Reads and combines data from all discovered .dtsx files into a single DataFrame.
        get_and_save_unique_values: Extracts and saves unique values from a specified column in the combined DataFrame.
    """

    def __init__(self, root_directory):
        """
        Initializes the SSISAnalyzer with a root directory to search within.
        """
        self.root_directory = root_directory
    
    def read_all_files(self, file_extension=".dtsx") -> pd.DataFrame:
        """
        Reads and combines data from all discovered .dtsx files into a single DataFrame.
        """
        # Iterate through the list of CSV file paths
        csv_files = self.get_files(file_extension=file_extension)
        dataframes = []
        for file_path in csv_files:
            df = pd.read_csv(file_path)
            df['File_path'] = file_path.split("\\")[-1].strip('.csv')
            dataframes.append(df)

        return pd.concat(dataframes, ignore_index=True)
        
    def get_and_save_unique_values(self, df: pd.DataFrame, column_name: str) -> None:
        """
        Extracts and saves unique values from a specified column in the combined DataFrame.
        """
        unique_values = df[column_name].unique()
        unique_df = pd.DataFrame(unique_values, columns=[column_name])
        unique_df.to_csv(self.root_directory.replace("\\csv", "\\analysis") + '\\' + f'total_{column_name}.csv', index=False)
