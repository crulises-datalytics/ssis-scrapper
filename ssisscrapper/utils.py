#%%
import xml.etree.ElementTree as ET

def parse_node(node):
    parsed_data = {}

    # Parse attributes
    if node.attrib:
        parsed_data['Attributes'] = {key: value for key, value in node.attrib.items()}

    # Parse sub-nodes
    for sub_node in node:
        sub_node_data = parse_node(sub_node)
        sub_node_tag = sub_node.tag

        if sub_node_tag in parsed_data:
            if isinstance(parsed_data[sub_node_tag], list):
                parsed_data[sub_node_tag].append(sub_node_data)
            else:
                parsed_data[sub_node_tag] = [parsed_data[sub_node_tag], sub_node_data]
        else:
            parsed_data[sub_node_tag] = sub_node_data

    return parsed_data

def parse_xml_file(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    parsed_data = {root.tag: parse_node(root)}
    return parsed_data

def get_parent_path(path, levels):
    return path[:-levels] if levels <= len(path) else []

def get_node_by_path(parsed_data, path):
    node = parsed_data
    for step in path:
        
        if isinstance(node, dict):
            node = node.get(step)
            if isinstance(node, list):
                node = node[0]
            
        elif isinstance(node, str):
            node:str = node
        
        else:
            node:str = 'None'
    return node

def get_nodes_by_key(parsed_data, key, levels_up):
    def recursive_search(data, key, path=None):
        if path is None:
            path = []
        result = []
        if isinstance(data, dict):
            for k, v in data.items():
                new_path = path + [k]
                if k == key:
                    parent_path = get_parent_path(new_path, levels_up)
                    parent_node = get_node_by_path(parsed_data, parent_path)
                    result.append((parent_node, v))
                result.extend(recursive_search(v, key, new_path))
        elif isinstance(data, list):
            for item in data:
                result.extend(recursive_search(item, key, path))
        return result

    return recursive_search(parsed_data, key)

if __name__ == "__main__":
    file_path = "LaborHoursAccntSubAccnt.dtsx"
    parsed_data = parse_xml_file(file_path)
    import json
    with open("out.json", "w") as f:
        f.write(json.dumps(parsed_data, indent=4))
    print("Parsed Data is written out to file")

    key = "{www.microsoft.com/sqlserver/dts/tasks/sqltask}SqlStatementSource"
    levels_up = 2
    nodes_with_key = get_nodes_by_key(parsed_data, key, levels_up)
    print(f"Nodes with key '{key}' {levels_up} levels up:")
    for parent, node in nodes_with_key:
        print(f"Parent: {parent}, Node: {node}")
