#%%
import re
import json
import pandas as pd

def extract_executable_type(obj, node=None, result=None):
    if result is None:
        result = []
    if isinstance(obj, dict):
        if 'Attributes' in obj and '{www.microsoft.com/SqlServer/Dts}ExecutableType' in obj['Attributes']:
            result.append({
                'RefId': obj['Attributes'].get('{www.microsoft.com/SqlServer/Dts}refId', ''),
                'ExecutableType': obj['Attributes']['{www.microsoft.com/SqlServer/Dts}ExecutableType'],
                'ObjectName': obj['Attributes'].get('{www.microsoft.com/SqlServer/Dts}ObjectName', '')  # Add this line
            })
        for key, value in obj.items():
            extract_executable_type(value, key, result)
    elif isinstance(obj, list):
        for item in obj:
            extract_executable_type(item, node, result)
    return result

def extract_executable_typeV2(obj, result=None):
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
                    'componentClassID': obj['Attributes'].get('componentClassID', ''),
                    'contactInfo': obj['Attributes'].get('contactInfo', ''),
                    'description': obj['Attributes'].get('description', ''),
                    'name': obj['Attributes'].get('name', '') 
                })
        for value in obj.values():
            extract_executable_typeV2(value, result)
    elif isinstance(obj, list):
        for item in obj:
            extract_executable_typeV2(item, result)
    return result


if __name__ == '__main__':
    with open('out.json') as f:
        data = json.load(f)

    executable_types = extract_executable_typeV2(data)
    #executable_types = list(set(executable_types))

    df = pd.DataFrame(executable_types)
    df.to_csv('executable_types.csv', index=False)
    print(executable_types)