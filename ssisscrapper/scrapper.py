#%%
import re
import json
import pandas as pd

def extract_executable_type(obj, result=None):
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
            extract_executable_type(value, result)
    elif isinstance(obj, list):
        for item in obj:
            extract_executable_type(item, result)
    return result


if __name__ == '__main__':
    with open('out.json') as f:
        data = json.load(f)

    executable_types = extract_executable_type(data)

    df = pd.DataFrame(executable_types)
    df.to_csv('executable_types.csv', index=False)
    print(executable_types)