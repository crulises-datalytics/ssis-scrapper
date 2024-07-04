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
                'Node': node,
                'ExecutableType': obj['Attributes']['{www.microsoft.com/SqlServer/Dts}ExecutableType'],
                'ObjectName': obj['Attributes'].get('{www.microsoft.com/SqlServer/Dts}ObjectName', '')  # Add this line
            })
        for key, value in obj.items():
            extract_executable_type(value, key, result)
    elif isinstance(obj, list):
        for item in obj:
            extract_executable_type(item, node, result)
    return result

if __name__ == '__main__':
    with open('out.json') as f:
        data = json.load(f)

    executable_types = extract_executable_type(data)
    #executable_types = list(set(executable_types))

    df = pd.DataFrame(executable_types)
    df.to_csv('executable_types.csv', index=False)
    print(executable_types)