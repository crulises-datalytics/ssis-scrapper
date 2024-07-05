#%%
from SSISMigrator import SSISMigrator
import json

if __name__ == '__main__':
    migrator = SSISMigrator()

    file_path = "LaborHoursAccntSubAccnt.dtsx"
    parsed_data = migrator.parse_xml_file(file_path)
    #executable_types = migrator.extract_executable_type(data)
    df = migrator.get_df(parsed_data)
    
    with open(f"json/{file_path.split(".")[0]}.json", "w") as f:
        f.write(json.dumps(parsed_data, indent=4))
    print("Parsed Data is written out to file")

    df.to_csv('executable_types.csv', index=False)

