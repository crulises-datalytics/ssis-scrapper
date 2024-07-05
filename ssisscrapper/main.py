#%%
from SSISModule import SSISMigrator, SSISDiscovery
import json
import os

if __name__ == '__main__':
    dir_path = r"C:\Users\luciano.argolo\ssis-scrapper\SSIS"
    target_dir = r"C:\Users\luciano.argolo\ssis-scrapper\dtsx"

    discovery = SSISDiscovery(dir_path)
    files = discovery.get_files()
    discovery.extract_dtsx_files(target_dir, files)

    migrator = SSISMigrator()
    file_paths = os.listdir(dir_path)


    for file_name in file_paths:
        file_path = dir_path + "\\" + file_name
        parsed_data = migrator.parse_xml_file(file_path)
        df = migrator.get_df(parsed_data)
        
        with open((dir_path + "\\" + file_name).replace('dtsx', 'json'), "w") as f:
            f.write(json.dumps(parsed_data, indent=4))
        print("Parsed Data is written out to file")

        df.to_csv((dir_path + "\\" + file_name).replace('dtsx', 'csv'), index=False)
