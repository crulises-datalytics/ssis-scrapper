#%%
from SSISModule import SSISMigrator, SSISDiscovery
import json
import os

if __name__ == '__main__':
    dir_path = r"C:\Users\luciano.argolo\ssis-scrapper\SSIS"
    target_dir = r"C:\Users\luciano.argolo\ssis-scrapper\dtsx"

    valid_dirs = ['StagingToEDW', 'DWBaseIncrementalLoad']
    discovery = SSISDiscovery(dir_path, valid_dirs=valid_dirs)
    files = discovery.get_files()
    discovery.extract_dtsx_files(target_dir, files)

    migrator = SSISMigrator()
    file_paths = os.listdir(target_dir)


    for file_name in file_paths:
        file_path = target_dir + "\\" + file_name
        parsed_data = migrator.parse_xml_file(file_path)
        df = migrator.get_df(parsed_data)
        
        with open(file_path.replace('dtsx', 'json'), "w") as f:
            f.write(json.dumps(parsed_data, indent=4))
        print("Parsed Data is written out to file")

        df.to_csv(file_path.replace('dtsx', 'csv'), index=False)
