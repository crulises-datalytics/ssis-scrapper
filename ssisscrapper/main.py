#%%
from SSISModule import SSISMigrator, SSISDiscovery
import json
import os

if __name__ == '__main__':

    #--------------------------------------
    # EXTRACITING ALL SP FROM BING.RAR FILE
    from SSISModule import SSISDiscovery

    dir_path = r"C:\Users\luciano.argolo\ssis-scrapper\bing"
    target_dir = r"C:\Users\luciano.argolo\ssis-scrapper\StoreProcedures"
    valid_dirs = ['Stored Procedures']

    discovery = SSISDiscovery(dir_path, valid_dirs=valid_dirs, file_extension=".sql")
    files = discovery.get_files()
    # discovery.extract_files(target_dir, files)
    #--------------------------------------


    #--------------------------------------
    # EXTRACITING ALL .dtsx files FROM SSIS folder
    dir_path = r"C:\Users\luciano.argolo\ssis-scrapper\SSIS"
    target_dir = r"C:\Users\luciano.argolo\ssis-scrapper\dtsx"
    valid_dirs = ['StagingToEDW', 'DWBaseIncrementalLoad']

    discovery = SSISDiscovery(dir_path, valid_dirs=valid_dirs, file_extension=".sql")
    files = discovery.get_files()
    # discovery.extract_files(target_dir, files)
    #--------------------------------------

    #--------------------------------------
    # PARSING ALL .dtsx files
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

