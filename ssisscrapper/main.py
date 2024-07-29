#%%
from SSISModule import SSISMigrator, SSISDiscovery
from utils import create_directories
import json
import os

if __name__ == '__main__':

    #--------------------------------------
    # EXTRACITING ALL SP FROM BING.RAR FILE
    from SSISModule import SSISDiscovery
    #--------------------------------------
    # EXTRACITING ALL SP FROM BING.RAR FILE

    path = os.getcwd()
    create_directories(['dtsx', 'json', 'csv', 'analysis', 'StoreProcedures', 'Sources_and_catalogs'], path)

    # EXTRACTING ALL .params files From bing folder
    dir_path = os.path.join(path, "bing")
    target_dir = os.path.join(path, "Sources_and_catalogs")
    valid_dirs = ['StagingToEDW', 'DataLakeHRISToBase', 'DWMartIncrementalLoad', 'DataLakeBaseToMart']

    #--------------------------------------
    discovery = SSISDiscovery(dir_path, valid_dirs=valid_dirs, file_extension=".params")
    files = discovery.get_files()
    discovery.extract_files(target_dir, files)
    #--------------------------------------


    #--------------------------------------
    # EXTRACITING ALL .dtsx files From bing folder
    dir_path = os.path.join(path, "bing")
    target_dir = os.path.join(path, "StoreProcedures")
    valid_dirs = ['Stored Procedures']

    #--------------------------------------
    discovery = SSISDiscovery(dir_path, valid_dirs=valid_dirs, file_extension=".sql")
    files = discovery.get_files()
    discovery.extract_files(target_dir, files, add_prefix=False)
    #--------------------------------------


    #--------------------------------------
    # EXTRACITING ALL .dtsx files From bing folder
    dir_path = os.path.join(path, "bing")
    target_dir = os.path.join(path, "dtsx")
    valid_dirs = ['StagingToEDW', 'DataLakeHRISToBase', 'DWMartIncrementalLoad', 'DataLakeBaseToMart','DataLakeADPToBase']

    discovery = SSISDiscovery(dir_path, valid_dirs=valid_dirs, file_extension=".dtsx")
    files = discovery.get_files()
    discovery.extract_files(target_dir, files)
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
