#%%

#FIRST PART ANALYZING AND GROUPING DIFFERENT PACKAGES FOR DIFFERENT VARIABLES
import pandas as pd
import json
import os
import re
from utils import dependencies, extract_sql_data, extract_values
from SSISModule import SSISAnalyzer, SSISDiscovery


path = os.getcwd()
root_directory = os.path.join(path, "csv")
target_dir = os.path.join(path, "analysis")

disc = SSISAnalyzer(root_directory=root_directory, valid_dirs=['csv'], file_extension=".csv")
df = disc.read_all_files()
df.to_csv(f"{target_dir}\\all_joined.csv", index=True)

#ALL DISTINCT EXEUTABLE TYPES THAT ARE IN THE PACKAGES WE ARE ANALYZING
disc.get_and_save_unique_values(df, column_name='ExecutableType')

#ALL THE QUERIES AND STORE PROCEDURES THAT ARE IN THE PACKAGES WE ARE ANALYZING
disc.get_and_save_unique_values(df, column_name='SqlTaskData')

columns=['RefId', 'SqlTaskData']
df_grouped = df.groupby(columns, as_index=True).count().reset_index(inplace=False)
df_grouped.to_csv(f"{target_dir}\\group_by_{'-'.join(columns)}.csv", index=True)



#TOTAL PACKAGES BEING CALLED BY PARENT PACKAGES
df_parent_packages = pd.read_csv(f"{target_dir}\\group_by_File_path-ExecutableType.csv")
df_parent_packages.reset_index(inplace=True, drop=True)
# df_parent_packages.drop(columns=['index'], inplace=True)
# df_parent_packages = df_parent_packages[df_parent_packages.columns[:3]]
df = df_parent_packages.where(df_parent_packages['ExecutableType'] == 'Microsoft.ExecutePackageTask').dropna()
df.rename(columns={'RefId':'TotalPackagesCalled'}, inplace=True)
df.to_csv(f"{target_dir}\\total_ParentPackages.csv", index=False)


#TOTAL STORE PROCEDURES CALLED IN ALL PACKAGES AND QUERIES
#filter by "EXEC" or "EXECUTE" in each row in column "sql Task Data"

df = pd.read_csv(f"{target_dir}\\all_joined.csv")
df = df[df['SqlTaskData'].str.contains('^[" ]?Exec', case=False, na=False)]
df['store_procedure_name'] = df['SqlTaskData'].str.extract('(sp[a-zA-Z_]+)', flags=re.IGNORECASE)[0]
#EXEC\s+([a-zA-Z_.\[\]]+)|Execute\s+([a-zA-Z_.\[\]]+)
df = df[['File_path', 'store_procedure_name']].drop_duplicates()
df.to_csv(f"{target_dir}\\total_StoreProcedures.csv", index=False)



#GENERATE TREE OF DEPENDENCIES
dir_path = os.path.join(path, "bing")
target_dir = os.path.join(path, "analysis")
valid_dirs = ['StagingToEDW', 'DataLakeHRISToBase', 'DWMartIncrementalLoad', 'DataLakeBaseToMart']

discovery = SSISDiscovery(dir_path, valid_dirs=valid_dirs, file_extension=".dtsx")
files_path = discovery.get_files()

map_dict = {}

for file_path in files_path:
    package_name = "|".join(file_path.split("\\")[-2:])
    map_dict.update({package_name: dependencies(file_path)})

with open(os.path.join(target_dir,'parenthood_relations.json') , "w") as f:
    f.write(json.dumps(map_dict, indent=4))

#%%

# GETS ALL THE SOURCES AND CATALOGS USED IN THE PACKAGES FOR EACH PROJECT
root_directory = os.path.join(path, "Sources_and_catalogs")
disc = SSISAnalyzer(root_directory=root_directory, valid_dirs=['Sources_and_catalogs'], file_extension=".params")
all_files_path = disc.get_files()
pattern = "//*[local-name()='Parameter']/*[local-name()='Properties']/*[local-name()='Property'][7]/text()"
df = extract_values(all_files_path, pattern, split_values=True, add_prefix=False)
df.to_csv(f"{target_dir}\\sources_and_catalogs.csv", index=False)   

#%%
import pandas as pd

df = pd.read_csv(path + "\\analysis\\total_StoreProcedures.csv")

root_directory = os.path.join(path, "StoreProcedures")
disc = SSISAnalyzer(root_directory=root_directory, valid_dirs=[".sql"], file_extension=".sql")
all_files_path = disc.get_files()
        
for file_path in all_files_path:
    with open(file_path, "r") as f:
        data = f.read()
        file_name = re.findall("(sp[a-zA-Z_]+)", file_path.replace(root_directory, ""))
        file_name = file_name[0] if len(file_name) > 0 else ""       
        df.loc[df['store_procedure_name'] == file_name, 'SqlTaskData'] = data
        df.loc[df['store_procedure_name'] == file_name, 'Match'] = True

df_final = extract_sql_data(df, columns_to_keep=['File_path', 'store_procedure_name', 'Extracted', 'db'])        
df = df[['File_path', 'store_procedure_name', 'Match']]
df.to_csv(path + "\\analysis\\matching_SPname_with_SPfiles.csv", index=False)
df_final.to_csv(path + "\\analysis\\extracted_sql_from_sp.csv", index=False)



#EXTRACTING ALL DATA CONSIDERED RELEVANT FROM QUERIES AND STORE PROCEDURES IN THE PACKAGES
root_directory = os.path.join(path, "bing")
disc = SSISAnalyzer(root_directory=root_directory, valid_dirs=['StagingToEDW', 'DataLakeHRISToBase', 'DWMartIncrementalLoad', 'DataLakeBaseToMart'], file_extension=".dtsx")
all_files_path = disc.get_files()

pattern = "//*[local-name()='component']/*[local-name()='properties']/*[local-name()='property']/text()"
df2 = extract_values(all_files_path, pattern, add_prefix=True)
df = pd.read_csv(path + "\\analysis\\all_joined.csv")

df_concatenated = pd.concat([df, df2], axis=0, ignore_index=True).sort_values(by="File_path")

df_final = extract_sql_data(df_concatenated)
df_final.to_csv(path + "\\analysis\\tables_sql.csv", index=False)

#%%