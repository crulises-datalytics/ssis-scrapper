#%%

#FIRST PART ANALYZING AND GROUPING DIFFERENT PACKAGES FOR DIFFERENT VARIABLES
import pandas as pd
import json
import os
from utils import dependencies
from SSISModule import SSISAnalyzer, SSISDiscovery


path = os.getcwd()
root_directory = os.path.join(path, "csv")
target_dir = os.path.join(path, "analysis")

disc = SSISAnalyzer(root_directory=root_directory, valid_dirs=['csv'], file_extension=".csv")
df = disc.read_all_files()

#ALL DISTINCT EXEUTABLE TYPES THAT ARE IN THE PACKAGES WE ARE ANALYZING
disc.get_and_save_unique_values(df, column_name='ExecutableType')

#ALL THE QUERIES AND STORE PROCEDURES THAT ARE IN THE PACKAGES WE ARE ANALYZING
disc.get_and_save_unique_values(df, column_name='SqlTaskData')

columns=['RefId', 'SqlTaskData']
df_grouped = df.groupby(columns, as_index=True).count().reset_index(inplace=False)
df_grouped.to_csv(f"{target_dir}\\group_by_{'-'.join(columns)}.csv", index=True)


columns = ['File_path', 'ExecutableType']
df = df.groupby(columns, as_index=True).count().reset_index(inplace=False)[columns + ['RefId']]
df.to_csv(f"{target_dir}\\group_by_{'-'.join(columns)}.csv", index=True)


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

df = pd.read_csv(f"{target_dir}\\total_SqlTaskData.csv")
df = df[df['SqlTaskData'].str.contains('^[" ]?Exec', case=False, na=False)]
df.to_csv(f"{target_dir}\\total_StoreProcedures.csv", index=False)


dir_path = os.path.join(path, "SSIS")
target_dir = os.path.join(path, "analysis")
valid_dirs = ['StagingToEDW', 'DWBaseIncrementalLoad']

discovery = SSISDiscovery(dir_path, valid_dirs=valid_dirs, file_extension=".dtsx")
files_path = discovery.get_files()

map_dict = {}

for file_path in files_path:
    package_name = "|".join(file_path.split("\\")[-2:])
    map_dict.update({package_name: dependencies(file_path)})

with open(os.path.join(target_dir,'dependencies.json') , "w") as f:
    f.write(json.dumps(map_dict, indent=4))