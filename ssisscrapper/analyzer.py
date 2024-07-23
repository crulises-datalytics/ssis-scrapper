#%%

#FIRST PART ANALYZING AND GROUPING DIFFERENT PACKAGES FOR DIFFERENT VARIABLES
import pandas as pd
import json
import os
import re
from utils import dependencies, extract_sql_data
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

df.to_csv(f"{target_dir}\\all_joined.csv", index=True)


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


dir_path = os.path.join(path, "bing")
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

#EXTRACTING ALL DATA CONSIDERED RELEVANT FROM QUERIES AND STORE PROCEDURES IN THE PACKAGES
df = pd.read_csv(path+"\\analysis\\all_joined.csv")
df = extract_sql_data(df)
df.to_csv(path+"\\analysis\\tables_sql.csv", index=False)

#%%

from lxml import etree

root_directory = os.path.join(path, "Sources_and_catalogs")
disc = SSISAnalyzer(root_directory=root_directory, valid_dirs=['Sources_and_catalogs'], file_extension=".params")
all_files_path = disc.get_files()

for file_path in all_files_path:
    tree = etree.parse(file_path)
    # XPath query to select the text, ignoring namespaces
    xpath_query = "//*[local-name()='Parameter']/*[local-name()='Properties']/*[local-name()='Property'][7]/text()"
    #xpath_query = "//SSIS:Parameter/SSIS:Properties/SSIS:Property[7]/text()"

    # Execute the XPath query
    values = tree.xpath(xpath_query)

    # Print the extracted values
    for value in values:
        if value:
            print(file_path)
            print(value.split(';')[:-1])
        

#%%

#file_path = r'C:\Users\luciano.argolo\ssis-scrapper\bing\BING SSIS\DataLakeHRISToBase\DataLakeHRISToBase\FND_FLEX_VALUE_SETS_B0.dtsx'

root_directory = os.path.join(path, "bing")
disc = SSISAnalyzer(root_directory=root_directory, valid_dirs=['DataLakeHRISToBase\DataLakeHRISToBase'], file_extension=".dtsx")
all_files_path = disc.get_files()

for file_path in all_files_path:

    if "test" not in file_path:
        tree = etree.parse(file_path)
        # XPath query to select the text, ignoring namespaces
        xpath_query = "//*[local-name()='component']/*[local-name()='properties']/*[local-name()='property']/text()"

        #xpath_query = "//*[local-name()='component']/*[local-name()='properties']/*[local-name()='property']/text()"

        # Execute the XPath query
        values = tree.xpath(xpath_query)

        # Print the extracted values
        # print(file_path, values, "\n\n")
        
        
        
#%%
import re

# Your regex pattern
pattern = r"from\s+[ _@A-Za-z0-9.\[\]]+|FROM\s+[ _@A-Za-z0-9.\[\]]+|join[ _A-Za-z0-9.\[\]]+|insert\s+into\s+[ _@A-Za-z0-9.\[\]]+|declare[ _A-Za-z0-9.\[\]]+|update\s+[ _A-Za-z0-9.\[\]]+"

# List of values to search
values = ['0', '[dbo].[TimeInBN]', '1252', 'false', '3', 'false', 'false', 'CHECK_CONSTRAINTS', '2147483647', '0', 'UPDATE dbo.TimeInBN SET\n\tMinimumDays\t= ?,\n\tMaximumDays\t= ?,\n\tTimeInGrouping\t= ?,\n\tBaseModifiedDate\t= ?,\n\tUpdateAuditID\t= ?\nWHERE\n\tTimeInID\t\t= ?', '1252', 'User::vInsertCount', 'select * from [dbo].[TimeInBN]', 'select * from (select * from [dbo].[TimeInBN]) [refTable]\nwhere [refTable].[TimeInID] = ?', '0', '0', '1', '0', '25', '25', '<referenceMetadata><referenceColumns><referenceColumn name="TimeInID" dataType="DT_I4" length="0" precision="0" scale="0" codePage="0"/><referenceColumn name="MinimumDays" dataType="DT_I4" length="0" precision="0" scale="0" codePage="0"/><referenceColumn name="MaximumDays" dataType="DT_I4" length="0" precision="0" scale="0" codePage="0"/><referenceColumn name="TimeInGrouping" dataType="DT_STR" length="100" precision="0" scale="0" codePage="1252"/><referenceColumn name="BaseCreatedDate" dataType="DT_DBTIMESTAMP2" length="0" precision="0" scale="7" codePage="0"/><referenceColumn name="BaseModifiedDate" dataType="DT_DBTIMESTAMP2" length="0" precision="0" scale="7" codePage="0"/><referenceColumn name="InsertAuditID" dataType="DT_I8" length="0" precision="0" scale="0" codePage="0"/><referenceColumn name="UpdateAuditID" dataType="DT_I8" length="0" precision="0" scale="0" codePage="0"/></referenceColumns></referenceMetadata>', '#{Package\\Base - Data Load\\DFT - Load Data From tfnHR_BaseGenerate_TimeInBN\\SRC - tfnHR_BaseGenerate_TimeInBN.Outputs[OLE DB Source Output].Columns[TimeInID]};', '1252', 'false', '0', 'SELECT * FROM [dbo].[tfnHR_BaseGenerate_TimeInBN] ()', '1252', 'false', '2', 'User::vUpdateCount']

# Filter values that match the regex pattern
matched_values = [val for val in values if re.search(pattern, val, re.IGNORECASE)]

# Print matched values
for val in matched_values:
    print(val)