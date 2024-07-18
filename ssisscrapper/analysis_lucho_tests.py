#%%
import pandas as pd
import json
import os
from utils import dependencies
from SSISModule import SSISAnalyzer, SSISDiscovery


path = os.getcwd()
root_directory = os.path.join(path, "csv")
target_dir = os.path.join(path, "analysis")

#------------------------------------------------
#FINDING IF ALL STORE PROCEDURES IN SSIS ARE IN BING.RAR (THANKS GOD IT SEEMS SO)
import pandas as pd
df_bing = pd.read_csv(path+"\\"+"extracted_unique_sp_from_bing_rar.csv")
df_ssis = pd.read_csv(path+"\\"+"extracted_unique_sp_from_ssis.csv")

#transform to lower all values in column "NAME"
df_bing['NAME'] = df_bing['NAME'].str.lower()
df_ssis['NAME'] = df_ssis['NAME'].str.lower()

#now perform a left join on df_ssis
df = pd.merge(df_ssis, df_bing, how='right', left_on='NAME', right_on='NAME')
assert df.count() == 117
#------------------------------------------------

# %%
#TOTAL STORE PROCEDURES CALLED IN ALL PACKAGES AND QUERIES
#filter by "EXEC" or "EXECUTE" in each row in column "sql Task Data"

df = pd.read_csv(path+"\\"+"analysis\total_SqlTaskData.csv")
df = df[df['SqlTaskData'].str.contains("EXEC", case=False, na=False)]
df.count()

#%%
#ESTIMATIONS ANALYSIS

import pandas as pd

df_estimations = pd.read_csv(path+"\\"+"estimations.csv")
df_packages = pd.read_csv(path+"\\"+"group_by_File_path-ExecutableType.csv")

#join on column "Executable Type" and bring all the columns from both packages, using inner join
df = pd.merge(df_estimations, df_packages, how='inner', left_on='ExecutableType', right_on='ExecutableType')
df.rename(columns={'RefId': 'total', 'File_path_x': 'File_path'}, inplace=True)


df = df[['File_path', 'ExecutableType', 'difficulty', 'total']]
df['Estimation'] = df['difficulty'] * df['total']
df = df.sort_values(by=['File_path', 'Estimation'], ascending=[False, True])
df.to_csv(path+"\\"+"clasification_estimation_by_executable_type.csv", index=False)

#%%
df = df[['File_path', 'Estimation']]
df.groupby('File_path').sum().sort_values(by='File_path', ascending=True).to_csv(path+"\\"+"clasification_estimation_by_file_path.csv", index=True)

disc = SSISAnalyzer(path+"\\"+"csv")
df = disc.read_all_files(file_extension=".csv")
#disc.get_and_save_unique_values(df, column_name='ExecutableType')

# %%

#i want to only keep the columns ExecutableTYpe, and File Path
df = df[['ExecutableType', 'File_path']]
df.groupby('File_path').count()

