#%%
#FIRST PART ANALYZING AND GROUPING DIFFERENT PACKAGES FOR DIFFERENT VARIABLES

import pandas as pd
from SSISModule import SSISAnalyzer

disc = SSISAnalyzer(root_directory=r"C:\Users\luciano.argolo\ssis-scrapper\csv", valid_dirs=['csv'], file_extension=".csv")
df = disc.read_all_files()


disc.get_and_save_unique_values(df, column_name='ExecutableType')
disc.get_and_save_unique_values(df, column_name='SqlTaskData')

columns=['RefId', 'SqlTaskData']
df_grouped = df.groupby(columns, as_index=True).count().reset_index(inplace=False)
df_grouped.to_csv(f"C:\\Users\\luciano.argolo\\ssis-scrapper\\analysis\\group_by_{'-'.join(columns)}.csv", index=True)


columns = ['File_path', 'ExecutableType']

df = df.groupby(columns, as_index=True).count().reset_index(inplace=False)[columns + ['RefId']]
df.to_csv(f"C:\\Users\\luciano.argolo\\ssis-scrapper\\analysis\\group_by_{'-'.join(columns)}.csv", index=True)

#------------------------------------------------
#%%

#------------------------------------------------
#FINDING IF ALL STORE PROCEDURES IN SSIS ARE IN BING.RAR (THANKS GOD IT SEEMS SO)
import pandas as pd
df_bing = pd.read_csv(r"C:\Users\luciano.argolo\ssis-scrapper\extracted_unique_sp_from_bing_rar.csv")
df_ssis = pd.read_csv(r"C:\Users\luciano.argolo\ssis-scrapper\extracted_unique_sp_from_ssis.csv")

#transform to lower all values in column "NAME"
df_bing['NAME'] = df_bing['NAME'].str.lower()
df_ssis['NAME'] = df_ssis['NAME'].str.lower()

#now perform a left join on df_ssis
df = pd.merge(df_ssis, df_bing, how='right', left_on='NAME', right_on='NAME')
assert df.count() == 117
#------------------------------------------------
#%%
#------------------------------------------------
# TRYING TO UNDERSTAND HOW EVERYTHING IS CONNECTED AND BEING EXECUTED
# AND HOW MANY PACKAGES ARE BEING EXECUTED TOO
import pandas as pd
df_parent_packages = pd.read_csv(r"C:\Users\luciano.argolo\ssis-scrapper\analysis\ParentPackages_by_executable_types.csv")

df_parent_packages.reset_index(inplace=True, drop=True)
df_parent_packages.drop(columns=['index'], inplace=True)


df_parent_packages = df_parent_packages[df_parent_packages.columns[:3]]
df = df_parent_packages.where(df_parent_packages['ExecutableType'] == 'Microsoft.ExecutePackageTask').dropna()
df['TotalPackages'] = df['TotalPackages'].astype(int)

df['TotalPackages'].sum()
#CONCLUSIÓN:

# En total se utilizan 117 Store Procedures (únicos), de forma repetida se llaman en total 170 aproximadamente
# Hay 16 Parent Packages (que desde ahí en teoría llaman a todos)
# 188 packages se llaman desde los Parent Packages
#------------------------------------------------
#%%

import pandas as pd
df_parent_packages = pd.read_csv(r"C:\Users\luciano.argolo\ssis-scrapper\analysis\group_by_File_path-ExecutableType.csv")

df_parent_packages.reset_index(inplace=True, drop=True)
df_parent_packages.drop(columns=['index'], inplace=True)



df_parent_packages = df_parent_packages[df_parent_packages.columns[:3]]
df = df_parent_packages.where(df_parent_packages['ExecutableType'] == 'Microsoft.ExecutePackageTask').dropna()
df['RefId'] = df['RefId'].astype(int)

df['RefId'].sum()

# En todos los paquetes se llaman a 441 paquetes
# Sacando los ParentPackages que llaman a 188 paquetes
# quedan 253 paquetes que son llamados por "SonPackages"
# %%
#TOTAL STORE PROCEDURES CALLED IN ALL PACKAGES AND QUERIES
#filter by "EXEC" or "EXECUTE" in each row in column "sql Task Data"

df = pd.read_csv(r"C:\Users\luciano.argolo\ssis-scrapper\analysis\total_SqlTaskData.csv")
df = df[df['SqlTaskData'].str.contains("EXEC", case=False, na=False)]
df.count()

#%%
#ESTIMATIONS ANALYSIS

import pandas as pd

df_estimations = pd.read_csv(r"C:\Users\luciano.argolo\ssis-scrapper\analysis\estimations.csv")
df_packages = pd.read_csv(r"C:\Users\luciano.argolo\ssis-scrapper\analysis\group_by_File_path-ExecutableType.csv")

#join on column "Executable Type" and bring all the columns from both packages, using inner join
df = pd.merge(df_estimations, df_packages, how='inner', left_on='ExecutableType', right_on='ExecutableType')
df.rename(columns={'RefId': 'total', 'File_path_x': 'File_path'}, inplace=True)


df = df[['File_path', 'ExecutableType', 'difficulty', 'total']]
df['Estimation'] = df['difficulty'] * df['total']
df = df.sort_values(by=['File_path', 'Estimation'], ascending=[False, True])
df.to_csv(r"C:\Users\luciano.argolo\ssis-scrapper\analysis\clasification_estimation_by_executable_type.csv", index=False)

#%%
df = df[['File_path', 'Estimation']]
df.groupby('File_path').sum().sort_values(by='File_path', ascending=True).to_csv(r"C:\Users\luciano.argolo\ssis-scrapper\analysis\clasification_estimation_by_file_path.csv", index=True)
# %%
