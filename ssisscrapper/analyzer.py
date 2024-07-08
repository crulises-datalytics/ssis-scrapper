#%%

import pandas as pd
from SSISModule import SSISAnalyzer

disc = SSISAnalyzer(r"C:\Users\luciano.argolo\ssis-scrapper\csv")
df = disc.read_all_files(file_extension=".csv")
#%%
disc.get_and_save_unique_values(df, column_name='ExecutableType')
disc.get_and_save_unique_values(df, column_name='SqlTaskData')

#%%
columns=['RefId', 'SqlTaskData']
df = df.groupby(columns, as_index=True).count().reset_index(inplace=False)
df.to_csv(f"C:\\Users\\luciano.argolo\\ssis-scrapper\\analysis\\group_by_{'-'.join(columns)}.csv", index=True)

# %%
df
#%%
#i want to only keep the columns ExecutableTYpe, and File Path
#df = df[['ExecutableType', 'File_path']]
columns = ['File_path', 'ExecutableType']
df = df.groupby(columns, as_index=True).count().reset_index(inplace=False)
#%%

df
#%%
#save it to a csv file
df.to_csv(f"C:\\Users\\luciano.argolo\\ssis-scrapper\\analysis\\group_by_{'-'.join(columns)}.csv", index=True)
# %%
df
#%%
df