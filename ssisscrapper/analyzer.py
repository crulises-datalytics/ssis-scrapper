#%%

import pandas as pd
from SSISModule import SSISAnalyzer

disc = SSISAnalyzer(r"C:\Users\luciano.argolo\ssis-scrapper\csv")
df = disc.read_all_files(file_extension=".csv")
#disc.get_and_save_unique_values(df, column_name='ExecutableType')

# %%

#i want to only keep the columns ExecutableTYpe, and File Path
df = df[['ExecutableType', 'File_path']]
df.groupby('File_path').count()
# %%
