#%%

import pandas as pd
from SSISModule import SSISAnalyzer

disc = SSISAnalyzer(root_directory=r"C:\Users\luciano.argolo\ssis-scrapper\csv", valid_dirs=['csv'], file_extension=".csv")
df = disc.read_all_files()
df = df[['File_path']].drop_duplicates()
df.to_csv(r"C:\Users\luciano.argolo\ssis-scrapper\analysis\all_files.csv", index=False)

#Nakash_packages_ssis.csv
df['File_path'] = df['File_path'].str.lower()

df_nakash = pd.read_csv(r"C:\Users\luciano.argolo\ssis-scrapper\analysis\Nakash_packages_SSIS.csv")

df['is_contained'] = df['File_path'].apply(lambda x: any(df_nakash['path'].str.contains(x)))
df = df[df['is_contained']]

#%%
csv_main_files = df_nakash['path'].tolist() 
# %%
csv_main_files