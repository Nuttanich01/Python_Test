import pandas as pd

raw = pd.read_csv('D:/Desktop/Test/Test_pandas/data/raw_data.csv')
df_raw = pd.DataFrame(raw)
#print(df_raw.info())
df_raw["Daily_Min_Temp"].fillna("0",inplace=True)
#print(df_raw.info())
df_raw.to_csv("D:/Desktop/Test/Test_pandas/data/transform_data.csv", index=False)
