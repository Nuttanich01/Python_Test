import pandas as pd

real = pd.read_csv('D:/Desktop/Test/Test_pandas/data/transform_data.csv')
real_df = pd.DataFrame(real)
#print(real_df.info())
real_df.to_csv("D:/Desktop/Test/Test_pandas/data/real_data.csv", index=False)