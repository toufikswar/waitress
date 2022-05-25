import pandas as pd

df = pd.read_excel("./all_remote_actions-sample.xlsx", index_col=0)


for index, row in df.iterrows():
    print(row["Category"])