import pandas as pd

url_csv = "https://raw.githubusercontent.com/bproencampos/integration-flow-challenge/develop/data_source/movies_metadatas.csv"

data = pd.read_csv(url_csv)

# print(df.head(5))