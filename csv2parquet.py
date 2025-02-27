import pandas as pd

file_path = "C://Users//Audrey//Documents//GitHub//Projet-SetO//data_fusion_filtre.csv"
df = pd.read_csv(file_path, parse_dates=["DateTime_x"])
df.to_parquet("C://Users//Audrey//Documents//GitHub//Projet-SetO//data.parquet", engine="pyarrow", compression="snappy")