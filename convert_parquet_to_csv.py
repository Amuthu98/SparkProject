import pandas as pd
import os


# convert all .parquet file to csv
for file in os.listdir("spark-streaming-esgi/my_parquet3.parquet3/"):
    if file.endswith(".parquet"):
        file_path = os.path.join("spark-streaming-esgi/my_parquet3.parquet3/", file)
        print(os.path.join("spark-streaming-esgi/my_parquet3.parquet3/", file))
        print("converting ...")
        df = pd.read_parquet(os.path.join("spark-streaming-esgi/my_parquet3.parquet3/", file))
        new_path = file_path.replace("snappy.parquet", "snappy.csv")
        df.to_csv(new_path)

# when all csv created, get them all in a list
all_csv = []
for file in os.listdir("spark-streaming-esgi/my_parquet3.parquet3/"):
    if file.endswith(".csv"):
       all_csv.append(os.path.join("spark-streaming-esgi/my_parquet3.parquet3/", file)) 

# merge all csvs into one dataframe, do transformations to delete useless data then create final csv
combined_csv = pd.concat([pd.read_csv(f) for f in all_csv ])
combined_csv["count"] = combined_csv["count"].astype(int)
print(combined_csv.dtypes)

print("before : \n", combined_csv.head(), "\n")
# keep only max rows for normal day and weekend
combined_csv = combined_csv.groupby(["normal_day_or_weekend"])
combined_csv = combined_csv.apply(lambda x: x.sort_values(["count"], ascending=False))
combined_csv = combined_csv.reset_index(drop=True)
combined_csv = combined_csv.groupby('normal_day_or_weekend').head(1)

print("after : \n",combined_csv, "\n")

# delete all csv (only combined will remain)
for file in all_csv:
    os.remove(file)
#print(df.loc[df['Value'].idxmax()])


combined_csv.to_csv("spark-streaming-esgi/my_parquet3.parquet3/combined.csv")




# df = pd.read_parquet('spark-streaming-esgi/my_parquet3.parquet3/part-00198-b4612866-1878-4e97-911b-fc3bd7593609-c000.snappy.parquet')
# df.to_csv('spark-streaming-esgi/my_parquet3.parquet3/last_converted_parquet.csv')