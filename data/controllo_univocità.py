import pandas as pd

# load the csv
df = pd.read_csv("./data/db_csv/store_inventory_final.csv")

# check for duplicates in the 'shelf_id' column
duplicati = df[df.duplicated(subset="shelf_id", keep=False)]

if duplicati.empty:
    print("No duplicates on 'shelf_id'.")
else:
    print("There are duplicates on 'shelf_id':")
    print(duplicati)



# count occurrences of each shelf_id
counts = df["shelf_id"].value_counts()

# keep only those that appear more than once
dupe_ids = counts[counts > 1]
print(dupe_ids)
