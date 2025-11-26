import pandas as pd

# carica il csv
df = pd.read_csv("./data/db_csv/store_inventory_final.csv")

# controlla se ci sono duplicati sulla colonna 'shelf_id'
duplicati = df[df.duplicated(subset="shelf_id", keep=False)]

if duplicati.empty:
    print("Nessun duplicato su 'shelf_id'.")
else:
    print("Ci sono duplicati su 'shelf_id':")
    print(duplicati)



# conta le occorrenze di ogni shelf_id
counts = df["shelf_id"].value_counts()

# prendi solo quelli che compaiono più di una volta
dupe_ids = counts[counts > 1]
print(dupe_ids)
