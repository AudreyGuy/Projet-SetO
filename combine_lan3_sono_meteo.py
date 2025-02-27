import pandas as pd
from tqdm import tqdm

chemin_data_lan3 = "E:\SetO\\data_lan3.csv" #À modifier
chemin_data_sono = "E:\SetO\\data_sono.csv" #À modifier
chemin_data_meteo = "E:\SetO\\meteo_lennox.csv" #À modifier
fichier_sortie = "E:\SetO\\data_fusion.csv" #À modifier

# Définition de l'intervalle de temps
start_datetime = pd.Timestamp('2023-11-08 00:00:00')
end_datetime = pd.Timestamp('2024-02-02 08:00:00')

# Fonction pour ouvrir les fichiers de données et sélectionner uniquement les données dans l'intervalle de temps définit
def read_and_filter_csv(file_path, datetime_column, start_datetime, end_datetime, chunk_size=10000):
    chunks = []
    with tqdm(total=None, desc=f"Processing {file_path}", unit="chunk") as pbar:
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            chunk[datetime_column] = pd.to_datetime(chunk[datetime_column])
            filtered_chunk = chunk[(chunk[datetime_column] >= start_datetime) & (chunk[datetime_column] <= end_datetime)]
            chunks.append(filtered_chunk)
            pbar.update(1)  # Update the progress bar for each chunk
    return pd.concat(chunks, ignore_index=True)

# Ouverture et filtre des données lan3
df_lan3_filtre = read_and_filter_csv(chemin_data_lan3,
    datetime_column='DateTime',
    start_datetime=start_datetime,
    end_datetime=end_datetime
)

# Ouverture et filtre des données du sonomètre
df_sono_filtre = read_and_filter_csv(chemin_data_sono,
    datetime_column='DateTime',
    start_datetime=start_datetime,
    end_datetime=end_datetime
)

# Ouverture du fichier de données météo
df_meteo = pd.read_csv(chemin_data_meteo,
    chunksize=10000, delimiter=';'
)
df_meteo = pd.concat(df_meteo, ignore_index=True)  # Assuming no filtering is needed for this file
df_meteo.rename(columns={'Date/Heure (HNL)': 'DateTime'}, inplace=True)
df_meteo['DateTime'] = pd.to_datetime(df_meteo['DateTime'], errors='coerce')  # Ensure consistent datetime type

# Combinaison des données du lan3 et du sonomètre
df_lan_sono = pd.merge(df_lan3_filtre, df_sono_filtre, on='DateTime', how='left')

# Ajout des données météo aux données du lan3 et du sonomètre
df_meteo['DateTime_hour'] = df_meteo['DateTime'].dt.floor('H')
df_lan_sono['DateTime_hour'] = df_lan_sono['DateTime'].dt.floor('H')

df_lan_sono_meteo = pd.merge(df_lan_sono, df_meteo, on='DateTime_hour', how='left')
df_lan_sono_meteo = df_lan_sono_meteo.drop(columns=['DateTime_hour', 'x', 'y', 'DateTime_y'], errors='ignore')

# Enregistrement du fichier final
df_lan_sono_meteo.to_csv(fichier_sortie, index=False, encoding='utf-8')
