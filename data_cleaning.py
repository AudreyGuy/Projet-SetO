import pandas as pd

fichier_entree = r"E:\SetO\data_fusion.csv" # À modifier
fichier_sortie = r"E:\SetO\data_fusion_filtre.csv" # À modifier

# Initialiser les variables pour la subdivision du fichier à ouvrir
chunk_size = 100000  # Nombre de lignes par chunk
chunks = []
chunk_count = 0  # Compteur pour les chunks

# Lecture et traitement par chunks
for chunk in pd.read_csv(fichier_entree, chunksize=chunk_size, delimiter=','):
    chunk_count += 1  # Incrémenter le compteur
    print(f"Traitement du chunk {chunk_count}")  # Afficher le numéro du chunk en cours
    chunks.append(chunk)  # Ajouter le chunk à la liste si nécessaire

# Combiner tous les chunks en un seul DataFrame
df = pd.concat(chunks, ignore_index=True)

# Filtrer et retirer les lignes où les flags des capteurs sont 'ER' (pour erreur)
df_filtered = df[
    (df['Flag_S1'] != 'ER') &
    (df['Flag_S2'] != 'ER') &
    (df['Flag_S3'] != 'ER') &
    (df['Flag_S4'] != 'ER') &
    (df['Flag_S5'] != 'ER')
]

# Filtrer et retirer les lignes où la colonne ColorTemperature est 'NaN'
df_filtered2 = df_filtered.dropna(subset=[
    'ColorTemperature(k)_S1', 'ColorTemperature(k)_S2',
    'ColorTemperature(k)_S3', 'ColorTemperature(k)_S4',
    'ColorTemperature(k)_S5'
])

columns = ['ColorTemperature(k)_S1', 'ColorTemperature(k)_S2',
           'ColorTemperature(k)_S3', 'ColorTemperature(k)_S4', 'ColorTemperature(k)_S5',
           'MSI_S1', 'MSI_S2', 'MSI_S3', 'MSI_S4', 'MSI_S5',
           'lux_S1', 'lux_S2', 'lux_S3', 'lux_S4', 'lux_S5']

# Convertir les colonnes en chaînes pour appliquer str.contains
df_filtered2[columns] = df_filtered2[columns].astype(str)

# Filtrer les lignes où la valeur des colonnes spécifiées contient un '-'
df_filtered3 = df_filtered2[~df_filtered2[columns].apply(lambda row: row.str.contains('-', na=False)).any(axis=1)]

# Sauvegarder le DataFrame filtré
df_filtered3.to_csv(fichier_sortie, index=False, encoding='utf-8')

print(f"Données filtrées sauvegardées dans {fichier_sortie}")
