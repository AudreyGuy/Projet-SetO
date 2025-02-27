import os
import pandas as pd

# Dossier contenant les fichiers CSV individuels du lan3
dossier_data_lan3 = "E:\SetO\\lan3_data\\" # À modifier
fichier_sortie = "E:\SetO\\data_lan3.csv" # À modifier

# Définir la variable OnlyOne
OnlyOne = False

# Liste pour stocker les DataFrames transformés
dff = []

# Faire une liste avec tous les fichiers dans le dossier
file_list = os.listdir(dossier_data_lan3)
total_files = len(file_list)  # Nombre total de fichiers à traiter

# Boucle pour traiter chaque fichier présent dans le dossier
for idx, file in enumerate(file_list, start=1):  # Utiliser enumerate pour un compteur
    print(f"Traitement du fichier {idx}/{total_files}: {file}")  # Affichage de la progression

    try:
        # Lecture du fichier CSV
        df = pd.read_csv(os.path.join(dossier_data_lan3, file), delimiter=',', low_memory=False)

        # Suppression des colonnes inutiles
        columns_to_drop = ['NumberSatellites', 'Gain', 'AcquisitionTime(ms)', 'Latitude', 'Longitude', 'Altitude']
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')

        # Création de la colonne DateTime
        date_columns = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Second']
        if all(col in df.columns for col in date_columns):
            df['DateTime'] = pd.to_datetime(df[['Year', 'Month', 'Day', 'Hour', 'Minute', 'Second']])
            df.drop(columns=date_columns, inplace=True, errors='ignore')
        else:
            print(f"Colonnes de date/temps manquantes dans le fichier {file}, saut du fichier.")
            continue

        # Pivot des données
        if 'Sensor' in df.columns:
            df_pivot = df.pivot(index='DateTime', columns='Sensor',
                                values=['ColorTemperature(k)', 'MSI', 'lux', 'Red', 'Green', 'Blue', 'Clear', 'Flag'])
            df_pivot.columns = ['_'.join(col).strip() for col in df_pivot.columns.values]
            df_pivot.reset_index(inplace=True)
        else:
            print(f"Colonne 'Sensor' manquante dans le fichier {file}, saut du fichier.")
            continue

        # Calcul du MSI Impact (si les colonnes nécessaires existent)
        if all(col in df_pivot.columns for col in ['MSI_S3', 'lux_S3', 'MSI_S5', 'lux_S5']):
            df_pivot['MSI_Impact'] = ((df_pivot['MSI_S3'] * df_pivot['lux_S3']) +
                                      (df_pivot['MSI_S5'] * df_pivot['lux_S5'])) / 2
        else:
            print(f"Colonnes nécessaires pour MSI Impact manquantes dans le fichier {file}, saut du calcul.")

        # Conversion en fuseau horaire de l'Est
        df_pivot['DateTime'] = pd.to_datetime(df_pivot['DateTime'], utc=True)
        df_pivot['DateTime'] = df_pivot['DateTime'].dt.tz_convert("US/Eastern")
        df_pivot['DateTime'] = df_pivot['DateTime'].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Afficher un fichier traité si OnlyOne est activé
        if OnlyOne:
            print(f"Traitement arrêté après le fichier : {file}")
            print(df_pivot)
            break

        # Ajouter le DataFrame pivoté à la liste
        dff.append(df_pivot)

    except Exception as e:
        print(f"Erreur lors de la lecture ou du traitement du fichier : {file}")
        print(f"Erreur détaillée : {e}")

# Combinaison de tous les DataFrames en un seul
if dff:
    final_df = pd.concat(dff, ignore_index=True)
    print(final_df.head())
    final_df.to_csv(fichier_sortie, index=False, encoding='utf-8')
    print(f"Fichier final enregistré : {fichier_sortie}")
else:
    print("Aucun fichier traité avec succès.")
