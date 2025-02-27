# Importation des librairies
import os
import pandas as pd

# Définir la variable OnlyOne
OnlyOne = False

# Dossier contenant les fichiers CSV sonomètre sirene
dossier = "E:\SetO\\sono_data\\" # À modifier
output_file = "E:\SetO\\data_sono.csv" # À modifier

dff = []

# Faire une liste avec tous les fichiers dans le dossier
file_list = os.listdir(dossier)
total_files = len(file_list)  # Nombre total de fichiers à traiter

# Boucle pour traiter chaque fichier présent dans le dossier
for idx, file in enumerate(file_list, start=1):  # Utiliser enumerate pour un compteur
    print(f"Traitement du fichier {idx}/{total_files}: {file}")  # Affichage de la progression

    try:
        # Lecture du fichier CSV
        df = pd.read_csv(os.path.join(dossier, file), delimiter=';', low_memory=False)

        df.rename(columns={'LEQ dB-A ': 'Leq_dBA', 'Lmin dB-A ': 'Lmin_dBA', 'Lmax dB-A ': 'Lmax_dBA', 'Lpeak dB-A ': 'Lpeak_dBA'}, inplace=True)
        df = df.drop(columns=['Unnamed: 5'], errors='ignore')

        # Convertir en DateTime
        df = df.rename(columns={'Time (Date hh:mm:ss.ms)': 'DateTime'})
        df['DateTime'] = pd.to_datetime(df['DateTime'])

        columns_to_modify = ['Leq_dBA', 'Lmin_dBA', 'Lmax_dBA', 'Lpeak_dBA']
        df[columns_to_modify] = df[columns_to_modify].replace(',', '.', regex=True)

        # Afficher un fichier traité si OnlyOne est activé
        if OnlyOne:
            print(f"Traitement arrêté après le fichier : {file}")
            print(df)
            break

        dff.append(df)

    except Exception as e:
        # Afficher le nom du fichier en cas d'erreur
        print(f"Erreur lors de la lecture du fichier : {file}")
        print(f"Erreur détaillée : {e}")


# Combinaison de tous les DataFrames en un seul
if dff:
    final_df = pd.concat(dff, ignore_index=True)
    print(final_df.head())
    final_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Fichier final enregistré : {output_file}")
else:
    print("Aucun fichier traité avec succès.")


