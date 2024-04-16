# Importation des librairies
import pandas as pd

# Paramétrage de la fenêtre 'run'
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 1000)

# Définition des dates/heures de début et de fin de la période d'acquisition
date/heure_début = '2024-01-01 00:00:00'
date/heure_fin = '2024-01-01 00:00:00'

#-------------------------------------------------LAN3---------------------------------------------------------------------------------------------

# Définition du chemin du fichier LAN3
file_path = 'C:\\Users\\labot\\Downloads\\LAN3-2023-11-16.csv'



# Boucle pour l'ouverture du fichier CSV et résolution de erreurs de format
try:
    df = pd.read_csv(file_path, on_bad_lines='skip')

except pd.errors.ParserError as e:
    print("ParserError:", e)
    # Gestion des erreurs de format et affichage d'un message d'erreur

# Transformation des valeurs négatives en 0


# Transformation du format des secondes en entier (arrondissement à la seconde)
df['Second'] = df['Second'].astype(int)

# Convertion des colones mois/jours/années/heures/minutes/secondes en une colonne Datetime avec le format datetime et convertion en temps de l'est
df['DateTime'] = pd.to_datetime(df[['Month', 'Day', 'Year','Hour','Minute','Second']])
df['DateTime'] = pd.to_datetime(df['DateTime'], utc=True)
df['DateTime'] = df['DateTime'].dt.tz_convert("US/Eastern")

# Application d'un format d'affichage pour datetime pour enlever l'indice du fuseau horaire
df['DateTime'] = df['DateTime'].dt.strftime("%Y-%m-%d %H:%M:%S")

# Suppression des colonnes 'dates/temps' nouvellement assemblées en une seule
df.drop(['Month','Day','Year','Hour','Minute','Second'], axis=1, inplace=True)

# Nouveau dataframe avec seulement les données du capteur S1
df_s1= df.loc[df['Sensor'] == 'S1']

# Enlever les lignes avec flag erreur
df_s1= df_s1[df_s1['Flag'] != 'ER']

# Enlever les lignes avec champs vides dans la colonne 'ColorTemp'
df_s1= df_s1[df_s1['ColorTemperature(k)'] != 'NaN']

# Masque pour garder que les données de nuits (ou celle de la periode d'acquisition de données)
mask = (df_s1['DateTime']> date/heure_début) & (df_s1['DateTime']<= date/heure_fin)
df_s1_n = df_s1.loc[mask]

# Nouveau dataframe avec seulement les valeurs de lux
df_s1_n_lux = df_s1_n.drop(['ColorTemperature(k)','MSI','Red','Green','Blue','Clear','Flag','Sensor','Latitude','Longitude','Altitude','NumberSatellites','Gain','AcquisitionTime(ms)'], axis=1)

# Transformation des valeurs négatives en 0
df_s1_n_lux['lux'] = df_s1_n_lux['lux'].clip(lower=0)

# Nouveau dataframe avec seulement les valeurs utiles pour le calcul du MSI impact du capteur 3
df_s3= df.loc[df['Sensor'] == 'S3']
df_s3.reset_index(drop=True, inplace=True)
df_s3.drop(['Latitude','Longitude','Altitude','Red','Green','Blue','Clear','DateTime'], axis=1, inplace=True)

# Association de nouveaux noms pour les colonnes gardées (pour que pouvoir appeler directement les colonnes du capteur dans le calcul du MSI impact) 
nn_s3={'Sensor':'Sensor3', 'MSI':'MSI3','lux':'lux3','ColorTemperature(k)':'ColorTemperature(k)3','Flag':'Flag3'}
df_s3.rename(columns=nn_s3, inplace=True)

# Nouveau dataframe avec seulement les valeurs utiles pour le calcul du MSI impact du capteur 5
df_s5= df.loc[df['Sensor'] == 'S5']
df_s5.reset_index(drop=True, inplace=True)
df_s5.drop(['Red','Green','Blue','Clear'], axis=1, inplace=True)

# Association de nouveaux noms pour les colonnes gardées (pour que pouvoir appeler directement les colonnes du capteur dans le calcul du MSI impact) 
nn_s5={'Sensor':'Sensor5', 'MSI':'MSI5','lux':'lux5','ColorTemperature(k)':'ColorTemperature(k)5','Flag':'Flag5'}
df_s5.rename(columns=nn_s5, inplace=True)

# Concatenation des dataframes capteur 3 et 5 côte à côte
df_c = pd.concat([df_s3, df_s5], axis=1)

# Création d'une nouvelle colonne MSI impact et application du calcul de l'impact msi 
df_c['MSI Impact'] = ((df_c['MSI3'] * df_c['lux3']) + (df_c['MSI5'] * df_c['lux5'])) / 2

# Nettoyage des valeurs erronées
df_c = df_c.dropna()

# Suppression des lignes avec erreur dans le Flag du capteur 3 et du capteur 5
df_c= df_c[df_c['Flag3'] != 'ER']
df_c= df_c[df_c['Flag5'] != 'ER']

# Masque pour garder que les données de nuits (ou celle de la periode d'acquisition de données)
mask2 = (df_c['DateTime']> date/heure_début) & (df_c['DateTime']<= date/heure_fin)
df_c = df_c.loc[mask2]

# Conservation seulement de la colonne MSI impact 
df_MSII = df_c.drop(['Sensor3','Sensor5','ColorTemperature(k)3','ColorTemperature(k)5','MSI3','MSI5','lux3','lux5','Flag3','Flag5'], axis=1)


# Concatenation du dataframe MSI impact avec le dataframe lux du capteur 1 selon la datetime
df_combined = pd.merge(df_MSII, df_s1_n_lux, on='DateTime', how='inner')

# Suppression des duplicats (plusieurs lignes avaient la meme seconde suite à l'arrondissement des millisecondes à la seconde)
df_combined = df_combined.drop_duplicates(subset=['DateTime'])

#------------------------------------------------------Sonomètre---------------------------------------------------------------------------------------

# Définition du chemin du fichier LAN3
file_path2 = "C:\\Users\\labot\\Downloads\\2023_11_17__01h22m57s.csv"

# Boucle pour l'ouverture du fichier CSV et résolution de erreurs de format
try:
    df_sono = pd.read_csv(file_path2, on_bad_lines='skip', delimiter=';')

except pd.errors.ParserError as e:
    print("ParserError:", e)
    # Gestion des erreurs de format et affichage d'un message d'erreur

# Modification du nom de la colonne 'Time' en Datetime, convertion en format datetime et convertion en temps de l'est
df_sono = df_sono.rename(columns={'Time (Date hh:mm:ss.ms)': 'DateTime'})
df_sono['DateTime'] = pd.to_datetime(df_sono['DateTime'], utc=True)
df_sono['DateTime'] = df_sono['DateTime'].dt.tz_convert("US/Eastern")

# Application d'un format d'affichage pour datetime pour enlever l'indice du fuseau horaire
df_sono['DateTime'] = df_sono['DateTime'].dt.strftime("%Y-%m-%d %H:%M:%S")


# Changer les virgules en points pour que les valeurs soient reconnues par QGIS
df_sono.columns = df_sono.columns.str.strip()
colonnes = ['L-Max dB -A', 'LEQ dB -A', 'L-Min dB -A','L-PeakdB -A']
for col in colonnes:
    df_sono[col] = df_sono[col].str.replace(',', '.')

# Changement du format des valeurs en Float
df_sono['L-Max dB -A'] = df_sono['L-Max dB -A'].astype(float)
df_sono['LEQ dB -A'] = df_sono['LEQ dB -A'].astype(float)
df_sono['L-Min dB -A'] = df_sono['L-Min dB -A'].astype(float)
df_sono['L-PeakdB -A'] = df_sono['L-PeakdB -A'].astype(float)

# Combinaison des dataframes LAN3 et sonometre
df_cf = pd.merge(df_combined, df_sono, on='DateTime', how='inner')

# Suppression des duplicats (plusieurs lignes avaient la meme seconde suite à l'arrondissement des millisecondes à la seconde)
df_cf = df_cf.drop_duplicates(subset=['DateTime'])

#Supprimer le hashtag pour exporter les données traitées
#df_cf.to_csv(r'chemin de l'emplacement d'exportation\LAN3_Sonomètre_2024-01-01.csv', index=False, sep=',')
