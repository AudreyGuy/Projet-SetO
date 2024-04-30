# Importation des librairies
import pandas as pd
from suntime import Sun
import os
from datetime import timedelta
import pytz

# Paramétrage de la fenêtre 'run'
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 1000)

#--------------------------------------------------LAN3 vélo auto------------------------------------------------------------

# Définition du chemin du fichier LAN3 vélo auto
file1 = "C:\\Users\\labot\\Downloads\\Vélo-auto olivier\\2024-04-23_depl.csv"
file2 = "C:\\Users\\labot\\Downloads\\Vélo-auto olivier\\2024-04-24_depl.csv"


# Boucle pour l'ouverture du premier fichier CSV et résolution de erreurs de format
try:
    df1 = pd.read_csv(file1, on_bad_lines='skip')

except pd.errors.ParserError as e:
    print("ParserError:", e)
    # Gestion des erreurs de format et affichage d'un message d'erreur

# Boucle pour l'ouverture du deuxième fichier CSV et résolution de erreurs de format
try:
    df2 = pd.read_csv(file2, on_bad_lines='skip')

except pd.errors.ParserError as e:
    print("ParserError:", e)
    # Gestion des erreurs de format et affichage d'un message d'erreur


# Combiner les deux fichiers pour avoir un seul fichier LAN3 vélo auto
df = pd.concat([df1, df2], axis=0)

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

# Convertir toutes les valeurs en chaînes de caractères
df_s1['lux'] = df_s1['lux'].astype(str)
df_s1['MSI'] = df_s1['MSI'].astype(str)
df_s1['ColorTemperature(k)'] = df_s1['ColorTemperature(k)'].astype(str)

# Supprimer les lignes avec "-" comme caractère
df_s1 = df_s1[~df_s1['lux'].str.contains('-', na=False)]
df_s1 = df_s1[~df_s1['MSI'].str.contains('-', na=False)]
df_s1 = df_s1[~df_s1['ColorTemperature(k)'].str.contains('-', na=False)]

# Retransformer les valeurs en flottants
df_s1['lux'] = df_s1['lux'].astype(float)
df_s1['MSI'] = df_s1['MSI'].astype(float)
df_s1['ColorTemperature(k)'] = df_s1['ColorTemperature(k)'].astype(float)

# Enlever les lignes avec flag erreur
df_s1= df_s1[df_s1['Flag'] != 'ER']

# Enlever les lignes avec champs vides dans la colonne 'ColorTemp'
df_s1= df_s1[df_s1['ColorTemperature(k)'] != 'NaN']

# Enlever les colonnes inutiles
df_s1 = df_s1.drop(['ColorTemperature(k)','Altitude','MSI','Red','Green','Blue','Clear','Flag','Sensor','NumberSatellites','Gain','AcquisitionTime(ms)'], axis=1)


#----------------------------------------------Sonomètre vélo auto------------------------------------------------------

# Définition du chemin du fichier sonomètre vélo auto
asterix = "C:\\Users\\labot\\Downloads\\Vélo-auto olivier\\Astérix_2024_04_23__20h28m41s.csv"

# Boucle pour l'ouverture du fichier CSV et résolution de erreurs de format
try:
    df_a = pd.read_csv(asterix, on_bad_lines='skip', delimiter=';')

except pd.errors.ParserError as e:
    print("ParserError:", e)
    # Gestion des erreurs de format et affichage d'un message d'erreur

# Modification du nom de la colonne 'Time' en Datetime, convertion en format datetime et convertion en temps de l'est
df_a = df_a.rename(columns={'Time (Date hh:mm:ss.ms)': 'DateTime'})
df_a['DateTime'] = pd.to_datetime(df_a['DateTime'])

# Application d'un format d'affichage pour datetime pour enlever l'indice du fuseau horaire
df_a['DateTime'] = df_a['DateTime'].dt.strftime("%Y-%m-%d %H:%M:%S")

# Combinaison du df Strava et sonometre
df_c = pd.merge(df_s1, df_a, on='DateTime', how='inner')

# Transformer les valeurs en flottants
df_c[' L-Max dB -A '] = df_c[' L-Max dB -A '].astype(float)
df_c[' LEQ dB -A '] = df_c[' LEQ dB -A '].astype(float)
df_c[' L-Min dB -A '] = df_c[' L-Min dB -A '].astype(float)

#-----------------------------------------------------Vélo--------------------------------------------------------------

# Définir les heures de début et de fin (à partir de QGIS en identifiant les index correspondant) de la période d'acquisition en vélo
index_debut = 38
index_fin = 341

# Nouveau df avec les données vélo
df_v = df_c.iloc[index_debut:index_fin + 1]

# Calculer la moyenne des valeurs de la colonne ' LEQ dB -A ' pour vélo
moyenne_velo = df_v[' LEQ dB -A '].mean()

print("Moyenne de LEQ vélo :", moyenne_velo)
#------------------------------------------------------Auto-------------------------------------------------------------

# Définir les heures de début et de fin (à partir de QGIS en identifiant les index correspondant) de la période d'acquisition en auto
index_debut = 839
index_fin = 1183

# Nouveau df avec les données vélo
df_a = df_c.iloc[index_debut:index_fin + 1]

# Calculer la moyenne des valeurs de la colonne ' LEQ dB -A ' pour auto
moyenne_auto = df_a[' LEQ dB -A '].mean()

print("Moyenne de lEQ auto :", moyenne_auto)
#---------------------------------------------------Exportation---------------------------------------------------------


#df_v.to_csv(r'C:\Users\labot\Downloads\Vélo-auto olivier\donnée_olivier_vélo_2024-04-23.csv', index=False, sep=',')
#df_a.to_csv(r'C:\Users\labot\Downloads\Vélo-auto olivier\donnée_olivier_auto_2024-04-23.csv', index=False, sep=',')


