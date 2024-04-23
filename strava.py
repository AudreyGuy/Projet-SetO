# Importation des librairies
import pandas as pd
from gpx_converter import Converter

# Paramétrage de la fenêtre 'run'
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 1000)

# Convertir le fichier gpx Strava en fichier csv
#Converter(input_file="C:\\Users\\labot\\Downloads\\V_lo_dans_l_apr_s_midi.gpx").gpx_to_csv(output_file="C:\\Users\\labot\\Downloads\\V_lo_dans_l_apr_s_midi.csv")

# Ouverture du dataframe
df = pd.read_csv("C:\\Users\\labot\\Downloads\\V_lo_dans_l_apr_s_midi.csv")

# Convertir en DateTime
df = df.rename(columns={'time': 'DateTime'})
df['DateTime'] = pd.to_datetime(df['DateTime'], utc=True)
df['DateTime'] = df['DateTime'].dt.tz_convert("US/Eastern")
df['DateTime'] = df['DateTime'].dt.strftime("%Y-%m-%d %H:%M:%S")

df.drop(['altitude'], axis=1, inplace=True)


#--------------------------------------------Sonomètre Astérix---------------------------------------------------------

# Définition du chemin du fichier sono
asterix = "C:\\Users\\labot\\Downloads\\Astérix_2024_04_02__12h59m43s.csv"

# Boucle pour l'ouverture du fichier CSV et résolution de erreurs de format
try:
    df_asterix = pd.read_csv(asterix, on_bad_lines='skip', delimiter=';')

except pd.errors.ParserError as e:
    print("ParserError:", e)
    # Gestion des erreurs de format et affichage d'un message d'erreur

# Modification du nom de la colonne 'Time' en Datetime, convertion en format datetime et convertion en temps de l'est
df_asterix = df_asterix.rename(columns={'Time (Date hh:mm:ss.ms)': 'DateTime'})
df_asterix['DateTime'] = pd.to_datetime(df_asterix['DateTime'])

# Application d'un format d'affichage pour datetime pour enlever l'indice du fuseau horaire
df_asterix['DateTime'] = df_asterix['DateTime'].dt.strftime("%Y-%m-%d %H:%M:%S")

# Combinaison du df Strava et sonometre
df_c = pd.merge(df, df_asterix, on='DateTime', how='inner')

#--------------------------------------------Sonomètre Obélix---------------------------------------------------------

# Définition du chemin du fichier sono
obelix = "C:\\Users\\labot\\Downloads\\Obélix_2024_04_02__13h02m05s.csv"

# Boucle pour l'ouverture du fichier CSV et résolution de erreurs de format
try:
    df_obelix = pd.read_csv(obelix, on_bad_lines='skip', delimiter=';')

except pd.errors.ParserError as e:
    print("ParserError:", e)
    # Gestion des erreurs de format et affichage d'un message d'erreur

# Modification du nom de la colonne 'Time' en Datetime, convertion en format datetime et convertion en temps de l'est
df_obelix = df_obelix.rename(columns={'Time (Date hh:mm:ss.ms)': 'DateTime'})
df_obelix['DateTime'] = pd.to_datetime(df_obelix['DateTime'])

# Application d'un format d'affichage pour datetime pour enlever l'indice du fuseau horaire
df_obelix['DateTime'] = df_obelix['DateTime'].dt.strftime("%Y-%m-%d %H:%M:%S")

# Combinaison du df Strava et sonometre
df_c1 = pd.merge(df, df_obelix, on='DateTime', how='inner')


print(df_c,df_c1)

df_c.to_csv(r'C:\Users\labot\Downloads\velo1_sonometre_2024-04-02.csv', index=False, sep=',')
df_c1.to_csv(r'C:\Users\labot\Downloads\velo2_sonometre_2024-04-02.csv', index=False, sep=',')