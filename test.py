import pandas as pd
from suntime import Sun
import os
from datetime import timedelta
import pytz

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 1000)

#Localisation de Sirene
position = Sun(45.40, -71.88)


#-----------------------------------------------------LAN3-----------------------------------------------------------


# Dossier contenant les fichiers CSV sonomètre sirene
dossier = "C:\\Users\\labot\\Downloads\\sirene_lan3\\"

dff=[]

# Faire une liste avec tous les fichiers dans le dossier
file_list = []
for file in os.listdir(dossier):
    file_list.append(file)

# Boucle pour traiter chaque fichier présent dans le dossier
for file in file_list:
    try:
            # Ouverture du fichier csv sous la forme d'un dataframe et stockage dans la variable 'df'
            df = pd.read_csv(dossier + file, delimiter=',', low_memory=False)

            #Dropper les lignes vides
            df.dropna(inplace=True)

            # deleting columns form dataframe
            df.drop(['NumberSatellites', 'Gain', 'AcquisitionTime(ms)', 'Latitude', 'Longitude', 'Altitude', ], axis=1, inplace=True)

            #Arrondir à la seconde
            df['Second'] = df['Second'].astype(int)

            # modifier le format de l'heure en datetime
            df['DateTime'] = pd.to_datetime(df[['Year', 'Month', 'Day', 'Hour', 'Minute', 'Second']])
            df['DateTime'] = pd.to_datetime(df['DateTime'])


            # enlever les colones double de date
            df.drop(['Year', 'Month', 'Day', 'Hour', 'Minute', 'Second'], axis=1, inplace=True)

            # données de MSI
            df_s3 = df.loc[df['Sensor'] == 'S3']
            df_s3.reset_index(drop=True, inplace=True)
            df_s3.drop(['Red', 'Green', 'Blue', 'Clear', 'DateTime'], axis=1, inplace=True)
            nn_s3 = {'Sensor': 'Sensor3', 'MSI': 'MSI3', 'lux': 'lux3', 'ColorTemperature(k)': 'ColorTemperature(k)3',
                     'Flag': 'Flag3'}
            df_s3.rename(columns=nn_s3, inplace=True)

            df_s5 = df.loc[df['Sensor'] == 'S5']
            df_s5.reset_index(drop=True, inplace=True)
            df_s5.drop(['Red', 'Green', 'Blue', 'Clear'], axis=1, inplace=True)
            nn_s5 = {'Sensor': 'Sensor5', 'MSI': 'MSI5', 'lux': 'lux5', 'ColorTemperature(k)': 'ColorTemperature(k)5',
                     'Flag': 'Flag5'}
            df_s5.rename(columns=nn_s5, inplace=True)

            df_m = pd.concat([df_s3, df_s5], axis=1)

            df_m['MSIImpact'] = ((df_m['MSI3'] * df_m['lux3']) + (df_m['MSI5'] * df_m['lux5'])) / 2

            df_m.drop(['MSI3', 'MSI5', 'lux3', 'lux5'], axis=1, inplace=True)

            # Mesure du lux avec capteur S1
            df_l = df.loc[df['Sensor'] == 'S1']
            df_l.reset_index(drop=True, inplace=True)
            df_l.drop(['Red', 'Green', 'Blue', 'Clear', 'MSI', 'DateTime'], axis=1, inplace=True)
            nn_l = {'Sensor': 'Sensor1', 'ColorTemperature(k)': 'ColorTemperature(k)1',
                    'Flag': 'Flag1'}
            df_l.rename(columns=nn_l, inplace=True)

            # Fusionner les deux dataframe
            df_c = pd.concat([df_m, df_l], axis=1)

            # Enlever les lignes érronées
            df_c = df_c.dropna()

            df_c = df_c[df_c['Flag1'] != 'ER']
            df_c = df_c[df_c['Flag3'] != 'ER']
            df_c = df_c[df_c['Flag5'] != 'ER']

            df_c = df_c[df_c['ColorTemperature(k)1'] != 'NaN']
            df_c = df_c[df_c['ColorTemperature(k)3'] != 'NaN']
            df_c = df_c[df_c['ColorTemperature(k)5'] != 'NaN']

            df_c.drop(['Sensor3', 'ColorTemperature(k)3', 'Flag3', 'Sensor5', 'ColorTemperature(k)5', 'Flag5', 'Sensor1', 'ColorTemperature(k)1', 'Flag1'], axis=1, inplace=True)

            # transformer les valeurs négatives en 0
            df_c['lux'] = df_c['lux'].clip(lower=0)

            # Filtrage des données
            crepuscule_soir = position.get_sunset_time(df_c['DateTime'][1])
            crepuscule_matin = position.get_sunrise_time(df_c['DateTime'][1])
            crepuscule_soir = crepuscule_soir + timedelta(hours=24, minutes=30)
            crepuscule_matin = crepuscule_matin - timedelta(minutes=30)
            crepuscule_soir = crepuscule_soir.strftime("%Y-%m-%d %H:%M:%S")
            crepuscule_matin = crepuscule_matin.strftime("%Y-%m-%d %H:%M:%S")


            #Application d'un mask pour garder que les valeurs de nuits
            mask = (df_c['DateTime'] >= crepuscule_soir) | (df_c['DateTime'] <= crepuscule_matin)
            df_c = df_c.loc[mask]
            df_c['DateTime'] = pd.to_datetime(df_c['DateTime'], utc=True)
            df_c['DateTime'] = df_c['DateTime'].dt.tz_convert("US/Eastern")
            df_c['DateTime'] = df_c['DateTime'].dt.strftime("%Y-%m-%d %H:%M:%S")
            dff.append(df_c)

    except Exception as e:
        # Afficher le nom du fichier en cas d'erreur
        print(f"Erreur lors de la lecture du fichier : {file}")
        print(f"Erreur détaillée : {e}")

merged_df = pd.concat(dff, axis=0)


# Convertir la colonne 'DateTime' en datetime et la définir comme index
merged_df['DateTime'] = pd.to_datetime(merged_df['DateTime'])
merged_df.set_index('DateTime', inplace=True)
merged_df.columns = merged_df.columns.str.strip()

colonnes_a_moyenner = ['MSIImpact', 'lux']

# Resample et moyennage par intervalles de 5 minutes
df_5min_lan3 = merged_df[colonnes_a_moyenner].resample('5min').mean()
df_5min_lan3 = df_5min_lan3.dropna()

# Réinitialiser l'index si nécessaire
df_5min_lan3.reset_index(inplace=True)

print(df_5min_lan3)


#-----------------------------------------------Sonomètre-------------------------------------------------------------

# Dossier contenant les fichiers CSV sonomètre sirene
dossier = "C:\\Users\\labot\\Downloads\\sirene_sono\\"

dff=[]

# Faire une liste avec tous les fichiers dans le dossier
file_list = []
for file in os.listdir(dossier):
    file_list.append(file)

# Boucle pour traiter chaque fichier présent dans le dossier
for file in file_list:
    try:

            # Lecture du fichier CSV
            df = pd.read_csv(dossier + file, delimiter=';')

            #Convertir en DateTime
            df = df.rename(columns={'Time (Date hh:mm:ss.ms)': 'DateTime'})
            df['DateTime'] = pd.to_datetime(df['DateTime'])


            # Filtrage des données
            crepuscule_soir = position.get_sunset_time(df['DateTime'][1])
            crepuscule_matin = position.get_sunrise_time(df['DateTime'][1])
            crepuscule_soir = crepuscule_soir - timedelta(hours=4, minutes=30)
            crepuscule_matin = crepuscule_matin - timedelta(hours=5, minutes=30)
            crepuscule_soir = crepuscule_soir.strftime("%Y-%m-%d %H:%M:%S")
            crepuscule_matin = crepuscule_matin.strftime("%Y-%m-%d %H:%M:%S")


            mask = (df['DateTime'] >= crepuscule_soir) | (df['DateTime'] <= crepuscule_matin)
            df = df.loc[mask]
            df.drop(['Unnamed: 5'], axis=1, inplace=True)
            dff.append(df)

    except Exception as e:
        # Afficher le nom du fichier en cas d'erreur
        print(f"Erreur lors de la lecture du fichier : {file}")
        print(f"Erreur détaillée : {e}")

merged_df = pd.concat(dff, axis=0)



# Convertir la colonne 'DateTime' en datetime et la définir comme index
merged_df['DateTime'] = pd.to_datetime(merged_df['DateTime'])
merged_df.set_index('DateTime', inplace=True)
merged_df.columns = merged_df.columns.str.strip()


colonnes_a_moyenner = ['LEQ dB-A', 'Lmin dB-A', 'Lmax dB-A','Lpeak dB-A']
for col in colonnes_a_moyenner:
    merged_df[col] = merged_df[col].str.replace(',', '.')

merged_df['LEQ dB-A'] = merged_df['LEQ dB-A'].astype(float)
merged_df['Lmin dB-A'] = merged_df['Lmin dB-A'].astype(float)
merged_df['Lmax dB-A'] = merged_df['Lmax dB-A'].astype(float)
merged_df['Lpeak dB-A'] = merged_df['Lpeak dB-A'].astype(float)


# Resample et moyennage par intervalles de 5 minutes
df_5min_sono = merged_df[colonnes_a_moyenner].resample('5min').mean()
df_5min_sono = df_5min_sono.dropna()


# Réinitialiser l'index si nécessaire
df_5min_sono.reset_index(inplace=True)

print(df_5min_sono)

#---------------------------------------------------Lan3 et sonomètre -------------------------------------------------------

#Combiner df LAN3 et sono
df_cf = pd.merge(df_5min_sono, df_5min_lan3, on='DateTime', how='inner')

df_cf = df_cf.drop_duplicates(subset=['DateTime'])

print(df_cf)

df_cf.to_csv(r'C:\Users\labot\Downloads\Sirene_données-filtrées-mean_5min.csv', index=False, sep=',')


