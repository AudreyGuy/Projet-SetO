import pandas as pd
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 1000)

# Specify the path to your CSV file
file_path = 'C:\\Users\\labot\\Downloads\\LAN3-2023-11-16.csv'

try:
    # Attempt to read the CSV file, handling parsing errors
    df = pd.read_csv(file_path, on_bad_lines='skip')

except pd.errors.ParserError as e:
    print("ParserError:", e)
    # Handle the error as needed, such as logging or displaying an error message

df['Second'] = df['Second'].astype(int)

# converting DateTime
df['DateTime'] = pd.to_datetime(df[['Month', 'Day', 'Year','Hour','Minute','Second']])
df['DateTime'] = pd.to_datetime(df['DateTime'], utc=True)
df['DateTime'] = df['DateTime'].dt.tz_convert("US/Eastern")

# convert to string without offset
df['DateTime'] = df['DateTime'].dt.strftime("%Y-%m-%d %H:%M:%S")

# deleting columns form dataframe
df.drop(['Month','Day','Year','Hour','Minute','Second'], axis=1, inplace=True)

# new dataframe with only S1 values
df_s1= df.loc[df['Sensor'] == 'S1']

#Removing row with ER
df_s1= df_s1[df_s1['Flag'] != 'ER']

#Removing row with N\A
df_s1= df_s1[df_s1['ColorTemperature(k)'] != 'NaN']

# keeping night values
mask = (df_s1['DateTime']>'2023-10-05 19:37:00') & (df_s1['DateTime']<='2024-02-05 23:25:00')
df_s1_n = df_s1.loc[mask]


#lux
df_s1_n_lux = df_s1_n.drop(['ColorTemperature(k)','MSI','Red','Green','Blue','Clear','Flag','Sensor','Latitude','Longitude','Altitude','NumberSatellites','Gain','AcquisitionTime(ms)'], axis=1)

#transformer les valeurs négatives en 0
df_s1_n_lux['lux'] = df_s1_n_lux['lux'].clip(lower=0)

#MSI impact
df_s3= df.loc[df['Sensor'] == 'S3']
df_s3.reset_index(drop=True, inplace=True)
df_s3.drop(['Latitude','Longitude','Altitude','Red','Green','Blue','Clear','DateTime'], axis=1, inplace=True)
nn_s3={'Sensor':'Sensor3', 'MSI':'MSI3','lux':'lux3','ColorTemperature(k)':'ColorTemperature(k)3','Flag':'Flag3'}
df_s3.rename(columns=nn_s3, inplace=True)

df_s5= df.loc[df['Sensor'] == 'S5']
df_s5.reset_index(drop=True, inplace=True)
df_s5.drop(['Red','Green','Blue','Clear'], axis=1, inplace=True)
nn_s5={'Sensor':'Sensor5', 'MSI':'MSI5','lux':'lux5','ColorTemperature(k)':'ColorTemperature(k)5','Flag':'Flag5'}
df_s5.rename(columns=nn_s5, inplace=True)

df_c = pd.concat([df_s3, df_s5], axis=1)

df_c['MSI Impact'] = ((df_c['MSI3'] * df_c['lux3']) + (df_c['MSI5'] * df_c['lux5'])) / 2

df_c = df_c.dropna()

df_c= df_c[df_c['Flag3'] != 'ER']
df_c= df_c[df_c['Flag5'] != 'ER']

# keeping night values
mask2 = (df_c['DateTime']>'2023-10-05 19:37:00') & (df_c['DateTime']<='2024-02-05 23:25:00')
df_c = df_c.loc[mask2]

df_MSII = df_c.drop(['Sensor3','Sensor5','ColorTemperature(k)3','ColorTemperature(k)5','MSI3','MSI5','lux3','lux5','Flag3','Flag5'], axis=1)



df_combined = pd.merge(df_MSII, df_s1_n_lux, on='DateTime', how='inner')

df_combined = df_combined.drop_duplicates(subset=['DateTime'])
#plusieurs lignes avec la meme seconde car arrondie milliseconde


#dataframe du sonomètre

file_path2 = "C:\\Users\\labot\\Downloads\\2023_11_17__01h22m57s.csv"

try:
    # Attempt to read the CSV file, handling parsing errors
    df_sono = pd.read_csv(file_path2, on_bad_lines='skip', delimiter=';')

except pd.errors.ParserError as e:
    print("ParserError:", e)
    # Handle the error as needed, such as logging or displaying an error message

df_sono = df_sono.rename(columns={'Time (Date hh:mm:ss.ms)': 'DateTime'})
df_sono['DateTime'] = pd.to_datetime(df_sono['DateTime'], utc=True)
df_sono['DateTime'] = df_sono['DateTime'].dt.tz_convert("US/Eastern")

# convert to string without offset
df_sono['DateTime'] = df_sono['DateTime'].dt.strftime("%Y-%m-%d %H:%M:%S")


#Changer les virgules en points
df_sono.columns = df_sono.columns.str.strip()
colonnes = ['L-Max dB -A', 'LEQ dB -A', 'L-Min dB -A','L-PeakdB -A']
for col in colonnes:
    df_sono[col] = df_sono[col].str.replace(',', '.')

df_sono['L-Max dB -A'] = df_sono['L-Max dB -A'].astype(float)
df_sono['LEQ dB -A'] = df_sono['LEQ dB -A'].astype(float)
df_sono['L-Min dB -A'] = df_sono['L-Min dB -A'].astype(float)
df_sono['L-PeakdB -A'] = df_sono['L-PeakdB -A'].astype(float)

#Combiner df LAN3 et sono
df_cf = pd.merge(df_combined, df_sono, on='DateTime', how='inner')

df_cf = df_cf.drop_duplicates(subset=['DateTime'])



print(df_cf)

#df_cf.to_csv(r'C:\Users\labot\Downloads\LAN3_Sonomètre_2023-11-16.csv', index=False, sep=',')