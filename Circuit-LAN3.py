import pandas as pd
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 1000)

# read csv file Jerry
df = pd.read_csv('C:\\Users\labot\Downloads\\2024-02-06.csv')

# deleting columns form dataframe
df.drop(['NumberSatellites','Gain','AcquisitionTime(ms)'], axis=1, inplace=True)

# converting 'Second' from float to int
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
mask = (df_s1['DateTime']>'2024-02-05 19:37:00') & (df_s1['DateTime']<='2024-02-05 23:25:00')
df_s1_n = df_s1.loc[mask]

# deleting duplicates
#df_s1_n = df_s1_n.sort_values('DateTime').drop_duplicates('DateTime', keep='last')

#MSI
#df_s1_n_MSI = df_s1_n.drop(['ColorTemperature(k)','lux','Red','Green','Blue','Clear'], axis=1)

#lux
df_s1_n_lux = df_s1_n.drop(['ColorTemperature(k)','MSI','Red','Green','Blue','Clear','Flag'], axis=1)

#transformer les valeurs négatives en 0
df_s1_n_lux['lux'] = df_s1_n_lux['lux'].clip(lower=0)



#print(df_s1_n_lux)
print(df_s1_n_lux)

# read csv file Tom
df_1 = pd.read_csv('C:\\Users\\labot\\Downloads\\2024-02-05-date-a-Will.csv')

# deleting columns form dataframe
df_1.drop(['NumberSatellites','Gain','AcquisitionTime(ms)'], axis=1, inplace=True)

# converting 'Second' from float to int
df_1['Second'] = df_1['Second'].astype(int)

# converting DateTime
df_1['DateTime'] = pd.to_datetime(df_1[['Month', 'Day', 'Year','Hour','Minute','Second']])
df_1['DateTime'] = pd.to_datetime(df_1['DateTime'], utc=True)
df_1['DateTime'] = df_1['DateTime'].dt.tz_convert("US/Eastern")

# convert to string without offset
df_1['DateTime'] = df_1['DateTime'].dt.strftime("%Y-%m-%d %H:%M:%S")

# deleting columns form dataframe
df_1.drop(['Month','Day','Year','Hour','Minute','Second'], axis=1, inplace=True)

# new dataframe with only S1 values
df_s1_= df_1.loc[df['Sensor'] == 'S1']

#Removing row with ER
df_s1_= df_s1_[df_s1_['Flag'] != 'ER']

#Removing row with N\A
df_s1_= df_s1_[df_s1_['ColorTemperature(k)'] != 'NaN']

# keeping night values
mask1 = (df_s1_['DateTime']>'2024-02-04 19:37:00') & (df_s1_['DateTime']<='2024-02-04 23:25:00')
df_s1_n_ = df_s1_.loc[mask1]

# deleting duplicates
#df_s1_n = df_s1_n.sort_values('DateTime').drop_duplicates('DateTime', keep='last')

#MSI
#df_s1_n_MSI = df_s1_n.drop(['ColorTemperature(k)','lux','Red','Green','Blue','Clear'], axis=1)

#lux
df_s1_n_lux_ = df_s1_n_.drop(['ColorTemperature(k)','MSI','Red','Green','Blue','Clear','Flag'], axis=1)

#print(df_s1_n_)
#print(df_s1_n_MSI)
#print(df_s1_n_lux_)

#Combining Dataframe Tom and Jerry lux
df_Tom_Jerry = pd.concat([df_s1_n_lux, df_s1_n_lux_])

#print(df_Tom_Jerry)

#MSI impact Jerry
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
mask2 = (df_c['DateTime']>'2024-02-05 19:37:00') & (df_c['DateTime']<='2024-02-05 23:25:00')
df_c = df_c.loc[mask2]

df_MSIIj = df_c.drop(['Sensor3','Sensor5','ColorTemperature(k)3','ColorTemperature(k)5','MSI3','MSI5','lux3','lux5','Flag3','Flag5'], axis=1)


#MSI impact Tom
df_s3t= df_1.loc[df_1['Sensor'] == 'S3']
df_s3t.reset_index(drop=True, inplace=True)
df_s3t.drop(['Latitude','Longitude','Altitude','Red','Green','Blue','Clear','DateTime'], axis=1, inplace=True)
nn_s3t={'Sensor':'Sensor3', 'MSI':'MSI3','lux':'lux3','ColorTemperature(k)':'ColorTemperature(k)3','Flag':'Flag3'}
df_s3t.rename(columns=nn_s3t, inplace=True)

df_s5t= df_1.loc[df_1['Sensor'] == 'S5']
df_s5t.reset_index(drop=True, inplace=True)
df_s5t.drop(['Red','Green','Blue','Clear'], axis=1, inplace=True)
nn_s5t={'Sensor':'Sensor5', 'MSI':'MSI5','lux':'lux5','ColorTemperature(k)':'ColorTemperature(k)5','Flag':'Flag5'}
df_s5t.rename(columns=nn_s5t, inplace=True)

df_ct = pd.concat([df_s3t, df_s5t], axis=1)

df_ct['MSI Impact'] = ((df_ct['MSI3'] * df_ct['lux3']) + (df_ct['MSI5'] * df_ct['lux5'])) / 2

df_ct = df_ct.dropna()

df_ct= df_ct[df_ct['Flag3'] != 'ER']
df_ct= df_ct[df_ct['Flag5'] != 'ER']

# keeping night values
mask3 = (df_ct['DateTime']>'2024-02-04 19:37:00') & (df_ct['DateTime']<='2024-02-04 23:25:00')
df_ct = df_ct.loc[mask3]

df_MSIIt = df_ct.drop(['Sensor3','Sensor5','ColorTemperature(k)3','ColorTemperature(k)5','MSI3','MSI5','lux3','lux5','Flag3','Flag5'], axis=1)

df_MSII = pd.concat([df_MSIIt, df_MSIIj], axis=0)

#print(df_MSII)


#df_Tom_Jerry.to_csv(r'C:\Users\labot\Downloads\LAN3_Tom_and_Jerry_lux_2024-02-05.csv', index=False, sep=',')
#df_MSII.to_csv(r'C:\Users\labot\Downloads\LAN3_Tom_and_Jerry_MSI_Impact_2024-02-05.csv', index=False, sep=',')

#df_s1_n.to_csv(r'C:\Users\labot\Downloads\LAN3_Jerry_2024-02-05.csv', index=False, sep=',')
#df_s1_n_MSI.to_csv(r'C:\Users\labot\Downloads\LAN3_Jerry_2024-02-05_MSI.csv', index=False, sep=',')
#df_s1_n_lux.to_csv(r'C:\Users\labot\Downloads\LAN3_Jerry_2024-02-05_lux.csv', index=False, sep=',')

#df_s1_n_.to_csv(r'C:\Users\labot\Downloads\LAN3_Jerry_2024-02-05.csv', index=False, sep=',')
#df_s1_n_MSI.to_csv(r'C:\Users\labot\Downloads\LAN3_Jerry_2024-02-05_MSI.csv', index=False, sep=',')
#df_s1_n_lux_.to_csv(r'C:\Users\labot\Downloads\LAN3_Jerry_2024-02-05_lux.csv', index=False, sep=',')