import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.dates as mdates

# Charger les données
file_path = "C://Users//Audrey//Documents//GitHub//Projet-SetO//data.parquet"  # Remplacez par le chemin de votre fichier
df = pd.read_parquet("data.parquet", engine="pyarrow")

# Convertir la colonne DateTime_x en datetime
df["DateTime_x"] = pd.to_datetime(df["DateTime_x"])

# Filtrer les données pour ne garder que les 7 premiers jours (si nécessaire)
df_first_7_days = df[df["DateTime_x"] < df["DateTime_x"].min() + pd.Timedelta(days=7)]

# Regrouper par heure pour avoir une seule valeur par heure (moyenne)
df_first_7_days_hourly = df_first_7_days.resample("h", on="DateTime_x").agg({
    "Leq_dBA": ["mean", "min", "max"],
    "lux_S1": ["mean", "min", "max"]
})
df_first_7_days_hourly.columns = ["Leq_mean", "Leq_min", "Leq_max", "Lux_mean", "Lux_min", "Lux_max"]
df_first_7_days_hourly = df_first_7_days_hourly.reset_index()

# Regrouper par heure pour avoir une seule valeur par heure (moyenne, min, max)
df_hourly = df.resample("h", on="DateTime_x").agg({"Leq_dBA": ["mean", "min", "max"], "lux_S1": ["mean", "min", "max"]})
df_hourly.columns = ["Leq_mean", "Leq_min", "Leq_max", "Lux_mean", "Lux_min", "Lux_max"]
df_hourly = df_hourly.reset_index()

# **1. Graphiques pour les 7 premiers jours**

# Créer la figure principale pour les 7 premiers jours
fig, axs = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
fig.suptitle("Analyse pour les 7 premiers jours", fontsize=16)

# Tracer le son (Leq_dBA)
axs[0].plot(df_first_7_days_hourly["DateTime_x"], df_first_7_days_hourly["Leq_mean"], linestyle="-", color="b", label="Leq (dBA)")
axs[0].fill_between(df_first_7_days_hourly["DateTime_x"], df_first_7_days_hourly["Leq_min"], df_first_7_days_hourly["Leq_max"], color="b", alpha=0.2)
axs[0].set_ylabel("Niveau sonore (dBA)")
axs[0].legend()
axs[0].grid(True)

# Tracer la lumière (lux_S1)
axs[1].plot(df_first_7_days_hourly["DateTime_x"], df_first_7_days_hourly["Lux_mean"], linestyle="-", color="orange", label="Lumière (lux)")
axs[1].fill_between(df_first_7_days_hourly["DateTime_x"], df_first_7_days_hourly["Lux_min"], df_first_7_days_hourly["Lux_max"], color="orange", alpha=0.2)
axs[1].set_ylabel("Éclairement (lux)")
axs[1].legend()
axs[1].grid(True)

# Format des dates sur l'axe X
axs[1].set_xlabel("Date et Heure")
axs[1].xaxis.set_major_locator(mdates.DayLocator())  # Un tick par jour
axs[1].xaxis.set_minor_locator(mdates.HourLocator(interval=1))  # Un tick par heure
axs[1].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))  # Format date-heure
plt.xticks(rotation=45)

plt.tight_layout()

# **2. Graphiques pour chaque mois (novembre, décembre, janvier)**

months = ["2023-11", "2023-12", "2024-01"]
for month in months:
    # Extraire les données du mois spécifique
    df_month = df_hourly[df_hourly["DateTime_x"].dt.strftime('%Y-%m') == month]
    
    # Créer une figure pour chaque mois
    fig, axs = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    fig.suptitle(f"Analyse pour {month}", fontsize=16)

    # Tracer le son (Leq_dBA)
    axs[0].plot(df_month["DateTime_x"], df_month["Leq_mean"], linestyle="-", color="b", label="Leq (dBA)")
    axs[0].fill_between(df_month["DateTime_x"], df_month["Leq_min"], df_month["Leq_max"], color="b", alpha=0.2)
    axs[0].set_ylabel("Niveau sonore (dBA)")
    axs[0].legend()
    axs[0].grid(True)

    # Tracer la lumière (lux_S1)
    axs[1].plot(df_month["DateTime_x"], df_month["Lux_mean"], linestyle="-", color="orange", label="Lumière (lux)")
    axs[1].fill_between(df_month["DateTime_x"], df_month["Lux_min"], df_month["Lux_max"], color="orange", alpha=0.2)
    axs[1].set_ylabel("Éclairement (lux)")
    axs[1].legend()
    axs[1].grid(True)

    # Format des dates sur l'axe X
    axs[1].set_xlabel("Date et Heure")
    axs[1].xaxis.set_major_locator(mdates.DayLocator())  # Un tick par jour
    axs[1].xaxis.set_minor_locator(mdates.HourLocator(interval=1))  # Un tick par heure
    axs[1].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))  # Format date-heure
    plt.xticks(rotation=45)

    plt.tight_layout()

plt.show()