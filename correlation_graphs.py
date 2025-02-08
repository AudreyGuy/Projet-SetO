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
    "lux_S1": ["mean", "min", "max"],
    "Temp (°C)": "mean",
    "Hum. rel (%)": "mean",
    "Vit. du vent (km/h)": "mean",
    "Pression à la station (kPa)": "mean",
    "Hauteur de précip. (mm)" : "mean"
})
df_first_7_days_hourly.columns = ["Leq_mean", "Leq_min", "Leq_max", "Lux_mean", "Lux_min", "Lux_max", 
                                  "Temp_mean", "Humidity_mean", "Wind_mean", "Pressure_mean", "precipitations"]
df_first_7_days_hourly = df_first_7_days_hourly.reset_index()

# Regrouper par heure pour avoir une seule valeur par heure (moyenne, min, max)
df_hourly = df.resample("h", on="DateTime_x").agg({"Leq_dBA": ["mean", "min", "max"], "lux_S1": ["mean", "min", "max"],
                                                   "Temp (°C)": "mean", "Hum. rel (%)": "mean", "Vit. du vent (km/h)": "mean",
                                                   "Pression à la station (kPa)": "mean", "Hauteur de précip. (mm)" : "mean"})
df_hourly.columns = ["Leq_mean", "Leq_min", "Leq_max", "Lux_mean", "Lux_min", "Lux_max", 
                     "Temp_mean", "Humidity_mean", "Wind_mean", "Pressure_mean", "precipitations"]
df_hourly = df_hourly.reset_index()

# **1. Graphiques pour les 7 premiers jours**

# Créer la figure principale pour les 7 premiers jours
fig, axs = plt.subplots(5, 1, figsize=(12, 12), sharex=True)
fig.suptitle("Analyse pour les 7 premiers jours", fontsize=16)

# Ajout du fond gris pour les fins de semaine
for i in range(len(df_first_7_days_hourly) - 1):
    start = df_first_7_days_hourly["DateTime_x"].iloc[i]
    end = df_first_7_days_hourly["DateTime_x"].iloc[i + 1]
    if start.weekday() >= 5:  # 5 = Samedi, 6 = Dimanche
        for ax in axs:
            ax.axvspan(start, end, color="lightgray", alpha=0.5)

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

# Tracer la température (°C)
axs[2].plot(df_first_7_days_hourly["DateTime_x"], df_first_7_days_hourly["Temp_mean"], linestyle="-", color="red", label="Température (°C)")
axs[2].set_ylabel("Température (°C)")
axs[2].legend()
axs[2].grid(True)

# Tracer l'humidité relative (%)
ax4 = axs[3]
ax4.plot(df_first_7_days_hourly["DateTime_x"], df_first_7_days_hourly["Humidity_mean"], linestyle="-", color="green", label="Humidité (%)")
ax4.set_ylabel("Humidité (%)")
ax4.legend(loc="upper left")
ax4.grid(True)

# Ajouter un axe secondaire pour les précipitations
ax5 = ax4.twinx()
ax5.plot(df_first_7_days_hourly["DateTime_x"], df_first_7_days_hourly["precipitations"], linestyle="-", color="cyan", label="Précipitations (mm)")
ax5.set_ylabel("Précipitations (mm)")
ax5.legend(loc="upper right")

# Tracer la vitesse du vent (km/h) et la pression atmosphérique (kPa)
ax6 = axs[4]  # Dernier subplot
ax6.plot(df_first_7_days_hourly["DateTime_x"], df_first_7_days_hourly["Wind_mean"], linestyle="-", color="purple", label="Vent (km/h)")
ax6.set_ylabel("Vent (km/h)")
ax6.legend(loc="upper left")
ax6.grid(True)

# Ajouter la pression atmosphérique sur le même axe mais avec une deuxième échelle Y
ax7 = ax6.twinx()
ax7.plot(df_first_7_days_hourly["DateTime_x"], df_first_7_days_hourly["Pressure_mean"], linestyle="-", color="brown", label="Pression (kPa)")
ax7.set_ylabel("Pression (kPa)")
ax7.legend(loc="upper right")

# Format des dates sur l'axe X
axs[4].set_xlabel("Date")
axs[4].xaxis.set_major_locator(mdates.DayLocator())  # Un tick par jour
axs[4].xaxis.set_minor_locator(mdates.HourLocator(interval=1))  # Un tick par heure
axs[4].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))  # Format date-heure
for ax in axs:
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

plt.tight_layout()

# **2. Graphiques pour chaque mois (novembre, décembre, janvier)**

months = ["2023-11", "2023-12", "2024-01"]
for month in months:
    # Extraire les données du mois spécifique
    df_month = df_hourly[df_hourly["DateTime_x"].dt.strftime('%Y-%m') == month]
    
    # Créer une figure pour chaque mois
    fig, axs = plt.subplots(5, 1, figsize=(12, 12), sharex=True)
    fig.suptitle(f"Analyse pour {month}", fontsize=16)

    # Ajout du fond gris pour les fins de semaine
    for i in range(len(df_month) - 1):
        start = df_month["DateTime_x"].iloc[i]
        end = df_month["DateTime_x"].iloc[i + 1]
        if start.weekday() >= 5:  # 5 = Samedi, 6 = Dimanche
            for ax in axs:
                ax.axvspan(start, end, color="lightgray", alpha=0.5)

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

    # Tracer la température (°C)
    axs[2].plot(df_month["DateTime_x"], df_month["Temp_mean"], linestyle="-", color="red", label="Température (°C)")
    axs[2].set_ylabel("Température (°C)")
    axs[2].legend()
    axs[2].grid(True)

    # Tracer l'humidité relative (%)
    ax4 = axs[3]
    ax4.plot(df_month["DateTime_x"], df_month["Humidity_mean"], linestyle="-", color="green", label="Humidité (%)")
    ax4.set_ylabel("Humidité (%)")
    ax4.legend(loc="upper left")
    ax4.grid(True)

    # Ajouter un axe secondaire pour les précipitations
    ax5 = ax4.twinx()
    ax5.plot(df_month["DateTime_x"], df_month["precipitations"], linestyle="-", color="cyan", label="Précipitations (mm)")
    ax5.set_ylabel("Précipitations (mm)")
    ax5.legend(loc="upper right")

    # Tracer la vitesse du vent (km/h) et la pression atmosphérique (kPa)
    ax6 = axs[4]  # Dernier subplot
    ax6.plot(df_month["DateTime_x"], df_month["Wind_mean"], linestyle="-", color="purple", label="Vent (km/h)")
    ax6.set_ylabel("Vent (km/h)")
    ax6.legend(loc="upper left")
    ax6.grid(True)

    # Ajouter la pression atmosphérique sur le même axe mais avec une deuxième échelle Y
    ax7 = ax6.twinx()
    ax7.plot(df_month["DateTime_x"], df_month["Pressure_mean"], linestyle="-", color="brown", label="Pression (kPa)")
    ax7.set_ylabel("Pression (kPa)")
    ax7.legend(loc="upper right")

    # Format des dates sur l'axe X
    axs[4].set_xlabel("Date")
    axs[4].xaxis.set_major_locator(mdates.DayLocator())  # Un tick par jour
    axs[4].xaxis.set_minor_locator(mdates.HourLocator(interval=1))  # Un tick par heure
    axs[4].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))  # Format date
    for ax in axs:
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

    plt.tight_layout()

plt.show()