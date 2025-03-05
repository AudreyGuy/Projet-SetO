import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

class DataVisualizer:
    def __init__(self, file_path):
        self.df = pd.read_parquet(file_path, engine="pyarrow")
        self.df["DateTime_x"] = pd.to_datetime(self.df["DateTime_x"])
        
        # 🔹 Conversion en pression acoustique (Pa)
        self.df["Leq_Pa"] = 20e-6 * 10**(self.df["Leq_dBA"] / 20)

        # 🔹 Création des DataFrames résumés
        self.df_hourly = self._prepare_hourly_data()
        self.df_minutes = self._prepare_minutes_data()
    
    def _prepare_hourly_data(self):
        df_hourly = self.df.resample("h", on="DateTime_x").agg({
            "Leq_Pa": ["mean", "min", "max"],
            "lux_S1": ["mean", "min", "max"],
            "Temp (°C)": "mean",
            "Hum. rel (%)": "mean",
            "Vit. du vent (km/h)": "mean",
            "Pression à la station (kPa)": "mean",
            "Hauteur de précip. (mm)": "mean"
        })

        # 🔹 Conversion inverse en dB
        df_hourly["Leq_mean"] = 20 * np.log10(df_hourly["Leq_Pa"]["mean"] / 20e-6)
        df_hourly["Leq_min"] = 20 * np.log10(df_hourly["Leq_Pa"]["min"] / 20e-6)
        df_hourly["Leq_max"] = 20 * np.log10(df_hourly["Leq_Pa"]["max"] / 20e-6)

        df_hourly.drop(columns=["Leq_Pa"], inplace=True)
        
        df_hourly.columns = ["Lux_mean", "Lux_min", "Lux_max", "Temp_mean", "Humidity_mean", "Wind_mean", "Pressure_mean", "Precipitations_mean","Leq_mean", "Leq_min", "Leq_max"]
        
        return df_hourly.reset_index()

    def _prepare_minutes_data(self):
        df_minutes = self.df.resample("min", on="DateTime_x").agg({
            "Leq_Pa": ["mean", "min", "max"],
            "lux_S1": ["mean", "min", "max"],
            "Temp (°C)": "mean",
            "Hum. rel (%)": "mean",
            "Vit. du vent (km/h)": "mean",
            "Pression à la station (kPa)": "mean",
            "Hauteur de précip. (mm)": "mean"
        })

        # 🔹 Conversion inverse en dB
        df_minutes["Leq_mean"] = 20 * np.log10(df_minutes["Leq_Pa"]["mean"] / 20e-6)
        df_minutes["Leq_min"] = 20 * np.log10(df_minutes["Leq_Pa"]["min"] / 20e-6)
        df_minutes["Leq_max"] = 20 * np.log10(df_minutes["Leq_Pa"]["max"] / 20e-6)

        df_minutes.drop(columns=["Leq_Pa"], inplace=True)

        df_minutes.columns = ["Lux_mean", "Lux_min", "Lux_max", "Temp_mean", "Humidity_mean", "Wind_mean", "Pressure_mean", "Precipitations_mean","Leq_mean", "Leq_min", "Leq_max"]

        return df_minutes.reset_index()
    
    def plot_data_by_days(self, start_date, days=7, elements=None, Pa=False):
        end_date = start_date + pd.Timedelta(days=days)
        self._plot_time_series(start_date, end_date, elements, None, None, Pa)
    
    def plot_data_by_months(self, start_date, months=1, elements=None, Pa=False):
        end_date = start_date + pd.DateOffset(months=months)
        self._plot_time_series(start_date, end_date, elements, None, None, Pa)
    
    def plot_data_by_hour_range(self, start_date, end_date, hours, elements=None, Pa=False):
        if end_date is None:
            end_date = start_date
        
        if end_date != start_date:
            df_filtered = self.df_minutes[(self.df_minutes["DateTime_x"] >= start_date) &
                                     (self.df_minutes["DateTime_x"] <= end_date + pd.Timedelta(days=1)) &
                                     (self.df_minutes["DateTime_x"].dt.hour.isin(hours))]
            days = df_filtered["DateTime_x"].dt.date.unique()
            for day in days:
                df_day = df_filtered[(df_filtered["DateTime_x"].dt.date >= day) & (df_filtered["DateTime_x"].dt.date <= day + pd.Timedelta(days=1))]
                self._plot_time_series(pd.Timestamp(day), None, elements, df_day, hours, Pa)
        else:
            df_filtered = self.df_minutes[(self.df_minutes["DateTime_x"] >= start_date) &
                                     (self.df_minutes["DateTime_x"] <= end_date + pd.Timedelta(days=1) + pd.Timedelta(hours=23) + pd.Timedelta(minutes=59)) &
                                     (self.df_minutes["DateTime_x"].dt.hour.isin(hours))]
            df_day = df_filtered[(df_filtered["DateTime_x"].dt.date >= start_date.date()) & (df_filtered["DateTime_x"].dt.date <= start_date.date() + pd.Timedelta(days=1))]
            self._plot_time_series(start_date, None, elements, df_day, hours, Pa)
    
    def _plot_time_series(self, start_date, end_date, elements=["Leq", "Lux"], df_filtered=None, hours=None, Pa=False):
        colors = {"Leq": "blue", "Lux": "orange", "Temp": "red", "Humidity": "green", 
                  "Precipitations": "cyan", "Wind": "purple", "Pressure": "brown"}
        
        if elements is None:
            elements = ["Leq", "Lux", "Temp", "Humidity", "Wind", "Pressure", "Precipitations"]

        fig, axs = plt.subplots(len(elements), 1, figsize=(12, 2 * len(elements)), sharex=True)

        if end_date is None:
            # 🔹 Vérification si la plage horaire dépasse minuit
            if hours and hours[0] > hours[-1]:  
                end_date = start_date + pd.Timedelta(days=1)
            else:
                end_date = start_date
            fig.suptitle(f'Analyse du {start_date}', fontsize=12)
        else:
            fig.suptitle(f'Analyse du {start_date} au {end_date}', fontsize=12)

        if df_filtered is None:
            df_filtered = self.df_hourly[(self.df_hourly["DateTime_x"] >= start_date) &
                                         (self.df_hourly["DateTime_x"] < end_date)]

        if len(elements) == 1:
            axs = [axs]

        # Ajout du fond gris pour les fins de semaine
        for i in range(len(df_filtered) - 1):
            start = df_filtered["DateTime_x"].iloc[i]
            end = df_filtered["DateTime_x"].iloc[i + 1]
            if start.weekday() >= 5:  # 5 = Samedi, 6 = Dimanche
                for ax in axs:
                    ax.axvspan(start, end, color="lightgray", alpha=0.5)
        
        for ax, element in zip(axs, elements):
            column_mean = f"{element}_mean"
            if column_mean in df_filtered.columns:
                ax.plot(df_filtered["DateTime_x"], df_filtered[column_mean], label=element, 
                        color=colors.get(element, "black"), linestyle="-", linewidth=1.5)
                if element == "Leq" and Pa:
                    ax.set_ylabel("Pression acoustique (Pa)")
                else:
                    ax.set_ylabel(element+" (dBA)")
                ax.legend()
                ax.grid(True, linestyle="--", linewidth=0.5)
                
                if element in ["Leq", "Lux"]:
                    column_min = f"{element}_min"
                    column_max = f"{element}_max"
                    ax.fill_between(df_filtered["DateTime_x"], df_filtered[column_min], df_filtered[column_max],
                                    color=colors.get(element, "black"), alpha=0.2)
                
                if element == "Lux":
                    ax.set_yscale("log")
        
        axs[-1].set_xlabel("Date et Heure")

        # Ajuster les limites de l'axe des x pour correspondre à la plage d'heures spécifiée
        if hours:
            # Ajuster les limites avec la date de début et fin en ajoutant les heures
            start_time = pd.Timestamp(start_date.date()).replace(hour=hours[0])
            end_time = pd.Timestamp(end_date.date()).replace(hour=hours[-1], minute=59)
            axs[-1].set_xlim([start_time, end_time])
        else:
            axs[-1].xaxis.set_major_locator(mdates.DayLocator())
            axs[-1].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

        plt.setp(axs[-1].xaxis.get_majorticklabels(), rotation=45, ha="right", fontsize=8)
        plt.tight_layout()
        plt.show()

# Exemple d'utilisation:
visualizer = DataVisualizer("data.parquet")
visualizer.plot_data_by_days(start_date=pd.Timestamp("2024-01-02"), days=4, elements=["Leq", "Lux"], Pa=False)
visualizer.plot_data_by_months(start_date=pd.Timestamp("2023-12-01"), months=1, elements=["Leq", "Lux"], Pa=False)
visualizer.plot_data_by_hour_range(start_date=pd.Timestamp("2024-01-01"), end_date=None, hours=[23,0,1,2,3], elements=["Leq", "Lux", "Temp", "Humidity", "Wind", "Pressure", "Precipitations"], Pa=False)
visualizer.plot_data_by_hour_range(start_date=pd.Timestamp("2024-02-01"), end_date=pd.Timestamp("2024-02-02"), hours=[5,6,7], elements=["Leq", "Lux"], Pa=False)