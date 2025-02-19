import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

class DataVisualizer:
    def __init__(self, file_path):
        self.df = pd.read_parquet(file_path, engine="pyarrow")
        self.df["DateTime_x"] = pd.to_datetime(self.df["DateTime_x"])
        self.df_hourly = self._prepare_hourly_data()
    
    def _prepare_hourly_data(self):
        df_hourly = self.df.resample("h", on="DateTime_x").agg({
            "Leq_dBA": ["mean", "min", "max"],
            "lux_S1": ["mean", "min", "max"],
            "Temp (°C)": "mean",
            "Hum. rel (%)": "mean",
            "Vit. du vent (km/h)": "mean",
            "Pression à la station (kPa)": "mean",
            "Hauteur de précip. (mm)": "mean"
        })
        df_hourly.columns = ["Leq_mean", "Leq_min", "Leq_max", "Lux_mean", "Lux_min", "Lux_max", 
                             "Temp_mean", "Humidity_mean", "Wind_mean", "Pressure_mean", "Precipitations"]
        return df_hourly.reset_index()
    
    def plot_data(self, start_date, days=7, hours=range(24), elements=["Leq", "Lux"]):
        if elements is None:
            elements = ["Leq", "Lux", "Temp", "Humidity", "Wind", "Pressure", "Precipitations"]
        
        end_date = start_date + pd.Timedelta(days=days)
        df_filtered = self.df_hourly[(self.df_hourly["DateTime_x"] >= start_date) &
                                     (self.df_hourly["DateTime_x"] < end_date) &
                                     (self.df_hourly["DateTime_x"].dt.hour.isin(hours))]
        
        fig, axs = plt.subplots(len(elements), 1, figsize=(12, 2 * len(elements)), sharex=True)
        if len(elements) == 1:
            axs = [axs]  # Pour éviter des erreurs avec un seul graphique
        
        for ax, element in zip(axs, elements):
            column_mean = f"{element}_mean"
            if column_mean in df_filtered.columns:
                ax.plot(df_filtered["DateTime_x"], df_filtered[column_mean], label=element, linestyle="-", linewidth=1.5)
                ax.set_ylabel(element)
                ax.legend()
                ax.grid(True)
            
                if element in ["Leq", "Lux"]:
                    column_min = f"{element}_min"
                    column_max = f"{element}_max"
                    ax.fill_between(df_filtered["DateTime_x"], df_filtered[column_min], df_filtered[column_max], alpha=0.2)
        
        axs[-1].set_xlabel("Date")
        axs[-1].xaxis.set_major_locator(mdates.DayLocator())
        axs[-1].xaxis.set_minor_locator(mdates.HourLocator(interval=1))
        axs[-1].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.setp(axs[-1].xaxis.get_majorticklabels(), rotation=45)
        
        plt.tight_layout()
        plt.show()#(block=False)  # Afficher sans bloquer

# Exemple d'utilisation:
visualizer = DataVisualizer("data.parquet")
visualizer.plot_data(start_date=pd.Timestamp("2024-01-01"), days=7, hours=range(23, 4), elements=["Leq", "Lux", "Temp"])
visualizer.plot_data(start_date=pd.Timestamp("2023-12-01"), days=7, hours=range(23, 4), elements=["Leq", "Lux", "Temp"])