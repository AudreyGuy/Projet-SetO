import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error

class DataAnalyzer:
    def __init__(self, file_path):
        self.df = pd.read_parquet(file_path, engine="pyarrow")
        self.df["DateTime_x"] = pd.to_datetime(self.df["DateTime_x"])
        self.df["Weekday"] = self.df["DateTime_x"].dt.weekday  # 0 = Lundi, 6 = Dimanche
        self.df["Hour"] = self.df["DateTime_x"].dt.hour
        self.df["IsWeekend"] = self.df["Weekday"] >= 5
    
    def analyze_correlation_leq_lux_at_noon(self):
        # Filtrer les données pour ne garder que celles à 12h
        df_midday = self.df[self.df["DateTime_x"].dt.hour == 12]

        # Grouper par jour et prendre la moyenne pour chaque jour
        df_midday = df_midday.groupby(df_midday["DateTime_x"].dt.date).mean(numeric_only=True)

        # Réinitialiser l'index et renommer la colonne de date
        df_midday = df_midday.reset_index().rename(columns={"index": "Date"})

        # Calculer la corrélation entre Leq et Lux
        correlation = df_midday["Leq_dBA"].corr(df_midday["lux_S1"])

        print(f"Corrélation entre Leq et Lux à midi : {correlation:.3f}")
        
        # Séparer semaine et week-end
        df_week = df_midday[df_midday["Weekday"] < 5]
        df_weekend = df_midday[df_midday["Weekday"] >= 5]

        correlation_semaine = df_week[["lux_S1", "Leq_dBA"]].corr().iloc[0, 1]
        correlation_weekend = df_weekend[["lux_S1", "Leq_dBA"]].corr().iloc[0, 1]

        print("Corrélation semaine:", correlation_semaine)
        print("Corrélation week-end:", correlation_weekend)
    
        # Tracer le nuage de points
        plt.figure(figsize=(10, 5))
        sns.regplot(x=df_week["lux_S1"], y=df_week["Leq_dBA"], label="Semaine", color='blue')
        sns.regplot(x=df_weekend["lux_S1"], y=df_weekend["Leq_dBA"], label="Week-end", color='red')
        # Ajouter la valeur de corrélation sur le graphique
        plt.text(0.05, 0.95, f"Corrélation globale: {correlation:.3f}\nCorrélation semaine: {correlation_semaine:.3f}\nCorrélation week-end: {correlation_weekend:.3f}", transform=plt.gca().transAxes, fontsize=12, verticalalignment='top')

        plt.xlabel("Lumière (lux)")
        plt.ylabel("Bruit (Leq dBA)")
        plt.title("Corrélation entre le bruit et la lumière à midi")
        plt.legend()
        plt.xscale("log")
        plt.grid(True, linestyle="--", linewidth=0.5)
        plt.show()

    def analyze_correlation_leq_lux_all_hours(self):
        # Extraire l'heure et le jour de la semaine
        self.df["Hour"] = self.df["DateTime_x"].dt.hour
        self.df["Weekday"] = self.df["DateTime_x"].dt.weekday  # Lundi = 0, Dimanche = 6

        # Grouper par jour et heure, puis prendre la moyenne
        df_hourly = self.df.groupby([self.df["DateTime_x"].dt.date, "Hour"]).mean(numeric_only=True).reset_index()

        # Calculer la corrélation pour chaque heure
        correlations = df_hourly.groupby("Hour")[["lux_S1", "Leq_dBA"]].corr().iloc[0::2, -1].reset_index()
        correlations = correlations.rename(columns={"Leq_dBA": "Correlation"}).drop(columns=["level_1"])

        # Séparer les jours de semaine et les week-ends
        df_hourly["Weekday"] = pd.to_datetime(df_hourly["DateTime_x"]).dt.weekday
        df_week = df_hourly[df_hourly["Weekday"] < 5]
        df_weekend = df_hourly[df_hourly["Weekday"] >= 5]

        # Calculer la corrélation pour semaine et week-end
        correlation_week = df_week.groupby("Hour")[["lux_S1", "Leq_dBA"]].corr().iloc[0::2, -1].reset_index()
        correlation_weekend = df_weekend.groupby("Hour")[["lux_S1", "Leq_dBA"]].corr().iloc[0::2, -1].reset_index()

        print("Corrélation semaine par heure:")
        print(correlation_week)

        print("Corrélation week-end par heure:")
        print(correlation_weekend)

        # Tracer la corrélation en fonction de l'heure
        plt.figure(figsize=(10, 5))
        plt.plot(correlations["Hour"], correlations["Correlation"], label="Tous les jours", color="black")
        plt.plot(correlation_week["Hour"], correlation_week["Leq_dBA"], label="Semaine", color="blue")
        plt.plot(correlation_weekend["Hour"], correlation_weekend["Leq_dBA"], label="Week-end", color="red")
        
        plt.xlabel("Heure de la journée")
        plt.ylabel("Corrélation Lux - Leq")
        plt.title("Corrélation entre le bruit et la lumière par heure")
        plt.legend()
        plt.grid(True, linestyle="--", linewidth=0.5)
        plt.xticks(np.arange(0, 24, 1))  # Ticks de 0 à 23, un tick par heure
        plt.show()
    
    def analyze_weather_impact_on_sound(self):
        features = ["Temp (°C)", "Hum. rel (%)", "Pression à la station (kPa)", "Hauteur de précip. (mm)"]
        df_ml = self.df.dropna(subset=["Leq_dBA"] + features)  # Retirer les valeurs manquantes
        
        X = df_ml[features]
        y = df_ml["Leq_dBA"]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        
        y_pred = model.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        print(f"Erreur absolue moyenne du modèle : {mae:.2f} dBA")
        
        importances = model.feature_importances_
        feature_importance = pd.DataFrame({'Feature': features, 'Importance': importances})
        feature_importance = feature_importance.sort_values(by='Importance', ascending=False)
        
        plt.figure(figsize=(8, 5))
        sns.barplot(data=feature_importance, x="Importance", y="Feature")
        plt.xlabel("Importance du paramètre")
        plt.ylabel("Paramètre météo")
        plt.title("Impact des paramètres météo sur le bruit")
        # Ajouter les valeurs sur les barres
        for index, value in enumerate(feature_importance["Importance"]):
            plt.text(value, index, f"{value:.2f}", va='center')
        plt.show()

    def analyze_weather_impact_on_light(self):
        features = ["Temp (°C)", "Hum. rel (%)", "Pression à la station (kPa)", "Hauteur de précip. (mm)"]
        df_ml = self.df.dropna(subset=["lux_S1"] + features)  # Retirer les valeurs manquantes
        
        X = df_ml[features]
        y = df_ml["lux_S1"]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        
        y_pred = model.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        print(f"Erreur absolue moyenne du modèle : {mae:.2f} lux")
        
        importances = model.feature_importances_
        feature_importance = pd.DataFrame({'Feature': features, 'Importance': importances})
        feature_importance = feature_importance.sort_values(by='Importance', ascending=False)
        
        plt.figure(figsize=(8, 5))
        sns.barplot(data=feature_importance, x="Importance", y="Feature")
        plt.xlabel("Importance du paramètre")
        plt.ylabel("Paramètre météo")
        plt.title("Impact des paramètres météo sur la lumière ambiante")

        # Ajouter les valeurs sur les barres
        for index, value in enumerate(feature_importance["Importance"]):
            plt.text(value, index, f"{value:.2f}", va='center')

        plt.show()
    
    def detect_peak_hours(self):
        df_hourly = self.df.groupby("Hour")["Leq_dBA"].mean()
        plt.figure(figsize=(8, 5))
        plt.plot(df_hourly.index, df_hourly.values, marker="o", linestyle="-", label="Moyenne horaire du bruit")
        plt.axvspan(6, 9, color="green", alpha=0.3, label="Heures de pointe matinales")
        plt.axvspan(15, 18, color="red", alpha=0.3, label="Heures de pointe soir")
        plt.xlabel("Heure de la journée")
        plt.ylabel("Bruit (Leq dBA)")
        plt.title("Effet des heures de pointe sur le niveau de bruit")
        plt.legend()
        plt.grid(True, linestyle="--", linewidth=0.5)
        plt.xticks(np.arange(0, 24, 1))  # Ticks de 0 à 23, un tick par heure
        plt.show()
    
# Exemple d'utilisation:
analyzer = DataAnalyzer("data.parquet")
analyzer.analyze_correlation_leq_lux_at_noon()
analyzer.analyze_correlation_leq_lux_all_hours()
analyzer.detect_peak_hours()
analyzer.analyze_weather_impact_on_sound()
analyzer.analyze_weather_impact_on_light()