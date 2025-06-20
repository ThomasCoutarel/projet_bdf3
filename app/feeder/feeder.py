from pyspark.sql import SparkSession
import os

# Configuration des chemins
class Config:
    # Chemin local pour Temp (données du jour uniquement)
    TEMP_PATH = "/app/bronze_data/temp"
    # Chemin HDFS pour Bronze (données cumulées)
    BRONZE_ROOT = "hdfs://namenode:8020/lakehouse/bronze"

    @staticmethod
    def get_temp_dates():
        """
        Liste les dates disponibles dans TEMP_PATH
        """
        try:
            return sorted(os.listdir(Config.TEMP_PATH))
        except FileNotFoundError:
            print(f"⚠️ Dossier temp non trouvé : {Config.TEMP_PATH}")
            return []

    @staticmethod
    def get_last_bronze_date():
        """
        Liste les dates disponibles dans Bronze (HDFS)
        """
        # Ici on va utiliser spark.read pour vérifier la présence car os.listdir ne fonctionne pas sur HDFS localement
        # Donc on listera via Spark (plus fiable)
        from py4j.protocol import Py4JJavaError
        try:
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
            path = spark._jvm.org.apache.hadoop.fs.Path(Config.BRONZE_ROOT)
            if not fs.exists(path):
                return None
            statuses = fs.listStatus(path)
            # Extraire les noms des dossiers (dates)
            dates = [status.getPath().getName() for status in statuses if status.isDirectory()]
            return sorted(dates)[-1] if dates else None
        except Py4JJavaError as e:
            print(f"Erreur en listant les dossiers bronze HDFS : {e}")
            return None

    @staticmethod
    def get_parquet_path(base, date_str):
        """
        Construit le chemin parquet pour une date donnée
        """
        return os.path.join(base, date_str, "parquet")


# Initialisation Spark
spark = SparkSession.builder \
    .appName("Feeder Optimisé Incrémental") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

print("🚀 Spark prêt")

# Étape 1 : identifier la dernière date Bronze cumulée
last_bronze_date = Config.get_last_bronze_date()
print(f"📦 Dernier bronze trouvé : {last_bronze_date}")

# Étape 2 : lister les dates Temp disponibles (local)
all_temp_dates = Config.get_temp_dates()
print(f"📅 Dates temp disponibles : {all_temp_dates}")

# Étape 3 : déterminer les dates à traiter (supérieures à la dernière date Bronze)
dates_to_process = [d for d in all_temp_dates if (last_bronze_date is None or d > last_bronze_date)]
print(f"🆕 Nouvelles dates à traiter : {dates_to_process}")

# Étape 4 : boucle de traitement incrémental
for new_date in dates_to_process:
    print(f"\n⏳ Traitement du jour : {new_date}")

    path_today = Config.get_parquet_path(Config.TEMP_PATH, new_date)
    path_today = f"file://{path_today}"  # préciser protocole local

    # Lecture Temp locale
    df_today = spark.read.parquet(path_today)
    print(f"📁 {df_today.count()} lignes lues depuis {path_today}")

    if last_bronze_date:
        path_bronze = Config.get_parquet_path(Config.BRONZE_ROOT, last_bronze_date)
        path_bronze = f"{path_bronze}/parquet" if not path_bronze.endswith("parquet") else path_bronze
        try:
            # Lecture Bronze cumulée précédente depuis HDFS
            df_prev = spark.read.parquet(path_bronze)
            print(f"📁 {df_prev.count()} lignes dans le bronze précédent : {last_bronze_date}")
            # Union incrémentale
            accumulated = df_prev.union(df_today)
        except Exception as e:
            print(f"⚠️ Bronze précédent non trouvé ou erreur lecture : {e}")
            print("🆕 Initialisation Bronze avec la date courante uniquement.")
            accumulated = df_today
    else:
        # Premier traitement, on initialise le Bronze avec les données du jour
        accumulated = df_today

    # Répartition (optionnelle, selon colonne "State")
    if "State" in accumulated.columns:
        accumulated = accumulated.repartition(10, "State")
    else:
        accumulated = accumulated.repartition(10)

    # Écriture Bronze cumulée dans HDFS
    new_bronze_path = Config.get_parquet_path(Config.BRONZE_ROOT, new_date)
    print(f"💾 Sauvegarde Bronze dans HDFS : {new_bronze_path}")
    accumulated.coalesce(1).write.mode("overwrite").parquet(new_bronze_path)

    print(f"✅ Bronze mis à jour pour {new_date} : {accumulated.count()} lignes")

    # Mise à jour de la dernière date Bronze
    last_bronze_date = new_date


spark.stop()
print("🔥 Traitement terminé")
