from pyspark.sql import SparkSession
import os
import shutil

BASE_PATH = "/bronze_data/temp/"

def list_dates(base_path):
    all_items = os.listdir(base_path)
    dates = [d for d in all_items if os.path.isdir(os.path.join(base_path, d))]
    dates.sort()
    return dates

def get_parquet_path(date_str):
    return os.path.join(BASE_PATH, date_str)

def feeder():
    dates = list_dates(BASE_PATH)
    if not dates:
        print("Aucun dossier trouvé dans", BASE_PATH)
        return

    spark = SparkSession.builder \
        .appName("Feeder") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .getOrCreate()

    print(f"Étape 1 : {dates[0]} (aucune fusion, données inchangées)")

    for i in range(1, len(dates)):
        prev_path = get_parquet_path(dates[i - 1])
        curr_path = get_parquet_path(dates[i])
        temp_path = curr_path + "_tmp"

        print(f"\nÉtape {i+1} : fusion de {prev_path} + {curr_path} → {curr_path}")

        df_prev = spark.read.parquet(prev_path)
        df_curr = spark.read.parquet(curr_path)

        # Fusion optimisée
        df_merged = df_prev.unionByName(df_curr).coalesce(10)

        # Écriture dans un dossier temporaire
        df_merged.write.mode("overwrite").parquet(temp_path)

        # Remplacement sécurisé
        if os.path.exists(curr_path):
            shutil.rmtree(curr_path)
        os.rename(temp_path, curr_path)

        print(f"{curr_path} mis à jour avec succès.")

    spark.stop()

if __name__ == "__main__":
    feeder()