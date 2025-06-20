from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError
from config import Config

def get_last_bronze_date(spark):
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        path = spark._jvm.org.apache.hadoop.fs.Path(Config.BRONZE_ROOT)
        if not fs.exists(path):
            return None
        statuses = fs.listStatus(path)
        dates = [status.getPath().getName() for status in statuses if status.isDirectory()]
        return sorted(dates)[-1] if dates else None
    except Py4JJavaError as e:
        print(f"Erreur HDFS : {e}")
        return None

def main():
    # ✅ Spark optimisé
    spark = SparkSession.builder \
        .appName("Feeder Incrémental Optimisé") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    print("🚀 Spark prêt")
    last_bronze_date = get_last_bronze_date(spark)
    print(f"📦 Dernier bronze : {last_bronze_date}")
    all_temp_dates = Config.get_temp_dates()
    dates_to_process = [d for d in all_temp_dates if (last_bronze_date is None or d > last_bronze_date)]
    print(f"🆕 Dates à traiter : {dates_to_process}")

    for new_date in dates_to_process:
        print(f"\n⏳ Traitement : {new_date}")
        path_today = f"file://{Config.get_parquet_path(Config.TEMP_PATH, new_date)}"
        df_today = spark.read.parquet(path_today)
        print(f"📁 {df_today.count()} lignes lues")

        # Lecture bronze précédent (si existant)
        if last_bronze_date:
            try:
                path_bronze = Config.get_parquet_path(Config.BRONZE_ROOT, last_bronze_date)
                df_prev = spark.read.parquet(path_bronze)
                accumulated = df_prev.unionByName(df_today)  # ✅ unionByName plus sécurisé
            except Exception as e:
                print(f"⚠️ Erreur lecture Bronze : {e}")
                accumulated = df_today
        else:
            accumulated = df_today

        # Repartition (si présence de "State")
        if "State" in accumulated.columns:
            accumulated = accumulated.repartition(10, "State")
        else:
            accumulated = accumulated.repartition(10)

        new_bronze_path = Config.get_parquet_path(Config.BRONZE_ROOT, new_date)
        print(f"💾 Sauvegarde : {new_bronze_path}")
        accumulated.coalesce(1).write.mode("overwrite").parquet(new_bronze_path)
        print(f"✅ Sauvegardé {accumulated.count()} lignes")

        last_bronze_date = new_date

    spark.stop()
    print("🔥 Terminé")

if __name__ == "__main__":
    main()
