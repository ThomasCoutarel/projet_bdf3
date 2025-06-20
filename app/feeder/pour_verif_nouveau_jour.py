from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialisation Spark
spark = SparkSession.builder \
    .appName("Clone fichier parquet avec nouvelle date") \
    .master("local[*]") \
    .getOrCreate()

# 📂 Chemin source et cible
source_file = "file:///app/bronze_data/temp/2025_06_17/parquet/part-00002-0647e098-05d6-4a02-ab34-82f776979550-c000.snappy.parquet"
target_dir = "file:///app/bronze_data/temp/2025_06_19/parquet/"


# 🗃️ Lecture du fichier Parquet
df = spark.read.parquet(source_file)
print("✅ Lecture du fichier source réussie")

# 🛠️ Remplacement (ou ajout) de la colonne situation_date
df_updated = df.withColumn("situation_date", lit("2025-06-19"))

# 💾 Écriture dans le nouveau dossier (en écrasant si besoin)
df_updated.write.mode("overwrite").parquet(target_dir)
print(f"✅ Fichier sauvegardé dans {target_dir}")

# 🧼 Arrêt Spark
spark.stop()
