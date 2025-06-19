from pyspark.sql import SparkSession
import os

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Write CSV Example") \
    .getOrCreate()

# Exemple de données
data = [
    ("Alice", 34),
    ("Bob", 45),
    ("Charlie", 29)
]
columns = ["name", "age"]

# Création du DataFrame
df = spark.createDataFrame(data, columns)

# Définir le chemin de sortie
output_path = "app/feeder"

# S'assurer que le dossier existe (créé côté système de fichiers si besoin)
os.makedirs(output_path, exist_ok=True)

# Écriture au format CSV (avec en-têtes et en un seul fichier)
df.coalesce(1).write \
    .option("header", True) \
    .mode("overwrite") \
    .csv(output_path)

# Arrêt de la session Spark
spark.stop()
