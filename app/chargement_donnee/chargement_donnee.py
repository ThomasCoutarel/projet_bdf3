import kagglehub
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from datetime import datetime, timedelta

# Download latest version
path = kagglehub.dataset_download("sobhanmoosavi/us-accidents")

print("Path to dataset files:", path)

# Créer la session Spark
spark = SparkSession.builder \
    .appName("chargement_donnee") \
    .getOrCreate()

df_initial  = spark.read.csv("/root/.cache/kagglehub/datasets/sobhanmoosavi/us-accidents/versions/13/US_Accidents_March23.csv", header=True, inferSchema=True)
df_initial.printSchema()
df_initial.show()

# 1. Comptons le nombre total de lignes
total_count = df_initial.count()

# 2. Calcule la taille de chaque partie (on divise en 3)
part_size = total_count // 3
# Attention au reste qui peut être non nul

# 3. Ajoutons une colonne "row_num" avec un numéro unique croissant
window_spec = Window.orderBy(F.monotonically_increasing_id())
df_with_row_num = df_initial.withColumn("row_num", F.row_number().over(window_spec))

# 4. Définissons une fonction pour récupérer une tranche du DF avec date ajoutée
def get_partition(df, start_row, end_row, date_str):
    partition_df = df.filter((F.col("row_num") > start_row) & (F.col("row_num") <= end_row))
    return partition_df.drop("row_num").withColumn("date", lit(date_str))

# 5. Préparons les dates (format "dd/MM/yyyy")
start_date = datetime.strptime("01/01/2025", "%d/%m/%Y")
date_list = [(start_date + timedelta(days=i)).strftime("%d/%m/%Y") for i in range(3)]

# 6. Crée les 3 partitions
part1 = get_partition(df_with_row_num, 0, part_size, date_list[0])
part2 = get_partition(df_with_row_num, part_size, part_size * 2, date_list[1])
part3 = get_partition(df_with_row_num, part_size * 2, total_count, date_list[2])

# 7. Maintenant tu peux sauvegarder chaque partie comme tu veux, par ex :
part1.write.csv("chargement_donnee/partie1.csv", header=True)
part2.write.csv("chargement_donnee/partie2.csv", header=True)
part3.write.csv("chargement_donnee/partie3.csv", header=True)
