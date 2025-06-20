from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Lakehouse Pipeline") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-server:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Crée les dossiers bronze/silver/gold sur HDFS
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
Path = spark._jvm.org.apache.hadoop.fs.Path

for layer in ["bronze", "silver", "gold"]:
    path = Path(f"hdfs://namenode:8020/lakehouse/{layer}")
    if not fs.exists(path):
        fs.mkdirs(path)

# Lecture de la table Hive via PostgreSQL (pour info)
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://host.docker.internal:5432/metastore_hive") \
    .option("user", "postgres") \
    .option("password", "20030205") \
    .option("dbtable", '"TBLS"') \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.show()

# Lecture du fichier CSV local
input_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("app/bronze_data/temp/2025_06_15/csv/part-00000-c94937ef-30fd-46e3-8a00-80297e1e8194-c000.csv")

# Écriture sur HDFS dans la couche bronze
input_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs://namenode:8020/lakehouse/bronze/2025_06_15/")
