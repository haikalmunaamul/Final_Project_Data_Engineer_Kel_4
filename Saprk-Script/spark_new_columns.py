from pyspark.sql import SparkSession

# Inisialisasi Spark session
spark = SparkSession.builder.appName("RemoveColumns").getOrCreate()

# Membaca file CSV
df = spark.read.csv("file:////home/vigo/merged_data/new_merged_data.csv", header=True, inferSchema=True)


# Menghapus kolom 'prodcategoryid' dan 'salesorg'
df = df.drop("prodcategoryid", "salesorg")


# Menyimpan DataFrame yang telah dimodifikasi ke file CSV baru
df.write.csv("file:////home/vigo/merged_data/output_final_olap.csv", header=True, mode="overwrite")

# Menghentikan Spark session
spark.stop()
