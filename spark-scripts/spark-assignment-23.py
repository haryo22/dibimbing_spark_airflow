import pyspark
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window

postgres_host = "dataeng-postgres"
postgres_dw_db = "warehouse"
postgres_user = "user"
postgres_password = "password"

sparkcontext = pyspark.SparkContext.getOrCreate(conf=(pyspark.SparkConf().setAppName("Dibimbing")))
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

jdbc_url = f"jdbc:postgresql://{postgres_host}/{postgres_dw_db}"
jdbc_properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified",
}

retail_df = spark.read.jdbc(jdbc_url, "public.retail", properties=jdbc_properties)

retail_df.show(5)

# Total Transaksi per Customer (customerid) --> Total transaksi = quantity * unitprice
retail_df = retail_df.withColumn("total_transaksi", retail_df["quantity"] * retail_df["unitprice"])

total_transaksi_df = retail_df.groupBy("customerid").agg(
    F.sum("total_transaksi").alias("total_transaksi")
)

total_transaksi_df.show(5)

# Jumlah User --> distinct customerid per invoicedate
purchase_df = retail_df.withColumn(
    "first_purchase",
    F.row_number().over(Window.partitionBy("customerid").orderBy("invoicedate"))
)

purchase_df = purchase_df.filter(purchase_df["first_purchase"] == 1)

jumlah_user = purchase_df.select("customerid").distinct().count()
print(f"Jumlah user: {jumlah_user}")

# Save ke CSV
def save_df_to_csv(spark_df, folder_path, filename):
    pandas_df = spark_df.toPandas()
    
    # Menentukan folder path di dalam folder yang dapat diakses
    os.makedirs(folder_path, exist_ok=True)
    
    # Menyimpan ke CSV menggunakan Pandas
    file_path = os.path.join(folder_path, f"{filename}.csv")
    pandas_df.to_csv(file_path, index=False)

    print(f"Data berhasil disimpan ke {file_path}")
    return file_path

save_df_to_csv(total_transaksi_df, "dibimbing_spark_airflow/data", "total_transaksi")
save_df_to_csv(purchase_df.select("customerid", "invoicedate"), "dibimbing_spark_airflow/data", "pembeli_baru")