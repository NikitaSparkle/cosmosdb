from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, array, expr
import uuid

def generate_uuid():
    return str(uuid.uuid4())

spark = SparkSession.builder.appName("BigDataLab").getOrCreate()

spark.udf.register("uuid", generate_uuid)

df = spark.read.format("delta").load("dbfs:/user/hive/warehouse/movies")

df.printSchema()
df.show(10)

def count_nulls_and_nans(df):
    expressions = []
    for c in df.columns:
        if dict(df.dtypes)[c] in ['double', 'float']:
            expressions.append(count(when(col(c).isNull() | isnan(c), c)).alias(c))
        else:
            expressions.append(count(when(col(c).isNull(), c)).alias(c))
    return df.select(expressions)

count_nulls_and_nans(df).show()

fill_values = {
    "extract": "No extract",
    "href": "No href",
    "thumbnail": "No thumbnail",
    "thumbnail_height": 0,
    "thumbnail_width": 0,
    "title": "No title",
    "year": 0
}
df = df.na.fill(fill_values)

df = df.withColumn("cast", when(col("cast").isNull(), array()).otherwise(col("cast")))
df = df.withColumn("genres", when(col("genres").isNull(), array()).otherwise(col("genres")))

df = df.na.drop()

df = df.dropDuplicates(["title", "year"])

df = df.withColumn("id", expr("uuid()"))

threshold = 2000  # Замініть на ваш реальний поріг
df = df.withColumn("new_column", when(df["year"] > threshold, "Category1").otherwise("Category2"))

df = df.filter(df["year"] >= 2021)

writeConfig = {
    "spark.cosmos.accountEndpoint": "https://bdlab-cosmos.documents.azure.com:443/",
    "spark.cosmos.accountKey": "tfm0eQGpEnRaJop1fKAAmatNPSDElg2PzzVHzpOjmif6G4Fscc1KuMXhKxMzZdl5bCJmdo9W6clsACDbizgcug==",
    "spark.cosmos.database": "cosmosdb",
    "spark.cosmos.container": "movies",
    "spark.cosmos.write.strategy": "ItemOverwrite"
}

df.write.format("cosmos.oltp").options(**writeConfig).mode("APPEND").save()

readConfig = {
    "spark.cosmos.accountEndpoint": "https://bdlab-cosmos.documents.azure.com:443/",
    "spark.cosmos.accountKey": "tfm0eQGpEnRaJop1fKAAmatNPSDElg2PzzVHzpOjmif6G4Fscc1KuMXhKxMzZdl5bCJmdo9W6clsACDbizgcug==",
    "spark.cosmos.database": "cosmosdb",
    "spark.cosmos.container": "movies"
}

df_cosmos = spark.read.format("cosmos.oltp").options(**readConfig).load()

df_cosmos.createOrReplaceTempView("cosmos_view")

result = spark.sql("SELECT * FROM cosmos_view WHERE new_column = 'Category1'")
result.show()

spark.stop()