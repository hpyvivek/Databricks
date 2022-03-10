// Databricks notebook source
import org.apache.spark.sql.functions._
 
val streamdf = spark.read
                    .option("header","true")
                    //.option("delimiter",";")
                    .option("inferschema","true")
                    .csv("dbfs:/mnt/viv_mount1/vivek-db/processed_Layer/processed_stream1")
display(streamdf)

// COMMAND ----------

val transStream1 = streamdf.select("dt","device_name","product_type","user_id","program_title","country_code")
                                .groupBy("dt","device_name","product_type","program_title","country_code")
                                .agg(countDistinct("user_id") as "unique_users",count("program_title") as "content_count")
                                .withColumn("load_date", current_date())
                                .withColumn("year", year(col("load_date")))
                                .withColumn("month", month(col("load_date")))
                                .withColumn("day", dayofmonth(col("load_date")))
                                .sort(col("unique_users").desc)
display(transStream1)

// COMMAND ----------

transStream1.coalesce(1).write.option("header","true")
.partitionBy("year", "month", "day")
.csv("dbfs:/mnt/viv_mount1/vivek-db/aggregate_layer/stream_aggregate_layer1")
                                                      


// COMMAND ----------

