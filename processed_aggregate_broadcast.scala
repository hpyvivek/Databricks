// Databricks notebook source
import org.apache.spark.sql.functions._
val broadcastdf = spark.read
                    .option("header","true")
                    //.option("delimiter",";")
                    .option("inferschema","true")
                    .csv("dbfs:/mnt/viv_mount1/vivek-db/processed_Layer/processed_broadcast_data/part-00000-tid-1384298129139785179-428ee2ce-3d74-4a5b-be11-0985558bd6c2-600-1-c000.csv")
                    //.withColumn("load_date",current_date().as("load_date"))
  
display(broadcastdf)

// COMMAND ----------

import org.apache.spark.sql.functions._
 
val streamdf = spark.read
                    .option("header","true")
                    //.option("delimiter",";")
                    .option("inferschema","true")
                    .csv("dbfs:/mnt/viv_mount1/vivek-db/processed_Layer/processed_stream1/part-00000-tid-2564948735536130630-aeb0380b-9993-4c92-a198-e973ef7f1857-480-1-c000.csv")
display(streamdf)

// COMMAND ----------

val joinedDF = broadcastdf.join(streamdf,
    broadcastdf.col("house_number") === streamdf.col("house_number") &&
    broadcastdf.col("broadcast_right_region") === streamdf.col("country_code"),"inner").distinct
    .drop(broadcastdf.col("house_number"))
    .drop(streamdf.col("dt"))
    .drop(broadcastdf.col("load_date"))
    .drop(broadcastdf.col("broadcast_right_region"))
display(joinedDF)

// COMMAND ----------

val result = joinedDF.select("dt","time","device_name","house_number",
                                                 "user_id","country_code","program_title",
                                                 "season","season_episode","genre","product_type",
                                                 "broadcast_right_start_date","broadcast_right_end_date")
                                .where(streamdf("product_type") === "tvod" || streamdf("product_type") === "est")
                                .withColumn("load_date",current_date())
                                .withColumn("year",year(col("load_date")))
                                .withColumn("month",month(col("load_date")))
                                .withColumn("day",dayofmonth(col("load_date")))
display(result)

// COMMAND ----------

result.coalesce(1).write.option("header","true")
.partitionBy("year", "month", "day")
.csv("dbfs:/mnt/viv_mount1/vivek-db/aggregate_layer/broadcast_aggregate_layer1")