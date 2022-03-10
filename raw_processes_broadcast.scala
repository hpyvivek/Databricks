// Databricks notebook source
import org.apache.spark.sql.functions._
val broadcastdf = spark.read
                    .option("header","true")
                    //.option("delimiter",";")
                    .option("inferschema","true")
                    .csv("dbfs:/mnt/viv_mount1/vivek-db/raw_layer/broadcast_right.csv")
                    .withColumn("load_date",current_date().as("load_date"))
  
display(broadcastdf)

// COMMAND ----------

val processesed_broadcast1  = broadcastdf.withColumn("broadcast_right_vod_type", lower(col("broadcast_right_vod_type")))
display(processesed_broadcast1)

// COMMAND ----------

 val processed_broadcast2 = processesed_broadcast1.withColumn("broadcast_right_region",
         when (col("broadcast_right_region") === "Denmark", "dk")
         .when (col("broadcast_right_region") === "Baltics", "ba")
         .when (col("broadcast_right_region") === "Bulgaria", "bg")
         .when (col("broadcast_right_region") === "Estonia", "ee")
         .when (col("broadcast_right_region") === "Finland", "fl")
         .when (col("broadcast_right_region") === "Latvia", "lv")
         .when (col("broadcast_right_region") === "Lithuania", "lt")
         .when (col("broadcast_right_region") === "Nordic", "nd")
         .when (col("broadcast_right_region") === "Norway", "no")
         .when (col("broadcast_right_region") === "Russia", "ru")
         .when (col("broadcast_right_region") === "Serbia", "rs")
         .when (col("broadcast_right_region") === "Slovenia", "si")
         .when (col("broadcast_right_region") === "Sweden", "se")
         .when (col("broadcast_right_region") === "VNH Region Group","vnh")
         .when (col("broadcast_right_region") === "Viasat History Region Group","vh")
         .otherwise("nocode"))
display(processed_broadcast2)

// COMMAND ----------

processed_broadcast2.coalesce(1).write.option("header","true").csv("dbfs:/mnt/viv_mount1/vivek-db/processed_Layer/processed_broadcast_data")