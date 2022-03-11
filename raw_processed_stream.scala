// Databricks notebook source
val access_key = "AKIfdkhjdkfhfkdjhfkdXBL"
val secret_key = "hkaslkdjlakdjlkdjldj39VT4Zs"
val encoded_secret_key = secret_key.replace("/", "%2F")
val aws_bucket_name = "databrickprac"
val mount_name = "viv_mount1"



dbutils.fs.mount(s"s3a://$access_key:$encoded_secret_key@$aws_bucket_name", s"/mnt/$mount_name")

// COMMAND ----------

val streamdf = spark.read
                    .option("header","true")
                    .option("delimiter",";")
                    .option("inferschema","true")
                    .csv("dbfs:/mnt/viv_mount1/vivek-db/raw_layer/started_streams.csv")
display(streamdf)

// COMMAND ----------

//import org.apache.spark.sql.functions._
 
val streamProcessesDF1 = spark.read
                      .option("header","true")
                      .option("delimiter", ";")
                      .csv("dbfs:/mnt/viv_mount1/vivek-db/raw_layer/started_streams.csv")
                      .withColumn("load_date",current_date().as("load_date"))
                                 
display(streamProcessesDF1)


// COMMAND ----------

streamProcessesDF1.write.option("header", true).save("/mnt/viv_mount1/vivek-db/processed_Layer/processed_stream".format("csv"))

// COMMAND ----------

streamProcessesDF1.coalesce(1).write.option("header","true").csv("dbfs:/mnt/viv_mount1/vivek-db/processed_Layer/processed_stream1")
 

// COMMAND ----------

