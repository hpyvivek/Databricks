// Databricks notebook source
val access_key = "AKIfjkhfkjhfdkjdfkjhfkjfhkjL"
val secret_key = "hfdkjfdkjhfkdjhkdjhdkjhs"
val encoded_secret_key = secret_key.replace("/", "%2F")
val aws_bucket_name = "databrickprac"
val mount_name = "viv_mount1"



dbutils.fs.mount(s"s3a://$access_key:$encoded_secret_key@$aws_bucket_name", s"/mnt/$mount_name")

// COMMAND ----------

// MAGIC %fs mounts

// COMMAND ----------

display(dbutils.fs.ls("/mnt/viv_mount"))

// COMMAND ----------

