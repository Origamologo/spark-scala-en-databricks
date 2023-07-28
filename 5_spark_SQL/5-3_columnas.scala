//Los DF son inmutables y cualquier transformación dará como resultado un nuevo DF
// Trabajo con columnas

val df = spark.read.parquet("/FileStore/section7/dataPARQUET.parquet")

// Primera alternativa para referirse a las columnas

df.printSchema

df.select("title").show()//Nos dará un nuevo DF con el contenido de la columna title truncado
df.select("title").show(false)//Nos dará un nuevo DF con el contenido de la columna title completo

// Segunda alternativa

import org.apache.spark.sql.functions.{col, column}

df.select(col("title")).show()

df.select(column("title")).show(false)

// Tercera alternativa

df.select($"title").show()
