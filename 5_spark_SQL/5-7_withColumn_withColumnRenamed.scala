// Funciones withColumn y withColumnRenamed
/*withColumn agrega una nueva columna a un DF.
withColumnRenamed sirve para cambiar el nombre de una columna*/

val df = spark.read.parquet("/FileStore/section7/datos.parquet")

// withColumn pide primero el nombre de la columna a crear y luego la expresión que define su contenido

import org.apache.spark.sql.functions.col

val dfValoracion = df.withColumn("valoracion", col("likes") - col("dislikes"))

display(dfValoracion)

dfValoracion.printSchema

val dfValoracionCompleja = df.withColumn("valoracion", col("likes") - col("dislikes")).withColumn("resto", col("valoracion") % 10)

display(dfValoracionCompleja)

dfValoracionCompleja.printSchema

// withColumnRenamed pide el nombre existente y el nuevo nombre

val dfRenombrado = df.withColumnRenamed("video_id", "id")

dfRenombrado.printSchema

// No devuelve error pero no realiza nada por detrás

val test = df.withColumnRenamed("nombrequenoexiste", "nuevonombre")

test.printSchema
