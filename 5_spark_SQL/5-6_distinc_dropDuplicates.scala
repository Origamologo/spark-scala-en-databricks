// Funciones distinct y dropDuplicates
/*distinc elimina filas duplicadas el DF.
dropDuplicates elimina filas duplicadas, pero le podemos decir en qué columnas tiene que fijarse*/

val df = spark.read.parquet("/FileStore/section7/data.parquet")

df.show

df.distinct.show

df.dropDuplicates("id", "color").show

df.dropDuplicates().show
