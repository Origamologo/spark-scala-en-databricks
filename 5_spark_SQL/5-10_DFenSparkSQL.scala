// Acciones sobre un dataframe en Spark SQL
/*Tienen el mismo comportomiento que las acciones realizadas en un RDD, 
por lo que desencadenan el cálculo de todas las transformaciones 
que conducen a una acción en particular*/

val df = spark.read.parquet("/FileStore/section7/datos.parquet")

df.show()

df.show(5, false)//Número de registros que queremos ver y si queremos o no que se trunque la salida

df.head(1)//Tomar registros del principio del DF

df.take(1)//Tomar registros al azar del DF

df.select("likes").collect//collect recoje todos los elementos dentro de la columna "likes" y nos los devuelve en un array

df.count//Recuento de las filas del DF
