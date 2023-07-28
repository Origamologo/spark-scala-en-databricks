// Agregación con agrupación
/*Agregación con agrupación es un proceso de dos pasos:
	1.- Realizar la agrupación mediante la transformación groupBy(col1, col2,...) y es ahí donde se especifica por qué columnas agrupar las filas. A diferencia de otras transformaciones que devuelven un DF, groupBy devuelve una instancia de la clase relacional groupDataset a la que luego podemos aplicar una o más funciones de agregación.
	2.- Añadir las funciones de agregación deseadas.*/

val vuelos = spark.read.parquet("/FileStore/section8/vuelos.parquet")
vuelos.printSchema

import org.apache.spark.sql.functions.{desc, col, avg}

vuelos.groupBy("ORIGIN_AIRPORT").count.orderBy(desc("count")).show//Toma el DF y lo agrupa por los vuelos que tienen el mismo aeropuerto de origen, cuenta por cada uno de esos aeropuertos y ordena el resultadod e salida de mayor a menor


vuelos.groupBy("ORIGIN_AIRPORT", "DESTINATION_AIRPORT").count.orderBy(desc("count")).show
