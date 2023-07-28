/*
 SPARK SESSION
Es un punto de entrada unificado a todas las funciones de spark.
Para cargar la sesión basta con ejecutar el comando

spark

Y luego importamos la sesión de spark y creamos una variable donde quede guardada  con

import org.apache.spark.sql.SparkSession
val <nombre_variable> = SparkSession.builder.appName("<nombre_app>").getOrCreate()

Para consultar la configuración de la sesión

<nombre_variable>.conf.getAll.foreach(println)
 */

import org.apache.spark.sql.SparkSession

val sparkS = SparkSession.builder.appName("curso-scala-spark").getOrCreate()

sparkS.conf.getAll.foreach(println)
