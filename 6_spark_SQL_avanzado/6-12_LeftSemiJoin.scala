// Left Semi Join
/*Devuelve filas solo del conjunto de datos de la izquierda 
cuando la expresión de join se evalúa como verdadera*/

val empleados = spark.read.parquet("/FileStore/section8/empleados.parquet")

val departamentos = spark.read.parquet("/FileStore/section8/departamentos.parquet")

empleados.show
departamentos.show

import org.apache.spark.sql.functions.col

empleados.join(departamentos, col("num_dpto") === col("id"), "left_semi").show

departamentos.join(empleados, col("num_dpto") === col("id"), "left_semi").show
