// Left Outer Join
/*Devuelve filas del conjunoto de dato de la izquierda incluso cuando la expresión de join se evalúa como falsa*/

val empleados = spark.read.parquet("/FileStore/section8/empleados.parquet")

val departamentos = spark.read.parquet("/FileStore/section8/departamentos.parquet")

import org.apache.spark.sql.functions.col

empleados.show
departamentos.show

empleados.join(departamentos, col("num_dpto") === col("id"), "leftouter").show

empleados.join(departamentos, col("num_dpto") === col("id"), "left_outer").show

empleados.join(departamentos, col("num_dpto") === col("id"), "left").show
