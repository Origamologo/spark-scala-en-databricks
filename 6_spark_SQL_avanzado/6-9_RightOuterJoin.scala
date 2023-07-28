// Right Outer Join
/*Devuelve filas del conjunto de datos de la derecha incluso cuando la expresión
de join se evalúa como falsa*/

val empleados = spark.read.parquet("/FileStore/section8/empleados.parquet")

val departamentos = spark.read.parquet("/FileStore/section8/departamentos.parquet")

empleados.show
departamentos.show

import org.apache.spark.sql.functions.col

empleados.join(departamentos, col("num_dpto") === col("id"), "rightouter").show

empleados.join(departamentos, col("num_dpto") === col("id"), "right_outer").show

empleados.join(departamentos, col("num_dpto") === col("id"), "right").show
