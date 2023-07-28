// Full Outer Join
/*Devuelve filas de ambops conjuntos de datos incluso cuando la expresión
de join se evalúa como falsa*/

val empleados = spark.read.parquet("/FileStore/section8/empleados.parquet")

val departamentos = spark.read.parquet("/FileStore/section8/departamentos.parquet")

// El comportamiento de este tipo de join es el mismo que el de combinar los joins left outer y right outer

empleados.show
departamentos.show

import org.apache.spark.sql.functions.col

empleados.join(departamentos,col("num_dpto") === col("id"), "outer").show
