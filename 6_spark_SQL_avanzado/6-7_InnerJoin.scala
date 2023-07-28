// Inner Joins

/*Devuelve filas de ambos conjuntos de datos cuando la expresión de join se evalua como verdadera.
Las filas que no tengan valores de columna coincidentes se excluirán del conjunto de datos resultantes*/

/* /FileStore/section8/departamentos.parquet
   /FileStore/section8/empleados.parquet */

val empleados = spark.read.parquet("/FileStore/section8/empleados.parquet")

val departamentos = spark.read.parquet("/FileStore/section8/departamentos.parquet")

empleados.show

departamentos.show

// Inner join

import org.apache.spark.sql.functions.col

val joinDF = empleados.join(departamentos, col("num_dpto") === col("id"))

joinDF.show

val joinDF = empleados.join(departamentos, col("num_dpto") === col("id"), "inner")//inner join es el join por defecto, así que no es necesario especificar el tipo de join en este caso

joinDF.show
