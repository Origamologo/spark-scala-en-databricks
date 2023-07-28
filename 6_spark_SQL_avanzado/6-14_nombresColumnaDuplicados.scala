// Manejo de nombres de columna duplicados

val empleados= spark.read.parquet("/FileStore/section8/empleados.parquet")

val departamentos = spark.read.parquet("/FileStore/section8/departamentos.parquet")

empleados.show
departamentos.show

val empleadosRen = empleados.withColumnRenamed("num_dpto", "id")//Cambiamos el nombre de la columna "num_dpto" a "id"

empleadosRen.show

// Si en este punto trataramos de hacer un join como el siguiente nos daría error

import org.apache.spark.sql.functions.col

empleadosRen.join(departamentos, col("id") === col("id")).show//Esto da error porque no sabe a qué DF referencia id

val dfDuplicados = empleadosRen.join(departamentos, empleadosRen.col("id") === departamentos.col("id"))//Aquí le decimos de qué DF viene cada columna

dfDuplicados.show

import org.apache.spark.sql.functions.col

dfDuplicados.select(col("id")).show//Esto da error porque en este DF hay dos columnas que se llaman "id"...

dfDuplicados.select(empleadosRen.col("id")).show//...pero el nuevo DF 'recuerda' el origen de cada columna y lo podemos referenciar

val sinDuplicados = empleadosRen.join(departamentos, Seq("id"))//Y con Seq conseguimos un DF que contenga una sola columna "id", en vez de duplicarla porque es idéntica en los dos DF originales

sinDuplicados.select(col("id")).show//Ahora sí que podemos trabajar con la columna "id" sin que de error

sinDuplicados.show
