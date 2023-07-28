// Cross-Join
/*Devuelve filas combinando cada fila del conjunto de datos de 
la izquierda con cada fila del conjunto de datos de la derecha.
Hay que tener cuidado porque el número de filas resultante es la 
multiplicación del número de filas de los DFs originales*/

val empleados = spark.read.parquet("/FileStore/section8/empleados.parquet")

val departamentos = spark.read.parquet("/FileStore/section8/departamentos.parquet")

empleados.show
departamentos.show

empleados.crossJoin(departamentos).show(100, false)
