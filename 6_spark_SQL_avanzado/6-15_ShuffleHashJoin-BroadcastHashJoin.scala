// Shuffle Hash Join y Broadcast Hash Join
/*Join es una de las operaciones más costosas de spark porque requiere mover 
una gran cantidad de datos de varias máquinas a través de una red. 
Al mover datos a través de una red, los datos generalmente pasan por un proceso 
de serialización y deserialización. Es importante tener en cuenta la reducción de
la frecuencia con que unimos grandes conjuntos de datos siempre que sea posible.
Spark usa dos estrategias diferentes para unir DFs:

	-Shuffle Hash Join: se usa cuando el tamaño de los datos es grande. Consta de dos pasos:
		1.-Calcular el valor hash de las columnas en la expresión de join de cada fila en 
		cada conjunto de datos y luego mover esas filas con el mismo valor hash a la misma partición. 
		Para determinar a qué partición se moverá una fila en particular, spark realiza una 
		operación aritmética que calcula el módulo del valor hash por el número de particiones.

		2.-Combina las columnas de aquellas filas que tienen el mismo valor hash de columna
	En alto nivel, estos dos pasos son similares a los pasos del modelo de programación de matrices.

	-Broadcast Hash Join: se solo usa cuando el tamaño de al menos uno de los conjuntos de datos es lo 
	suficientemente pequeño como para caber en la memoria de los drivers. Evita el movimiento de ambos 
	conjuntos de datos moviendo solo el más pequeño. Consta de dos pasos:
		1.-Transmitir una copia de todo el conjunto de datos más pequeño a cada una de las 
		particiones del conjunto de datos más grande.

		2.-Recorrer cada fila en el conjunto de datos más grande y buscar las filas correspondientes 
		en el conjunto de datos más pequeño con valores de columna coincidentes.

Siempre que sea posible se usará Broadcast Hash Join. Spark SQL suele determinar automáticamente cuál 
se debe usar, en función de algunas estadísticas que tiene sobre los conjuntos de datos mientras los lee.*/

val empleados = spark.read.parquet("/FileStore/section8/empleados.parquet")

val departamentos = spark.read.parquet("/FileStore/section8/departamentos.parquet")

empleados.show
departamentos.show

import org.apache.spark.sql.functions.{col, broadcast}

empleados.join(broadcast(departamentos), col("num_dpto") === col("id")).show//Le indicamos que haga el join con broadcast y es en el paréntesis de la función broadcast donde le indicamos qué DF queremos que envíe a la memoria de los drivers.

empleados.join(broadcast(departamentos), col("num_dpto") === col("id")).explain
