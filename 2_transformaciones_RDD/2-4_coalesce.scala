// Función coalesce
/*
Combina las particiones de entrada par dar un RDD con las particiones de salida que queramos,
pero siempre reduce el número de particiones.
*/

val sc = spark.sparkContext

val rdd = sc.parallelize(Seq(1 to 10), 10)

rdd.getNumPartitions

val rdd5 = rdd.coalesce(5)

rdd5.getNumPartitions
