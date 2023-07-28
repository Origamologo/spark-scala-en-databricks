// Función repartition
/*
Puede aumentar o decrecer el número de particiones del RDD, mientras que coalesce sólo lo reduce.
Tanto repartition() como coalesce() son operaciones muy costosas en memoria pero coalesce() está más optimizada.
*/

val sc = spark.sparkContext

val rdd = sc.parallelize(1 to 10), 5)

rdd.getNumPartitions

val rdd7 = rdd.repartition(7)

rdd7.getNumPartitions

val rdd3 = rdd.repartition(3)

rdd3.getNumPartitions
