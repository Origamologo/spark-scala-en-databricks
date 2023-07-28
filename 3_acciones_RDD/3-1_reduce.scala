// Función reduce
/*
Aplica la función de reducción a todos los elementos del RDD y la envía al contrololador o driver
*/

val sc = spark.sparkContext

val rdd = sc.parallelize(1 to 10)

rdd.reduce(_ + _)//Suma todos los elementos del RDD

10*11/2

val rddP = sc.parallelize(1 to 3)

rddP.reduce(_ * _)//Multiplica todos los elementos del RDD
