// Función count
/*
Cuenta el número de elementos del RDD y lo envía al controlador
 */

val sc = spark.sparkContext

val rdd = sc.parallelize(Seq("j", "o", "s", "e"))

rdd.count

val rddN = sc.parallelize(1 to 50)

rddN.count
