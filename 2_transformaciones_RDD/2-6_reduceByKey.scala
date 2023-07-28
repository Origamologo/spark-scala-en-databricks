// Función reduceByKey
/*
Se usa para fusionar los valores de cada clave usando una función asociativa de reducción.
Opera usando el par key-value.
*/

val sc = spark.sparkContext

val rdd = sc.parallelize(Seq(
  ("casa", 2),
  ("parque", 1),
  ("que", 5),
  ("casa", 1),
  ("escuela", 2),
  ("casa", 1),
  ("que", 1)
))

rdd.collect

val rddReducido = rdd.reduceByKey(_ + _)//Nos suma el número asociado a las palabras repetidas y elimina las repeticiones 

rddReducido.collect

val rddReducido2 = rdd.reduceByKey((x, y) => x + y)//Hace lo mismo que la anterior

rddReducido2.collect
