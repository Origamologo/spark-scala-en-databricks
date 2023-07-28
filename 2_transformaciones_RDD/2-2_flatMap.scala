// Función flatMap
/*
Al igual que map, aplica una función a todas las particiones de entrada,
pero también aplana (o hace un flatens) a los elementos del RDD de entrada.
*/

val sc = spark.sparkContext

val rdd = sc.parallelize(Seq(1,2,3,4,5))

rdd.collect

val rddCuadrado = rdd.map(x => List(x, x * x)) //Esto crea una lista con listas anidadas dentro

rddCuadrado.collect

val rddCuadradoFlat = rdd.flatMap(x => List(x, x * x)) //Esto crea una única lista

rddCuadradoFlat.collect

val rddTexto = sc.parallelize(Seq("azul rojo verde", "morado amarillo negro"))

val rddColoresFlat = rddTexto.flatMap(_.split(" "))//Nos devulve un array con un color en cada posición

rddColores.collect
