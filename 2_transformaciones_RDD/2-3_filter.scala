// Función filter
/*
Filtra el RDD según una función dada o sea, que en el RDD salida
tenemos los elementos que cumplan la condición descrita en la función
*/

val sc = spark.sparkContext

val rdd = sc.parallelize(1 to 10)

val rddDivisible3 = rdd.filter(_ % 3 == 0)//Nos da los números que son divisibles por 3

rddDivisible3.collect

val rddTexto = sc.parallelize(Seq("juan", "julia", "pedro", "katia"))

val rddInicioJ = rddTexto.filter(_.startsWith("j"))//Nos da los nombres que empiezan por j

rddInicioJ.collect

val rddInicioJFinA = rddTexto.filter(x => x.startsWith("j") & x.endsWith("a"))//Nos da los nombres que empiezan por j y acaban por a

rddInicioJFinA.collect

val rddOrNombre = rddTexto.filter(x => x.startsWith("j") | x.startsWith("k"))//Nos da los nombres que empiezan por j o acaban por a

rddOrNombre.collect
