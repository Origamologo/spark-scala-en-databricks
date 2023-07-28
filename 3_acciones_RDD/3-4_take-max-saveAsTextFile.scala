// Funciones take, max y saveAsTextFile

val sc = spark.sparkContext

// take
// take: devuelve el registro especificado como argumento.
val rdd = sc.parallelize("La programación es bella".split(" "))

rdd.take(2)//Toma los 2 primeros elementos del RDD

rdd.take(4)//Toma los 4 primeros elementos del RDD

// max 
//max: devuelve el registro máximo.

val rddNumero = sc.parallelize(1 to 15)

rddNumero.max

val rddPar15 = rddNumero.filter(_ % 2 == 0)

rddPar15.max

//min: devuleve el registro mínimo
rddPar15.min

// saveAsTextFile
// saveAsTextFile: usando esta acción, podemos escribir el RDD en un archivo de texto.

rddPar15.saveAsTextFile("/FileStore/Seccion5/Lectura28")//Así nos va a crear un archivo txt por cada partition

dbutils.fs.ls("/FileStore/Seccion5/Lectura28")

rddPar15.coalesce(1).saveAsTextFile("/FileStore/Seccion5/Lectura28/rdd1")//Así nos guarda el RDD en un solo txt

//dbutils es una librería que nos permite operar y consultar los archivos
dbutils.fs.ls("/FileStore/Seccion5/Lectura28/rdd1")//fs es FileSystem y ls es list
