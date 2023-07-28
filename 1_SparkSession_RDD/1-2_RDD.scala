RDD (Resident Distributed Dataset)
Es la abstracción principal de spark
  Dependencias: otorga resilencia a los RDD
  Particiones: paraleliza el cálculo
  Función de cálculo: iterador para los datos almacenados en el RDD

i/ Diferentes formas de crear un RDD

// Crear un RDD vacío

val sc = spark.sparkContext //Primero corremos esta línea

val rddVacio = sc.emptyRDD //Ahora cargamos el RDD en una variable


// Crear un RDD con parallelize

val rddVacio1 = sc.parallelize(Seq(), 3) //Lo creamos con un secuencia vacía y 3 particiones

rddVacio1.getNumPartitions //Nos da el número de particiones del RDD

val rdd = sc.parallelize(Seq(1,2,3,4,5)) //RDD que contiene números del 1 al 5

rdd.collect //Vemos el contenido del RDD


// Crear un RDD desde un archivo de texto
//1º Subimos un archivo de texto haciendo clic en File => Upload data 

val rddTexto = sc.textFile("<remote_path_to_file>")

rddTexto.collect

val rddTextoCompleto = sc.wholeTextFiles("<remote_path_to_file>")//Lo mismo que textFile, pero incluyendo saltos de línea y líneas vacías

rddTextoCompleto.collect

val rddTextoVariasLineas = sc.textFile("<remote_path_to_file>") //cada línea será un elemento del RDD

rddTextoVariasLineas.collect.length


// Operaciones sencillas con RDD

val rddSuma = rdd.map(x => x + 1) //Con el map decimos que a cada elemento del RDD le vamos a sumar 1

rddSuma.collect


// Crear un RDD a partir de un DataFrame
//1º Creamos un dataframe
import spark.implicits._

val df = Seq((1, "jk"), (2, "ki")).toDF("id", "letras")

df.show()
//2º Creamos un RDD a partir del dataframe
val rddDataFrame = df.rdd //Él solito nos hará las particiones necesarias

rddDataFrame.collect


//Ejercicio: crear rdd con pares del 1 al 20

val rddPar =sc.parallelize(1 to 20).filter(_ % 2 == 0)
//Otra forma de hacer lo mismo:
val rddPar =sc.parallelize(1 to 20).filter(x => x % 2 == 0)


//Ejercicio: rdd con pares mayores de 10 y menores de 20
//Aprovecharemos el rddPar
val rddParMayor10 = rddPar.filter(_ > 10)
