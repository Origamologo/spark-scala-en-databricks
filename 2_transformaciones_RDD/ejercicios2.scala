val sc = spark.sparkContext

val lenguajes = sc.parallelize(Seq("Python", "R", "C", "Scala", "Ruby", "SQL"))

val lenguajesMay = lenguajes.map(_.toUpperCase)

val rddStartR = lenguajes.filter(_.startsWith("R")//Nos dará sólo lo que empiece por R


val rddPares = sc.parallelize(20 to 30).filter(_%2 == 0)

import scala.math.sqrt
val rddSqrt = pares.map(sqrt(_))

val rddParesSqrt = pares.flatmap(x => List(x, sqrt(x)))//Lista con el número y su raiz cuadrada


val rddSqrt20 = rddSqrt.repartition(20)//Eleva las particiones a 20


def procesar(s: String): Array[String] = {
 //Elimina los espacios en blanco y los paréntesis y divide por la coma
  s.replaceAll(" ","").replaceAll("\\(","").replaceAll("\\)","").split(",")
}

rddTransacciones.map(procesar(_)).map(x => (x(0), x(1).tofloat)).reduceByKey(_+_)//Aplica procesar, reduce a un decimal el segundo valor de cada tupla y y suma los values de los registros repetidos
