// Funciones definidas por el usuario UDF
/*Se pueden escribir funciones y usarlas de manera similar al uso de funciones integradas*/

import org.apache.spark.sql.functions.{col, udf}

val cubo = udf((n: Long) => n*n*n)//Esta función calcula el cubo de de un número dado

val df = spark.range(10)

df.select(
  col("id"),
  cubo(col("id")).as("cubo")//Aplicamos la función cubo
).show
