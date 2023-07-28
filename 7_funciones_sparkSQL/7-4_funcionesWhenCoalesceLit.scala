// Funciones when, coalesce y lit

// when y otherwise
/*Evalúa un valor frente a una lista de condiciones
y devuelve un resultado*/

val df = spark.read.parquet("/FileStore/section9/lectura71.parquet")

df.show

import org.apache.spark.sql.functions.{col, when, coalesce, lit}

df.select(
  col("nombre"),
  when(col("pago") === 1, "pagado").when(col("pago") === 2, "sin pagar").otherwise("sin iniciar").as("pago")
  ).show//Añadimos una etiqueta cuando se cumple la condición de when u otra si no se cumple, que indicamos con otherwis


// coalesce y lit
/*coalesce toma uno o más valores de columna y devuelve el primero que no es nulo. 
Los argumentos de coalesce deben ser tipo columna.

lit nos permite completar un valor*/

df.select(
  coalesce(col("nombre"), lit("sin nombre")).as("nombre"),//coalesce va tomando valores hasta que se encuentra un null. Cuando hay un null, lit lo sustituye por "sin nombre"
  col("pago")
).show
