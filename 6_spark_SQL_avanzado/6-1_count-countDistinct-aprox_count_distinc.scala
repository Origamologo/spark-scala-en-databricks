// Funciones count, countDistinct y approx_count_distinct
/*La realización de análisis en big data suele implicar algún tipo 
de agregación para resumir los datos con el fin de extraer patrones, 
conocimientos o generar informes resumidos.*/

/* /FileStore/section8/muestra.parquet
  /FileStore/section8/vuelos.parquet */

val dfMuestra = spark.read.parquet("/FileStore/section8/muestra.parquet")
val dfVuelos = spark.read.parquet("/FileStore/section8/vuelos.parquet")


df.show()

// count para averiguar la cantidad de elementos en un grupo

import org.apache.spark.sql.functions.{col, count, countDistinct, approx_count_distinct}

df.select(
  count(col("nombre")).as("conteo_nombre"),
  count(col("color")).as("conteo_color")
).show //La función count no incluye los null en el conteo. Para incluir null usaremos el *

df.select(
  count(col("nombre")).as("conteo_nombre"),
  count(col("color")).as("conteo_color"),
  count("*").as("conteo_general") //Esto sí que incluye los null en el conteo
).show

// countDistinct para saber el número de valores diferentes en un grupo

df.select(
  countDistinct(col("color")).as("conteo_colores_dif")//Ignora los null
).show

// approx_count_distinct(col, max_estimated_error=0.05)
/*Contar el número de elementos únicos en un gran conjunto de datos puede 
ser una operación muy costosa. Puede ser suficiente con tener un recuento aproximado*/

val vuelos = spark.read.parquet("/FileStore/section8/vuelos.parquet")

vuelos.printSchema

display(vuelos)

// Realizar un countdistinct y un aprox_count_distinct de las aerolineas

vuelos.select(
  countDistinct(col("AIRLINE")).as("conteo_aerolines"),
  approx_count_distinct(col("AIRLINE")).as("conteo_aprox")
).show
